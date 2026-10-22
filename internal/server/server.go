package server

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"flag"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/config"
	driver "ssf.dev/ssf/drivers/csi"
	"ssf.dev/ssf/gateway"
	"ssf.dev/ssf/guard"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
	"ssf.dev/ssf/node"
	"ssf.dev/ssf/observe"
	"ssf.dev/ssf/runtime"
	"strings"
	"syscall"
	"time"
)

func Main(role string) {
	path := flag.String("config", "/etc/ssf/config.json", "JSON configuration")
	flag.Parse()
	if err := Run(role, *path); err != nil {
		slog.Error("service stopped", "role", role, "error", err)
		os.Exit(1)
	}
}
func Run(role, path string) error {
	c, e := config.Load(path)
	if e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	tlsServer, e := auth.TLS(c.Cert, c.Key, c.CA, true)
	if e != nil {
		return e
	}
	tlsClient, e := auth.TLS(c.Cert, c.Key, c.CA, false)
	if e != nil {
		return e
	}
	if c.Listen == "" {
		return errors.New("listen address required")
	}
	observations := &observe.Registry{}
	metrics := http.NewServeMux()
	metrics.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	metrics.Handle("/metrics", observations)
	if c.HealthListen != "" {
		h := &http.Server{Addr: c.HealthListen, Handler: metrics, ReadHeaderTimeout: 5 * time.Second}
		go h.ListenAndServe()
		defer h.Close()
	}
	if role == "guard" {
		cfg, e := config.Kube(c.Kubeconfig)
		if e != nil {
			return e
		}
		api, e := kubernetes.NewForConfig(cfg)
		if e != nil {
			return e
		}
		db, e := ledger.Open(ctx, c.DatabaseURL)
		if e != nil {
			return e
		}
		defer db.Close()
		mux := http.NewServeMux()
		dyn, e := dynamic.NewForConfig(cfg)
		if e != nil {
			return e
		}
		if e = kube.ValidateProfiles(c.VolumeProfiles); e != nil {
			return e
		}
		handler := &guard.Handler{Ledger: db, API: api, Dynamic: dyn, VolumeProfiles: c.VolumeProfiles, ManagedNamespaces: c.ManagedNamespaces, CoordinatorUsers: c.CoordinatorUsers, SnapshotControllerUsers: c.SnapshotControllerUsers}
		mux.Handle("/validate", handler)
		mutator := *handler
		mutator.Mutating = true
		mux.Handle("/mutate", &mutator)
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			if e := db.Pool.Ping(r.Context()); e != nil {
				http.Error(w, "ledger unavailable", 503)
				return
			}
			w.WriteHeader(200)
		})
		t := tlsServer.Clone()
		t.ClientAuth = tls.NoClientCert // API-server webhook authenticates the server; admission RBAC is authoritative.
		h := &http.Server{Addr: c.Listen, Handler: mux, TLSConfig: t, ReadHeaderTimeout: 5 * time.Second}
		go func() { <-ctx.Done(); h.Close() }()
		return h.ListenAndServeTLS("", "")
	}
	if len(c.AllowedPeers) == 0 {
		return errors.New("explicit mTLS URI peer allowlist required")
	}
	g := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsServer)), grpc.ChainUnaryInterceptor(auth.PeerAllowlist(c.AllowedPeers), func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		start := time.Now()
		out, e := next(ctx, req)
		observations.Observe(info.FullMethod, time.Since(start), e)
		slog.Info("rpc", "method", info.FullMethod, "duration", time.Since(start), "success", e == nil)
		return out, e
	}))
	defer g.Stop()
	healthpb.RegisterHealthServer(g, health.NewServer())
	switch role {
	case "gateway":
		s, e := driver.Dial(ctx, c.DriverEndpoint, c.DriverName, "controller", "v1", tlsClient)
		if e != nil {
			return e
		}
		defer s.Conn.Close()
		db, e := ledger.Open(ctx, c.DatabaseURL)
		if e != nil {
			return e
		}
		defer db.Close()
		proxy := &gateway.Server{Controller: s.Controller, Identity: s.Identity, Authorizer: &gateway.LedgerAuthorizer{Store: db, Driver: c.DriverName}}
		csi.RegisterIdentityServer(g, proxy)
		csi.RegisterControllerServer(g, proxy)
	case "node":
		if c.NodeName == "" {
			c.NodeName = os.Getenv("SSF_NODE_NAME")
		}
		if c.BootID == "" {
			b, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
			if e != nil {
				return e
			}
			c.BootID = strings.TrimSpace(string(b))
		}
		if c.Root == "" || c.NodeName == "" {
			return errors.New("node identity and state root required")
		}
		keyBytes, e := os.ReadFile(c.PlanPublicKey)
		if e != nil {
			return e
		}
		key, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(keyBytes)))
		if e != nil || len(key) != ed25519.PublicKeySize {
			return errors.New("invalid plan public key")
		}
		wal, e := node.OpenWAL(c.Root + "/node-state/wal.db")
		if e != nil {
			return e
		}
		defer wal.Close()
		cfg, e := config.Kube(c.Kubeconfig)
		if e != nil {
			return e
		}
		api, e := kubernetes.NewForConfig(cfg)
		if e != nil {
			return e
		}
		registry := &config.Registry{Config: c, TLS: tlsClient, API: api}
		defer registry.Registry.Close()
		evidence, e := runtime.NewRemoteEvidence(c.EvidenceEndpoint, tlsClient)
		if e != nil {
			return e
		}
		engine := &node.Engine{WAL: wal, Sessions: registry, Secrets: &kube.Client{API: api}, Evidence: evidence, Key: key, NodeID: c.NodeName, BootID: c.BootID, Root: c.Root}
		if c.PreparationEndpoint != "" {
			engine.Preparer = &runtime.LocalPreparation{URL: c.PreparationEndpoint, TokenFile: c.PreparationTokenFile}
		}
		nativeNode, e := api.CoreV1().Nodes().Get(ctx, c.NodeName, metav1.GetOptions{})
		if e != nil {
			return e
		}
		engine.NodeUID = string(nativeNode.UID)
		engine.Observe = observations.Observe
		if e = engine.Recover(ctx); e != nil {
			return e
		}
		v1.RegisterRuntimeStorageNodeServer(g, &node.Service{Engine: engine})
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if e := engine.Recover(ctx); e != nil {
						slog.Error("recovery pending", "error", e)
					}
				}
			}
		}()
	default:
		return errors.New("unknown service role")
	}
	lis, e := net.Listen("tcp", c.Listen)
	if e != nil {
		return e
	}
	defer lis.Close()
	go func() { <-ctx.Done(); g.GracefulStop() }()
	return g.Serve(lis)
}
