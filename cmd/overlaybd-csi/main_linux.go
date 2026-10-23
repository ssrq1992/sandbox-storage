//go:build linux

// overlaybd-csi serves private standard CSI and managed-device lifecycle RPCs.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	k8s "k8s.io/client-go/kubernetes"
	control "ssf.dev/control"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/config"
	"ssf.dev/ssf/drivers/overlaybd"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
	"ssf.dev/ssf/runtime"
)

type repository struct {
	Endpoint string   `json:"endpoint"`
	ID       string   `json:"id"`
	Socket   string   `json:"socket"`
	Nodes    []string `json:"nodes"`
}
type configuration struct {
	OfflineExpansion   bool            `json:"offlineExpansion,omitempty"`
	CaptureCoordinator *control.Config `json:"captureCoordinator,omitempty"`
	Kubeconfig         string          `json:"kubeconfig"`
	Listen             string          `json:"listen"`
	WorkerRepository   string          `json:"workerRepository"`
	Peers              []string        `json:"peers"`
	Role               string          `json:"role"`
	Socket             string          `json:"socket"`
	DatabaseURLFile    string          `json:"databaseURLFile"`
	Repositories       []repository    `json:"repositories"`
	BackendSocket      string          `json:"backendSocket"`
	WAL                string          `json:"wal"`
	Node               string          `json:"node"`
	NodeUID            string          `json:"nodeUID"`
	CSINode            string          `json:"csiNode"`
	Root               string          `json:"root"`
	PlanPublicKey      string          `json:"planPublicKey"`
	CaptureEvidenceURL string          `json:"captureEvidenceURL"`
	EvidenceURL        string          `json:"evidenceURL"`
	Certificate        string          `json:"certificate"`
	Key                string          `json:"key"`
	CA                 string          `json:"ca"`
}

func readConfig(path string) (configuration, error) {
	var c configuration
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return c, err
	}
	var trailing any
	if err = d.Decode(&trailing); err != io.EOF {
		return c, errors.New("trailing configuration content")
	}
	if c.Role != "controller" && c.Role != "node" && c.Role != "worker" {
		return c, errors.New("role must be controller, node or worker")
	}
	if c.Role != "worker" && (!filepath.IsAbs(c.Socket) || filepath.Clean(c.Socket) != c.Socket) {
		return c, errors.New("canonical private Unix socket required")
	}
	return c, nil
}
func serve(ctx context.Context, c configuration) error {
	if c.Role == "worker" {
		return serveWorker(ctx, c)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
	defer server.Stop()
	var drainNode *overlaybd.CSINode
	if c.Role == "node" {
		if c.Node != "" && c.NodeUID == "" {
			cfg, err := config.Kube(c.Kubeconfig)
			if err != nil {
				return err
			}
			api, err := k8s.NewForConfig(cfg)
			if err != nil {
				return err
			}
			observed, err := api.CoreV1().Nodes().Get(ctx, c.Node, metav1.GetOptions{})
			if err != nil {
				return err
			}
			c.NodeUID = string(observed.UID)
		}
		if c.Node == "" || c.NodeUID == "" || c.CSINode == "" || !filepath.IsAbs(c.Root) || filepath.Clean(c.Root) != c.Root {
			return errors.New("complete node identity and private SSF root required")
		}
		raw, err := os.ReadFile(c.PlanPublicKey)
		if err != nil {
			return err
		}
		key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil || len(key) != ed25519.PublicKeySize {
			return errors.New("invalid plan public key")
		}
		boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
		if err != nil {
			return err
		}
		tlsConfig, err := auth.TLS(c.Certificate, c.Key, c.CA, false)
		if err != nil {
			return err
		}
		verifier, err := runtime.NewRemoteEvidence(c.EvidenceURL, tlsConfig)
		if err != nil {
			return err
		}
		captureVerifier, err := runtime.NewRemoteCaptureEvidence(c.CaptureEvidenceURL, tlsConfig)
		if err != nil {
			return err
		}
		backend, err := overlaybd.DialBackend(ctx, c.BackendSocket)
		if err != nil {
			return err
		}
		defer backend.Close()
		wal, err := overlaybd.OpenSessions(c.WAL)
		if err != nil {
			return err
		}
		defer wal.Close()
		if err = os.MkdirAll(filepath.Join(c.Root, "publish"), 0700); err != nil {
			return err
		}
		node := &overlaybd.CSINode{Authorization: overlaybd.NodeAuthorization{Key: key, Node: c.Node, NodeUID: c.NodeUID, Boot: strings.TrimSpace(string(boot)), CSINode: c.CSINode}, Operations: &overlaybd.NodeOperations{Sessions: wal, Backend: api.NewNativeStorageBackendClient(backend)}, Mounts: overlaybd.LinuxBlockPublisher{Root: filepath.Join(c.Root, "publish")}, Root: c.Root, Evidence: verifier, CaptureEvidence: captureVerifier}
		drainNode = node
		if err = overlaybd.RegisterNode(server, node); err != nil {
			return err
		}
		recoveryContext, cancelRecovery := context.WithCancel(ctx)
		recoveryDone := make(chan struct{})
		go func() {
			defer close(recoveryDone)
			node.RunRecovery(recoveryContext, func(err error) { log.Printf("OverlayBD recovery pending: %v", err) })
		}()
		defer func() { cancelRecovery(); <-recoveryDone }()
	} else {
		raw, err := os.ReadFile(c.DatabaseURLFile)
		if err != nil {
			return err
		}
		store, err := ledger.Open(ctx, strings.TrimSpace(string(raw)))
		if err != nil {
			return err
		}
		defer store.Close()
		if err = store.Migrate(ctx); err != nil {
			return err
		}
		journal := &overlaybd.Journal{Pool: store.Pool}
		if err = journal.Migrate(ctx); err != nil {
			return err
		}
		creator := &overlaybd.CSICreator{Journal: journal, Repositories: map[string]overlaybd.RepositoryWorker{}}
		deleter := &overlaybd.CSIDeleter{Journal: journal, Authorizer: overlaybd.LedgerDeletion{Store: store}, Backends: map[string]overlaybd.DeleteBackend{}}
		attacher := &overlaybd.CSIAttacher{Journal: journal, Ledger: store, Backends: map[string]overlaybd.HeadBackend{}}
		expander := &overlaybd.CSIExpander{Journal: journal, Authorizer: overlaybd.LedgerExpansion{Store: store}, Backends: map[string]overlaybd.MaintenanceBackend{}}
		for _, r := range c.Repositories {
			if r.ID == "" || creator.Repositories[r.ID].Backend != nil {
				return errors.New("empty or duplicate repository ID")
			}
			var connection *grpc.ClientConn
			if (r.Socket == "") == (r.Endpoint == "") {
				return errors.New("repository requires exactly one private socket or authenticated worker endpoint")
			}
			if r.Socket != "" {
				connection, err = overlaybd.DialBackend(ctx, r.Socket, overlaybd.CapabilityNativeVolume)
			} else {
				tlsConfig, tlsErr := auth.TLS(c.Certificate, c.Key, c.CA, false)
				if tlsErr != nil {
					return tlsErr
				}
				connection, err = overlaybd.DialWorker(ctx, r.Endpoint, tlsConfig)
			}
			if err != nil {
				return err
			}
			defer connection.Close()
			backend := api.NewNativeStorageBackendClient(connection)
			identity, err := backend.Describe(ctx, &api.DescribeRequest{})
			if err != nil {
				return err
			}
			found := false
			for _, id := range identity.RepositoryIds {
				if id == r.ID {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("worker does not own repository %s", r.ID)
			}
			creator.Repositories[r.ID] = overlaybd.RepositoryWorker{ID: r.ID, Nodes: r.Nodes, Backend: backend}
			deleter.Backends[r.ID] = backend
			attacher.Backends[r.ID] = backend
			if c.OfflineExpansion {
				if err = overlaybd.ValidateBackend(identity, overlaybd.CapabilityOfflineExpansion); err != nil {
					return err
				}
				expander.Backends[r.ID] = backend
			}
		}
		restConfig, err := config.Kube(c.Kubeconfig)
		if err != nil {
			return err
		}
		cluster, err := k8s.NewForConfig(restConfig)
		if err != nil {
			return err
		}
		objects, err := dynamic.NewForConfig(restConfig)
		if err != nil {
			return err
		}
		authority := &overlaybd.KubernetesCaptureAuthority{Kube: &kube.Client{API: cluster, Dynamic: objects}, Ledger: store, Idle: &overlaybd.IdleSnapshots{Ledger: store, Journal: journal, Backends: attacher.Backends}}
		if c.CaptureCoordinator != nil {
			captureClient, err := control.New(*c.CaptureCoordinator)
			if err != nil {
				return err
			}
			defer captureClient.Close()
			authority.Live = &overlaybd.RemoteLiveSnapshots{Client: captureClient, Runtime: "agentenv"}
		}
		snapshots := &overlaybd.CSISnapshots{Journal: journal, Authority: authority}
		recoveryContext, cancelRecovery := context.WithCancel(ctx)
		recoveryDone := make(chan struct{})
		go func() {
			defer close(recoveryDone)
			authority.RunDirectRecovery(recoveryContext, func(err error) { log.Printf("direct snapshot recovery pending: %v", err) })
		}()
		defer func() { cancelRecovery(); <-recoveryDone }()
		if c.OfflineExpansion {
			err = overlaybd.RegisterControllerWithExpansion(server, creator, deleter, attacher, expander, snapshots)
		} else {
			err = overlaybd.RegisterController(server, creator, deleter, attacher, snapshots)
		}
		if err != nil {
			return err
		}
	}
	parent := filepath.Dir(c.Socket)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("CSI socket parent must be a private directory (0700)")
	}
	lock, err := os.OpenFile(c.Socket+".lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return errors.New("driver endpoint already owned")
	}
	if info, err = os.Lstat(c.Socket); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("refusing non-socket endpoint replacement")
		}
		if err = os.Remove(c.Socket); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.Listen("unix", c.Socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err = os.Chmod(c.Socket, 0600); err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		if drainNode != nil {
			// The listener stays available for capture/unpublish/unstage. A
			// signal is never evidence that the original VMM has stopped.
			drainNode.BeginDrain()
			for {
				attempt, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err := drainNode.Reconcile(attempt)
				cancel()
				pending, checkErr := drainNode.DrainPending()
				if checkErr == nil && pending == 0 {
					break
				}
				log.Printf("OverlayBD drain pending: sessions=%d reconcile=%v check=%v", pending, err, checkErr)
				time.Sleep(time.Second)
			}
		}
		server.GracefulStop()
	}()
	return server.Serve(listener)
}

// probe uses only the local CSI Identity API; it never opens a backend or
// performs a schema migration or device operation.
func probe(ctx context.Context, c configuration) error {
	if c.Role == "worker" {
		return errors.New("CSI probe requires controller or node role")
	}
	connection, err := grpc.NewClient("unix://"+c.Socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", c.Socket)
		}))
	if err != nil {
		return err
	}
	defer connection.Close()
	result, err := csi.NewIdentityClient(connection).Probe(ctx, &csi.ProbeRequest{})
	if err != nil {
		return err
	}
	if result.Ready == nil || !result.Ready.Value {
		return errors.New("CSI service not ready")
	}
	return nil
}
func main() {
	file := flag.String("config", "", "driver JSON configuration")
	check := flag.Bool("probe", false, "probe local CSI readiness without starting services")
	backendCheck := flag.Bool("probe-backend", false, "probe private native host without migrations or device operations")
	flag.Parse()
	c, err := readConfig(*file)
	if err != nil {
		log.Fatal(err)
	}
	if *backendCheck {
		if *check {
			log.Fatal("probe modes are mutually exclusive")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		connection, err := overlaybd.DialBackend(ctx, c.BackendSocket)
		if err != nil {
			log.Fatal(err)
		}
		connection.Close()
		return
	}
	if *check {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := probe(ctx, c); err != nil {
			log.Fatal(err)
		}
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if err = serve(ctx, c); err != nil {
		log.Fatal(err)
	}
}

func serveWorker(ctx context.Context, c configuration) error {
	if c.Listen == "" || c.WorkerRepository == "" || len(c.Peers) == 0 {
		return errors.New("worker listen address, repository and explicit controller peer allowlist required")
	}
	tlsConfig, err := auth.TLS(c.Certificate, c.Key, c.CA, true)
	if err != nil {
		return err
	}
	connection, err := overlaybd.DialBackend(ctx, c.BackendSocket, overlaybd.CapabilityNativeVolume)
	if err != nil {
		return err
	}
	defer connection.Close()
	worker := &overlaybd.BackendWorker{RepositoryID: c.WorkerRepository, Backend: api.NewNativeStorageBackendClient(connection)}
	if _, err = worker.Describe(ctx, &api.DescribeRequest{}); err != nil {
		return err
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.ChainUnaryInterceptor(auth.PeerAllowlist(c.Peers)), grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
	defer server.Stop()
	api.RegisterNativeStorageBackendServer(server, worker)
	listener, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() { <-ctx.Done(); server.GracefulStop() }()
	return server.Serve(listener)
}
