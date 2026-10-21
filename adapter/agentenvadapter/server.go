package agentenvadapter

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"net"
	"net/http"
	"os"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/config"
	"ssf.dev/ssf/observe"
	"strings"
	"time"
)

type storageClient struct{ Client v1.RuntimeStorageNodeClient }

func (s storageClient) PrepareVolumes(c context.Context, q *v1.PrepareRequest) (*v1.PrepareResponse, error) {
	return s.Client.PrepareVolumes(c, q)
}
func (s storageClient) Expand(c context.Context, op, binding string, bytes int64) error {
	_, err := s.Client.ExpandVolume(c, &v1.ExpandRequest{OperationId: op, BindingUid: binding, CapacityBytes: bytes})
	return err
}
func (s storageClient) Ready(c context.Context, op, instance, proof string) error {
	_, e := s.Client.MarkRuntimeReady(c, &v1.RuntimeRequest{OperationId: op, SandboxInstanceUid: instance, Evidence: proof})
	return e
}
func (s storageClient) Release(c context.Context, op, instance, proof, plan string) error {
	_, e := s.Client.ReleaseVolumes(c, &v1.RuntimeRequest{OperationId: op, SandboxInstanceUid: instance, Evidence: proof, SignedNodePlan: plan})
	return e
}
func readKey(file string, size int) ([]byte, error) {
	b, e := os.ReadFile(file)
	if e != nil {
		return nil, e
	}
	v, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if e != nil {
		return nil, e
	}
	if len(v) != size {
		return nil, errors.New("invalid signing key size")
	}
	return v, nil
}
func Run(ctx context.Context, role string, c Config) error {
	if e := c.Validate(role); e != nil {
		return e
	}
	serverTLS, e := auth.TLS(c.Cert, c.Key, c.CA, true)
	if e != nil {
		return e
	}
	clientTLS, e := auth.TLS(c.Cert, c.Key, c.CA, false)
	if e != nil {
		return e
	}

	rest, e := config.Kube(c.Kubeconfig)
	if e != nil {
		return e
	}
	api, e := kubernetes.NewForConfig(rest)
	if e != nil {
		return e
	}
	metrics := &observe.Registry{}
	var handler http.Handler
	var readiness func(context.Context) error
	var drain func()
	if role == "node" {
		host, _, e := net.SplitHostPort(c.HookListen)
		if e != nil || (host != "127.0.0.1" && host != "::1") {
			return errors.New("hook listener must be Pod loopback")
		}
		k, e := api.CoreV1().Nodes().Get(ctx, c.NodeName, metav1.GetOptions{})
		if e != nil {
			return e
		}
		if c.NodeUID != "" && c.NodeUID != string(k.UID) {
			return errors.New("configured NodeUID differs")
		}
		c.NodeUID = string(k.UID)
		boot, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
		if e != nil {
			return e
		}
		pub, e := readKey(c.PlanPublicKey, ed25519.PublicKeySize)
		if e != nil {
			return e
		}
		key, e := os.ReadFile(c.AgentKeyFile)
		if e != nil {
			return e
		}
		j, e := OpenJournal(c.Root)
		if e != nil {
			return e
		}
		defer j.Close()
		conn, e := grpc.NewClient(c.SSFEndpoint, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
		if e != nil {
			return e
		}
		defer conn.Close()
		agent := &HTTPClient{Client: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 32}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect forbidden") }}, APIKey: strings.TrimSpace(string(key))}
		pidns, e := os.Readlink("/proc/self/ns/pid")
		if e != nil {
			return e
		}
		n := &Node{PIDNamespace: pidns, Config: c, BootID: strings.TrimSpace(string(boot)), Key: ed25519.PublicKey(pub), Journal: j, SSF: storageClient{v1.NewRuntimeStorageNodeClient(conn)}, Agent: agent, Context: ctx, Metrics: metrics}
		if c.NativeMode {
			if c.NativeNodeID == "" {
				c.NativeNodeID = os.Getenv("AENV_NODE_ID")
				n.Config.NativeNodeID = c.NativeNodeID
			}
			if c.HookTokenFile == "" || c.DispatchKeyFile == "" || c.NativeNodeID == "" {
				return errors.New("native provider authentication and node identity required")
			}
			if c.StandaloneConfig != "" {
				if c.SchedulerEndpoint != "" {
					return errors.New("standalone cannot also use a scheduler endpoint")
				}
				close, err := n.openStandalone(ctx, c.StandaloneConfig)
				if err != nil {
					return err
				}
				defer close()
			} else {
				if c.NodeControlKeyFile == "" || c.SchedulerEndpoint == "" {
					return errors.New("native scheduler authentication required")
				}
				// Role-separated payload authentication preserves native transport.
				controlConn, err := grpc.NewClient(c.SchedulerEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return err
				}
				defer controlConn.Close()
				n.Control = func(ctx context.Context, command NativeCommand) (json.RawMessage, error) {
					return InvokeNativeControl(ctx, controlConn, c, command)
				}
			}
		}
		if c.NativeMode {
			drain = func() {
				timeout := time.Duration(c.DrainTimeoutSeconds) * time.Second
				if timeout <= 0 {
					timeout = 2 * time.Minute
				}
				cleanup, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				if err := n.Drain(cleanup); err != nil {
					fmt.Fprintln(os.Stderr, "native storage drain retained pending work:", err)
				}
			}
		}
		handler = n
		readiness = func(ctx context.Context) error {
			return agent.Call(ctx, "GET", base(c.AgentURL)+"/health", nil, nil)
		}
		hook := httpServer(c.HookListen, http.HandlerFunc(n.Hooks))
		defer hook.Close()
		go func() {
			if e := hook.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
				fmt.Fprintln(os.Stderr, e)
			}
		}()
		go n.Recover(ctx)
	} else {
		return errors.New("only the native node plugin role is supported")
	}
	if c.HealthListen != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics)
		mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if e := readiness(ctx); e != nil {
				problem(w, 503, e)
				return
			}
			w.WriteHeader(200)
		})
		h := httpServer(c.HealthListen, mux)
		defer h.Close()
		go h.ListenAndServe()
	}
	h := httpServer(c.Listen, handler)
	h.TLSConfig = serverTLS
	go func() {
		<-ctx.Done()
		if drain != nil {
			drain()
		}
		_ = h.Close()
	}()
	e = h.ListenAndServeTLS("", "")
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
