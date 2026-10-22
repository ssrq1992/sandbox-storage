package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	control "ssf.dev/control"
	adapter "ssf.dev/ssf/adapter/agentenvadapter"
	"ssf.dev/ssf/coordinator"
	"syscall"
	"time"
)

type config struct {
	Listen         string         `json:"listen"`
	HealthListen   string         `json:"health_listen"`
	TLS            control.Config `json:"tls"`
	SchedulerPeers []string       `json:"scheduler_peers"`
	NodePeers      []string       `json:"node_peers"`
	StoragePeers   []string       `json:"storage_peers"`
	Observer       control.Config `json:"observer"`
	RuntimeConfig  string         `json:"runtime_config"`
}

func run(ctx context.Context, path string) error {
	raw, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	var cfg config
	if e = json.Unmarshal(raw, &cfg); e != nil {
		return e
	}
	if cfg.Listen == "" || cfg.HealthListen == "" || len(cfg.NodePeers) == 0 {
		return errors.New("coordinator listeners and node peers required")
	}
	native, e := adapter.LoadNativeConfig(cfg.RuntimeConfig)
	if e != nil {
		return e
	}
	serverTLS, e := control.TLS(cfg.TLS, true)
	if e != nil {
		return e
	}
	host, closeHost, e := adapter.OpenNativeControl(ctx, native)
	if e != nil {
		return e
	}
	defer closeHost()
	plugin := &adapter.ControlService{Control: host, Config: native}
	if native.Standalone == nil {
		plugin.GatewayKey, e = adapter.LoadControlKey(native.GatewayKeyFile)
		if e != nil {
			return e
		}
		observer, err := control.New(cfg.Observer)
		if err != nil {
			return err
		}
		defer observer.Close()
		plugin.Observe = func(ctx context.Context) ([]control.Node, error) {
			r, e := observer.Call(ctx, "ObserveNodes", control.Request{Runtime: "agentenv"})
			return r.Candidates, e
		}
	} else {
		if e = native.Standalone.Validate(); e != nil {
			return e
		}
		plugin.Observe = func(context.Context) ([]control.Node, error) {
			return []control.Node{{ID: native.Standalone.NodeID, Endpoint: native.Standalone.Endpoint}}, nil
		}
	}
	plugin.DispatchKey, e = adapter.LoadControlKey(native.DispatchKeyFile)
	if e != nil {
		return e
	}
	plugin.NodeKey, e = adapter.LoadControlKey(native.NodeKeyFile)
	if e != nil {
		return e
	}
	if string(plugin.DispatchKey) == string(plugin.NodeKey) || (len(plugin.GatewayKey) > 0 && (string(plugin.GatewayKey) == string(plugin.NodeKey) || string(plugin.GatewayKey) == string(plugin.DispatchKey))) {
		return errors.New("control keys must be distinct")
	}
	refresh := host.Refresh
	host.Refresh = func(ctx context.Context, c adapter.NativeCandidate) (adapter.NativeCandidate, error) {
		nodes, err := plugin.Observe(ctx)
		if err != nil {
			return c, err
		}
		for _, n := range nodes {
			if n.ID == c.ID {
				c.Endpoint = n.Endpoint
				return refresh(ctx, c)
			}
		}
		return c, errors.New("native node heartbeat is not ready")
	}
	service := &coordinator.Service{Runtimes: map[string]coordinator.Runtime{"agentenv": plugin}, SchedulerPeers: cfg.SchedulerPeers, NodePeers: cfg.NodePeers, StoragePeers: cfg.StoragePeers}
	plugin.AdmissionAllowed = func() bool { return !service.Draining.Load() }
	server := &http.Server{Addr: cfg.Listen, Handler: service, TLSConfig: serverTLS, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	health := http.NewServeMux()
	health.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	health.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if service.Draining.Load() {
			http.Error(w, "draining", 503)
			return
		}
		c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := host.Ledger.Session(c).QueryRow(c, "SELECT 1").Scan(new(int)); err != nil {
			http.Error(w, "ledger unavailable", 503)
			return
		}
		w.WriteHeader(200)
	})
	probe := &http.Server{Addr: cfg.HealthListen, Handler: health, ReadHeaderTimeout: 5 * time.Second}
	defer probe.Close()
	defer server.Close()
	recovery, cancelRecovery := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		host.RecoverNative(recovery, time.Duration(native.RecoverySeconds)*time.Second)
	}()
	defer func() { cancelRecovery(); <-done }()
	failed := make(chan error, 2)
	go func() { failed <- server.ListenAndServeTLS("", "") }()
	go func() { failed <- probe.ListenAndServe() }()
	select {
	case e = <-failed:
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	case <-ctx.Done():
	}
	service.Draining.Store(true)
	drain, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	return server.Shutdown(drain)
}
func main() {
	path := flag.String("config", "", "coordinator JSON config")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *path); err != nil {
		log.Fatal(err)
	}
}
