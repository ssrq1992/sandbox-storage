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
	sandboxsched "ssf.dev/ssf/adapter/sandboxschedadapter"
	"ssf.dev/ssf/coordinator"
	"syscall"
	"time"
)

type config struct {
	Listen                    string         `json:"listen"`
	HealthListen              string         `json:"health_listen"`
	TLS                       control.Config `json:"tls"`
	SchedulerPeers            []string       `json:"scheduler_peers"`
	NodePeers                 []string       `json:"node_peers"`
	StoragePeers              []string       `json:"storage_peers"`
	SandboxSchedRuntimeConfig string         `json:"sandboxsched_runtime_config"`
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
	if cfg.SandboxSchedRuntimeConfig == "" {
		return errors.New("sandboxsched_runtime_config required")
	}
	schedConfig, e := sandboxsched.LoadControlConfig(cfg.SandboxSchedRuntimeConfig)
	if e != nil {
		return e
	}
	serverTLS, e := control.TLS(cfg.TLS, true)
	if e != nil {
		return e
	}
	host, closeSched, e := sandboxsched.OpenSandboxSchedControl(ctx, schedConfig)
	if e != nil {
		return e
	}
	defer closeSched()
	runtimes := map[string]coordinator.Runtime{"sandboxsched": &sandboxsched.ControlService{Control: host, Config: schedConfig}}
	service := &coordinator.Service{Runtimes: runtimes, SchedulerPeers: cfg.SchedulerPeers, NodePeers: cfg.NodePeers, StoragePeers: cfg.StoragePeers}
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
