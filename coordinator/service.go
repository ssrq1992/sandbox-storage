// Package coordinator hosts runtime plugins without importing runtime-specific
// types. The shared ledger is transactional implementation, not a database RPC.
package coordinator

import (
	"context"
	"errors"
	"net/http"
	control "ssf.dev/control"
	"strings"
	"sync/atomic"
)

type Runtime interface {
	Call(context.Context, string, control.Request) (control.Response, error)
}
type Service struct {
	Runtimes                                map[string]Runtime
	SchedulerPeers, NodePeers, StoragePeers []string
	Draining                                atomic.Bool
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := strings.TrimPrefix(r.URL.Path, "/"+control.Version+"/")
	if r.URL.Path == method {
		http.NotFound(w, r)
		return
	}
	allowed := s.SchedulerPeers
	switch method {
	case "ResolveLaunch", "PrepareLaunch", "InspectOperation":
	case "CaptureVolume":
		allowed = s.StoragePeers
	case "InvokeLifecycle", "LocalLaunch":
		allowed = s.NodePeers
	default:
		http.NotFound(w, r)
		return
	}
	if !control.HasPeer(r.TLS, allowed) {
		http.Error(w, "control peer denied", http.StatusForbidden)
		return
	}
	q, e := control.Decode(w, r)
	if e != nil {
		control.Write(w, control.Response{}, &control.Error{Code: "invalid", Message: e.Error()})
		return
	}
	plugin, ok := s.Runtimes[q.Runtime]
	if !ok {
		control.Write(w, control.Response{}, &control.Error{Code: "unsupported", Message: "runtime not registered"})
		return
	}
	if s.Draining.Load() && (method == "ResolveLaunch" || method == "PrepareLaunch" || method == "LocalLaunch") {
		control.Write(w, control.Response{}, errors.New("coordinator draining"))
		return
	}
	out, e := plugin.Call(r.Context(), method, q)
	control.Write(w, out, e)
}
