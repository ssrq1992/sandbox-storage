package coordinator

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	control "ssf.dev/control"
	"testing"
)

type runtimeFunc func(context.Context, string, control.Request) (control.Response, error)

func (f runtimeFunc) Call(c context.Context, m string, q control.Request) (control.Response, error) {
	return f(c, m, q)
}
func TestRoleSeparationProtocolAndDrain(t *testing.T) {
	calls := 0
	s := &Service{SchedulerPeers: []string{"spiffe://scheduler"}, NodePeers: []string{"spiffe://node"}, StoragePeers: []string{"spiffe://storage"}, Runtimes: map[string]Runtime{"agentenv": runtimeFunc(func(context.Context, string, control.Request) (control.Response, error) {
		calls++
		return control.Response{}, nil
	})}}
	invoke := func(role, method, version string) int {
		raw, _ := json.Marshal(control.Request{Version: version, Runtime: "agentenv"})
		r := httptest.NewRequest("POST", "/"+control.Version+"/"+method, bytes.NewReader(raw))
		uri, _ := url.Parse(role)
		r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{uri}}}}}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w.Code
	}
	if invoke("spiffe://node", "PrepareLaunch", control.Version) != 403 || invoke("spiffe://scheduler", "InvokeLifecycle", control.Version) != 403 {
		t.Fatal("role bypass")
	}
	if invoke("spiffe://scheduler", "ResolveLaunch", "old") != 409 || calls != 0 {
		t.Fatal("old protocol executed")
	}
	if invoke("spiffe://scheduler", "ResolveLaunch", control.Version) != 200 {
		t.Fatal("registered runtime rejected")
	}
	if invoke("spiffe://storage", "PrepareLaunch", control.Version) != 403 || invoke("spiffe://node", "CaptureVolume", control.Version) != 403 || invoke("spiffe://scheduler", "CaptureVolume", control.Version) != 403 {
		t.Fatal("storage role bypass")
	}
	if invoke("spiffe://storage", "CaptureVolume", control.Version) != 200 {
		t.Fatal("storage capture denied")
	}
	s.Draining.Store(true)
	if invoke("spiffe://scheduler", "PrepareLaunch", control.Version) != 409 {
		t.Fatal("admitted while draining")
	}
	if invoke("spiffe://node", "InvokeLifecycle", control.Version) != 200 {
		t.Fatal("drain prevented stop evidence")
	}
}
