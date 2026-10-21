package agentenvadapter

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"ssf.dev/ssf/core"
)

func TestStandaloneRequiresFixedLoopbackEndpoints(t *testing.T) {
	valid := NativeStandalone{NodeID: "native", KubeNode: "host", Endpoint: "http://127.0.0.1:8080", PluginURL: "https://localhost:9443"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"http://remote:8080", "http://user@localhost:8080", "http://localhost:8080/path", "http://localhost:8080?token=x", "https://localhost:8080"} {
		invalid := valid
		invalid.Endpoint = endpoint
		if invalid.Validate() == nil {
			t.Fatalf("accepted untrusted endpoint %s", endpoint)
		}
	}
}

func TestStandaloneAdmissionPreservesBodyAndRejectsOtherAPIs(t *testing.T) {
	body := []byte("{ \"volumeMounts\": [], \"metadata\": {\"value\":\"exact bytes\"} }")
	calls := 0
	n := &Node{LocalPrepare: func(_ context.Context, q NativeIntent) (NativeDispatch, error) {
		calls++
		if string(q.Body) != string(body) || q.BodyDigest != core.Hash(string(body)) || q.Tenant != "" || q.FixedNode != "" {
			t.Fatalf("mutated public body or accepted identity: %+v", q)
		}
		return NativeDispatch{}, core.ErrPending
	}}
	for _, path := range []string{"/sandboxes", "/sandboxes-cold", "/v2/sandboxes", "/admin"} {
		raw, _ := json.Marshal(map[string]any{"request_id": "request-1", "path": path, "body": body})
		w := httptest.NewRecorder()
		n.nativeLocalAdmit(w, httptest.NewRequest("POST", "/storage/local-admit", strings.NewReader(string(raw))))
		want := 409
		if path == "/admin" {
			want = 400
		}
		if w.Code != want {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
	if calls != 3 {
		t.Fatalf("admitted invalid API: %d", calls)
	}
	disabled := &Node{}
	w := httptest.NewRecorder()
	disabled.nativeLocalAdmit(w, httptest.NewRequest("POST", "/storage/local-admit", strings.NewReader("{}")))
	if w.Code != 409 {
		t.Fatal("disabled standalone accepted request")
	}
}
