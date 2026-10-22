package agentenvadapter

import (
	"bytes"
	"context"
	"encoding/json"
	control "ssf.dev/control"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	"sync"
	"testing"
	"time"
)

func TestRemoteResolvePinsTemplateAndPreservesOriginalSelection(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	if _, e := c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_runs WHERE id=$1`, id); e != nil {
		t.Fatal(e)
	}
	c.Volumes = &NativeVolumes{Ledger: c.Ledger}
	c.Refresh = func(_ context.Context, n NativeCandidate) (NativeCandidate, error) {
		n.KubeNode = n.ID
		n.NodeUID = n.ID + "uid"
		n.BootID = "boot"
		n.Capacity = Resources{CPU: 8, MemoryMiB: 8192}
		n.Defaults = Resources{CPU: 1, MemoryMiB: 128}
		return n, nil
	}
	projections := 0
	c.ProjectTemplate = func(context.Context, string, []NativeCandidate) (NativeTemplate, error) {
		projections++
		return NativeTemplate{ID: "immutable", Resources: Resources{CPU: 1, MemoryMiB: 128}}, nil
	}
	key := bytes.Repeat([]byte("g"), 32)
	s := &ControlService{Control: c, GatewayKey: key, DispatchKey: bytes.Repeat([]byte("d"), 32), Observe: func(context.Context) ([]control.Node, error) {
		return []control.Node{{ID: "a", Endpoint: "http://a"}, {ID: "b", Endpoint: "http://b"}}, nil
	}}
	body := []byte(`{"templateID":"alias"}`)
	signed, e := auth.SignJSONEnvelope(key, NativeStorageProtocol, "gateway", id, map[string]any{"method": "POST", "path": "/sandboxes", "body": body, "body_digest": core.Hash(string(body))}, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	q := control.Request{Runtime: "agentenv", Envelope: signed, Candidates: []control.Node{{ID: "a"}, {ID: "b"}}}
	for i := 0; i < 2; i++ {
		r, e := s.Call(ctx, "ResolveLaunch", q)
		if e != nil || r.LaunchID != id || len(r.Candidates) != 2 {
			t.Fatalf("resolve %+v %v", r, e)
		}
	}
	if projections != 1 {
		t.Fatal("mutable template alias re-resolved", projections)
	}
	var count int
	if e = c.Ledger.Pool.QueryRow(ctx, `SELECT count(*) FROM agentenv_native_reservations WHERE id=$1`, "native/"+id).Scan(&count); e != nil || count != 0 {
		t.Fatal("resolve reserved compute", e)
	}
	q.Selected = "b"
	q.LaunchID = id
	old, err := auth.SignJSONEnvelope(key, NativeStorageProtocol, "gateway", id, map[string]any{"method": "POST", "path": "/sandboxes", "body": body, "body_digest": core.Hash(string(body))}, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	q.Envelope = old
	if _, err = s.Call(ctx, "ResolveLaunch", q); err == nil {
		t.Fatal("expired signature opened a new resolution")
	}

	first, e := s.Call(ctx, "PrepareLaunch", q)
	if e != nil || first.Selected.ID != "b" {
		t.Fatalf("server chose placement: %+v %v", first, e)
	}
	inspected, e := s.Call(ctx, "InspectOperation", q)
	if e != nil || inspected.Selected.ID != "b" || len(inspected.Dispatch) == 0 {
		t.Fatal("lost dispatch cannot be recovered", e)
	}
	// A second process opening the same ledger must return the first run.
	other := *s
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := other.Call(ctx, "PrepareLaunch", q)
			if e != nil || r.Selected.ID != "b" {
				t.Errorf("duplicate dispatch %+v %v", r, e)
			}
		}()
	}
	wg.Wait()
	var a, b auth.JSONEnvelope
	_ = json.Unmarshal(first.Dispatch, &a)
	_ = json.Unmarshal(inspected.Dispatch, &b)
	if !bytes.Equal(a.Payload, b.Payload) {
		t.Fatal("dispatch changed after lost response")
	}
	changed := q
	changed.Envelope = bytes.Repeat([]byte("x"), 32)
	if _, e = s.Call(ctx, "PrepareLaunch", changed); e == nil {
		t.Fatal("unsigned request accepted")
	}
	t.Cleanup(func() {
		_, _ = c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_reservations WHERE id=$1`, "native/"+id)
	})
}

func TestInspectRetainsUncertainReservationAndRejectsReselection(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	if _, e := c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_runs WHERE id=$1`, id); e != nil {
		t.Fatal(e)
	}
	a := NativeCandidate{ID: id + "a", Endpoint: "http://a", KubeNode: "worker-a", NodeUID: "a", BootID: "boot", Capacity: Resources{8, 8192}, Defaults: Resources{1, 128}}
	b := a
	b.ID = id + "b"
	b.Endpoint = "http://b"
	b.KubeNode = "worker-b"
	b.NodeUID = "b"
	c.Refresh = func(_ context.Context, n NativeCandidate) (NativeCandidate, error) {
		if n.ID == a.ID {
			return a, nil
		}
		return b, nil
	}
	q := NativeIntent{RequestID: id, Tenant: "test", Path: "/sandboxes-cold", Body: []byte(`{"cpuCount":1,"memoryMB":128}`)}
	q.BodyDigest = core.Hash(string(q.Body))
	d, e := c.prepareLaunch(ctx, q, []NativeCandidate{a, b}, nil, true)
	if e != nil {
		t.Fatal(e)
	}
	admission := NativeAdmission{Ledger: c.Ledger, Resources: d.Resources, Candidates: []NativeCandidate{a}, Refresh: c.Refresh, Choose: fixedChoice}
	if _, e = admission.Reserve(ctx, "native/"+id, nil); e != nil {
		t.Fatal(e)
	}
	service := ControlService{Control: c}
	state, e := service.inspect(ctx, q)
	if e != nil || state.Selected == nil || state.Selected.ID != a.ID || len(state.Dispatch) != 0 {
		t.Fatalf("lost reservation: %+v %v", state, e)
	}
	admission.Candidates = []NativeCandidate{b}
	if _, e = admission.Reserve(ctx, "native/"+id, nil); e == nil {
		t.Fatal("unknown reservation moved to a second node")
	}
	recovered, e := c.Prepare(ctx, q, []NativeCandidate{a}, fixedChoice)
	if e != nil || recovered.Candidate.ID != a.ID || recovered.RunUID != d.RunUID {
		t.Fatalf("same launch not recovered: %+v %v", recovered, e)
	}
	t.Cleanup(func() {
		_, _ = c.Ledger.Pool.Exec(ctx, `DELETE FROM agentenv_native_reservations WHERE id=$1`, "native/"+id)
	})
}

func TestCoordinatorDrainRejectsNewLifecycleWritersBeforeSideEffects(t *testing.T) {
	key := bytes.Repeat([]byte("n"), 32)
	s := ControlService{NodeKey: key, AdmissionAllowed: func() bool { return false }}
	for _, action := range []string{"resume", "fork", "compute"} {
		envelope, e := auth.SignJSONEnvelope(key, NativeStorageProtocol, "node", "event", NativeCommand{Action: action, NodeID: "node"}, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.Call(context.Background(), "InvokeLifecycle", control.Request{Envelope: envelope}); e != core.ErrPending {
			t.Fatalf("drain admitted %s: %v", action, e)
		}
	}
}
