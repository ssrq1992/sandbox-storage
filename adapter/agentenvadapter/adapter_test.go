package agentenvadapter

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
	"sync"
	"testing"
	"time"
)

func testNode(t *testing.T) (*Node, StartRequest) {
	t.Helper()
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	j, e := OpenJournal(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { j.Close() })
	n := &Node{Journal: j, Key: pub, Config: Config{NodeName: "n", NodeUID: "uid", RuntimeRoot: t.TempDir()}, BootID: "boot", Context: context.Background()}
	p := core.Plan{ID: "plan", Operation: "consume-op", Sandbox: "instance", Node: "n", NodeUID: "uid", BootID: "boot", Expires: time.Now().Add(time.Minute), Bindings: []core.Binding{{ID: "b", Volume: core.Volume{Capability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}}}}}
	token, e := auth.Sign(p, key)
	if e != nil {
		t.Fatal(e)
	}
	q := StartRequest{ID: "op", Instance: "instance", Operation: "consume-op", Plan: token}
	return n, q
}
func TestDurableCancelPreventsLateLaunch(t *testing.T) {
	n, q := testNode(t)
	r, e := n.Register(q, true)
	if e != nil {
		t.Fatal(e)
	}
	if e = n.stop(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	r, e = n.Register(q, false)
	if e != nil || !r.Stop {
		t.Fatalf("cancel lost: %+v %v", r, e)
	}
	if e = n.Work(context.Background(), q.ID); e != nil {
		t.Fatal(e)
	}
	r, _ = n.Journal.Get(q.ID)
	if r.Dispatched || r.State != "Stopped" {
		t.Fatal(r)
	}
	if e = n.verifyRuntime(r, "detached"); e != nil {
		t.Fatal(e)
	}
	q.Resources.CPU = 3
	if _, e = n.Register(q, false); !errors.Is(e, core.ErrConflict) {
		t.Fatal("conflicting request accepted", e)
	}
}
func TestUnknownNativeCreateRetainsResources(t *testing.T) {
	n, q := testNode(t)
	_, e := n.Register(q, false)
	if e != nil {
		t.Fatal(e)
	}
	r, e := n.Journal.Update(q.ID, func(r *NodeRecord) error { r.Dispatched = true; r.Stop = true; return nil })
	if e != nil {
		t.Fatal(e)
	}
	if e = n.stop(context.Background(), r); e == nil {
		t.Fatal("unknown dispatched create considered stopped")
	}
	r, _ = n.Journal.Get(q.ID)
	if r.State == "Stopped" {
		t.Fatal("unknown outcome discarded")
	}
}
func TestNativeInstanceBindingAndEventOrdering(t *testing.T) {
	n, q := testNode(t)
	r, e := n.Register(q, false)
	if e != nil {
		t.Fatal(e)
	}
	ev := RuntimeEvent{Sequence: 1, Ticket: r.Ticket, SandboxID: "sandbox", InstanceID: "native-1", BootID: "boot", Event: "prepare"}
	if e = n.acceptEvent(ev); e != nil {
		t.Fatal(e)
	}
	old := ev
	ev.InstanceID = "native-2"
	if e = n.acceptEvent(ev); e == nil {
		t.Fatal("duplicate native instance accepted")
	}
	ev = old
	ev.Event = "stopped"
	ev.Sequence = 2
	data, _ := json.Marshal(ev)
	if e = os.WriteFile(filepath.Join(n.Config.RuntimeRoot, ev.InstanceID+".json"), data, 0600); e != nil {
		t.Fatal(e)
	}
	if e = n.acceptEvent(ev); e != nil {
		t.Fatal(e)
	}
	if e = n.acceptEvent(old); e != nil {
		t.Fatal(e)
	}
	r, _ = n.Journal.Get(q.ID)
	if r.Runtime.Event != "stopped" {
		t.Fatal("late prepare resurrected stopped runtime")
	}
	if e = n.verifyRuntime(r, "detached"); e != nil {
		t.Fatal(e)
	}
}
func TestJournalConcurrentUpdatesAndRestart(t *testing.T) {
	root := t.TempDir()
	j, e := OpenJournal(root)
	if e != nil {
		t.Fatal(e)
	}
	_, e = j.Update("a", func(r *NodeRecord) error { r.Ticket = "secret"; r.Format = map[string]string{}; return nil })
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := j.Update("a", func(r *NodeRecord) error { r.Runtime.Sequence++; return nil })
			if e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	j.Close()
	j, e = OpenJournal(root)
	if e != nil {
		t.Fatal(e)
	}
	defer j.Close()
	r, e := j.ByTicket("secret")
	if e != nil || r.Runtime.Sequence != 30 {
		t.Fatalf("durability: %+v %v", r, e)
	}
}
func TestRebootAndPodReplacementEvidence(t *testing.T) {
	n, q := testNode(t)
	r, e := n.Register(q, false)
	if e != nil {
		t.Fatal(e)
	}
	n.PIDNamespace = "different"
	if n.Work(context.Background(), q.ID) == nil {
		t.Fatal("PID namespace replacement silently accepted")
	}
	if n.verifyRuntime(r, "detached") == nil {
		t.Fatal("namespace absence treated as fencing")
	}
	n.BootID = "new-boot"
	if e = n.Work(context.Background(), q.ID); e != nil {
		t.Fatal(e)
	}
	r, _ = n.Journal.Get(q.ID)
	if !r.Stop || n.verifyRuntime(r, "detached") != nil {
		t.Fatal("host reboot not recovered")
	}
	n.Config.NodeUID = "replacement"
	if e = n.Work(context.Background(), q.ID); e == nil {
		t.Fatal("node UID reuse accepted")
	}
}
func TestLostInitialCallbackRecoveredFromNativeJournal(t *testing.T) {
	n, q := testNode(t)
	r, e := n.Register(q, false)
	if e != nil {
		t.Fatal(e)
	}
	_, e = n.Journal.Update(q.ID, func(x *NodeRecord) error { x.Dispatched = true; return nil })
	if e != nil {
		t.Fatal(e)
	}
	ev := RuntimeEvent{Sequence: 2, Ticket: r.Ticket, SandboxID: "sandbox", InstanceID: "native", BootID: n.BootID, Event: "stopped"}
	b, _ := json.Marshal(ev)
	if e = os.WriteFile(filepath.Join(n.Config.RuntimeRoot, "native.json"), b, 0600); e != nil {
		t.Fatal(e)
	}
	if e = n.Work(context.Background(), q.ID); e != nil {
		t.Fatal(e)
	}
	r, _ = n.Journal.Get(q.ID)
	if r.State != "Stopped" || r.Runtime.InstanceID != "native" {
		t.Fatal("lost notification not recovered", r)
	}
}
func TestRevocationBeforeDelayedPlan(t *testing.T) {
	n, q := testNode(t)
	_, e := n.Journal.Update(q.ID, func(r *NodeRecord) error {
		*r = NodeRecord{Request: StartRequest{ID: q.ID, Instance: q.Instance}, Ticket: newID(), State: "Revoked", Stop: true}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	r, e := n.Register(q, false)
	if e != nil || !r.Stop {
		t.Fatal("late start bypassed revocation", e)
	}
	if e = n.Work(context.Background(), q.ID); e != nil {
		t.Fatal(e)
	}
	r, _ = n.Journal.Get(q.ID)
	if r.Dispatched || r.State != "Stopped" {
		t.Fatal(r)
	}
}
func TestPlanRefreshCannotChangeBindings(t *testing.T) {
	n, q := testNode(t)
	p, e := auth.Verify(q.Plan, n.Key, n.Config.NodeName, n.BootID, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	n.Key = pub
	q.Plan, _ = auth.Sign(p, key)
	if _, e = n.Register(q, false); e != nil {
		t.Fatal(e)
	}
	p.Expires = time.Now().Add(2 * time.Minute)
	q.Plan, _ = auth.Sign(p, key)
	if _, e = n.Register(q, false); e != nil {
		t.Fatal("same plan renewal rejected", e)
	}
	p.Bindings[0].Volume.Handle = "different"
	q.Plan, _ = auth.Sign(p, key)
	if _, e = n.Register(q, false); !errors.Is(e, core.ErrConflict) {
		t.Fatal("plan replacement accepted", e)
	}
}

func TestInitializeRequiresEntireDeviceEmpty(t *testing.T) {
	if e := checkZero(context.Background(), bytes.NewReader(make([]byte, 2<<20))); e != nil {
		t.Fatal(e)
	}
	b := make([]byte, (2<<20)+1)
	b[len(b)-1] = 1
	if checkZero(context.Background(), bytes.NewReader(b)) == nil {
		t.Fatal("unrecognized existing data would be formatted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(checkZero(ctx, bytes.NewReader(nil)), context.Canceled) {
		t.Fatal("cancelled scan accepted")
	}
}

func TestPeerIdentityRequiresVerifiedSingleURI(t *testing.T) {
	req := httptest.NewRequest("GET", "https://node/v1/info", nil)
	n, _ := testNode(t)
	n.Config.ControlPeers = []string{"spiffe://ssf/control"}
	w := httptest.NewRecorder()
	n.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal("unauthenticated peer admitted", w.Code)
	}
	uri, _ := url.Parse("spiffe://ssf/control")
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{URIs: []*url.URL{uri}}}}
	if identity(req) != "" {
		t.Fatal("unverified certificate admitted")
	}
	req.TLS.VerifiedChains = [][]*x509.Certificate{{{URIs: []*url.URL{uri, uri}}}}
	if identity(req) != "" {
		t.Fatal("ambiguous peer identity admitted")
	}
}
