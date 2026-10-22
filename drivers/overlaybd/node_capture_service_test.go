package overlaybd

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

type captureVerifier struct {
	paused bool
	calls  int
}

func (v *captureVerifier) VerifyCapture(_ context.Context, q core.CaptureIntent, bs []core.Binding, proof string) error {
	v.calls++
	if !v.paused || proof != "trusted" || len(bs) != 1 || q.RunUID != bs[0].Run() {
		return errors.New("barrier not held")
	}
	return nil
}
func TestManagedCaptureVerifiesBarrierAndReplaysAfterThaw(t *testing.T) {
	ctx := context.Background()
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	wal, err := OpenSessions(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	b := core.Binding{ID: "binding", OwnerUID: "owner", RunUID: "run", Epoch: 1, Node: "node", Tenant: "tenant", Sandbox: "sandbox", Volume: core.Volume{Driver: DriverName}}
	p := core.Plan{ID: "plan", Operation: "operation", Node: "node", NodeUID: "uid", BootID: "boot", Tenant: b.Tenant, Sandbox: b.Sandbox, Expires: time.Now().Add(time.Hour), NodeIDs: map[string]string{DriverName: "csi-node"}, Bindings: []core.Binding{b}}
	token, err := auth.Sign(p, key)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := p
	unsigned.ID = ""
	unsigned.Expires = time.Time{}
	i := intent()
	i.ID = core.Hash("overlaybd/session/" + b.ID)
	i.OwnerID = b.OwnerUID
	i.RunID = b.RunUID
	i.Epoch = b.Epoch
	i.AuthorizationDigest = core.Digest(unsigned)
	i.HeadRevision = core.Hash("head")
	s, err := wal.Plan(i)
	s = mustSession(t, s, err)
	s, err = wal.BeginCreate(s)
	s = mustSession(t, s, err)
	e := evidence()
	e.SessionID = i.ID
	e.Epoch = i.Epoch
	e.HeadRevision = i.HeadRevision
	e.CapacityBytes = 4096
	e.FSUUID = "fs"
	s, err = wal.RecordDevice(s, e)
	mustSession(t, s, err)
	verifier := &captureVerifier{paused: true}
	backend := &finalBackend{}
	node := &CSINode{Operations: &NodeOperations{Sessions: wal, Backend: backend}, CaptureEvidence: verifier, Authorization: NodeAuthorization{Key: pub, Node: "node", NodeUID: "uid", Boot: "boot", CSINode: "csi-node"}}
	service := &ManagedNodeService{Node: node}
	intent := core.CaptureIntent{Version: core.CaptureVersion, ID: "capture", Operation: p.Operation, OwnerUID: b.OwnerUID, RunUID: b.RunUID, BarrierID: "barrier", NativeArtifact: "artifact", NativeDigest: core.Hash("native"), Classes: map[string]string{b.ID: "class"}}
	raw, _ := json.Marshal(intent)
	q := &v1.DriverCaptureRequest{SignedNodePlan: token, BindingUid: b.ID, CaptureIntentJson: string(raw), Evidence: "forged"}
	if _, err = service.PrepareCapture(ctx, q); err == nil || backend.captures != 0 {
		t.Fatal("unverified barrier captured")
	}
	q.Evidence = "trusted"
	result, err := service.PrepareCapture(ctx, q)
	if err != nil || result.State != "Captured" {
		t.Fatalf("capture: %v %v", result, err)
	}
	verifier.paused = false
	q.Evidence = ""
	if _, err = service.PrepareCapture(ctx, q); err != nil {
		t.Fatalf("durable replay required live barrier: %v", err)
	}
	result, err = service.PublishCapture(ctx, q)
	if err != nil || result.State != "Published" {
		t.Fatalf("publication after thaw: %v %v", result, err)
	}
	if backend.captures != 1 || verifier.calls != 2 {
		t.Fatalf("reverified or recaptured: %d %d", backend.captures, verifier.calls)
	}
	intent.NativeDigest = core.Hash("replacement")
	raw, _ = json.Marshal(intent)
	q.CaptureIntentJson = string(raw)
	if _, err = service.PublishCapture(ctx, q); err == nil {
		t.Fatal("replaced native capture accepted")
	}
	intent.ID = "new-capture"
	raw, _ = json.Marshal(intent)
	q.CaptureIntentJson = string(raw)
	q.Evidence = "trusted"
	if _, err = service.PrepareCapture(ctx, q); err == nil || backend.captures != 1 {
		t.Fatal("new capture allowed after thaw")
	}
}
