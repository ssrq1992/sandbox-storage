package node

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/core"
	driver "ssf.dev/ssf/drivers/csi"
	"testing"
)

type captureClientFixture struct {
	v1.ManagedStorageDriverClient
	calls int
	wrong bool
}

func (m *captureClientFixture) PrepareCapture(_ context.Context, q *v1.DriverCaptureRequest, _ ...grpc.CallOption) (*v1.DriverCaptureResult, error) {
	m.calls++
	var intent core.CaptureIntent
	_ = json.Unmarshal([]byte(q.CaptureIntentJson), &intent)
	binding := q.BindingUid
	if m.wrong {
		binding = "other"
	}
	return &v1.DriverCaptureResult{BindingUid: binding, CaptureId: core.Hash(intent.ID + "/" + q.BindingUid), BarrierId: intent.BarrierID, State: "Captured", SealedRevision: core.Hash("sealed")}, nil
}

type captureSessionFixture struct {
	node    *fakeNode
	client  *captureClientFixture
	enabled bool
}

func (s *captureSessionFixture) Node(context.Context, string) (*driver.Session, error) {
	return &driver.Session{Node: s.node, Managed: s.client, Capture: s.enabled, NodeID: "csi-node", Stage: true}, nil
}
func TestCaptureRoutingRequiresReadyAndCapabilityAndMatchingReceipt(t *testing.T) {
	ctx := context.Background()
	engine, f, key := setup(t)
	token := plan(key, "run")
	client := &captureClientFixture{}
	sessions := &captureSessionFixture{node: f, client: client}
	engine.Sessions = sessions
	if _, err := engine.Prepare(ctx, "run", token); err != nil {
		t.Fatal(err)
	}
	intent := core.CaptureIntent{Version: core.CaptureVersion, ID: "capture", Operation: "run", OwnerUID: "run", RunUID: "run", BarrierID: "barrier", NativeArtifact: "native", NativeDigest: core.Hash("native"), Classes: map[string]string{"run": "class"}}
	raw, _ := json.Marshal(intent)
	q := &v1.DriverCaptureRequest{SignedNodePlan: token, BindingUid: "run", CaptureIntentJson: string(raw), Evidence: "trusted"}
	service := &Service{Engine: engine}
	if _, err := service.PrepareCapture(ctx, q); err == nil || client.calls != 0 {
		t.Fatal("uncommitted run captured")
	}
	if _, err := engine.Ready(ctx, "run", "run", "verified-test-state"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.PrepareCapture(ctx, q); err == nil || client.calls != 0 {
		t.Fatal("unnegotiated capture forwarded")
	}
	sessions.enabled = true
	if _, err := service.PrepareCapture(ctx, q); err != nil {
		t.Fatal(err)
	}
	client.wrong = true
	if _, err := service.PrepareCapture(ctx, q); err == nil {
		t.Fatal("foreign receipt accepted")
	}
}
