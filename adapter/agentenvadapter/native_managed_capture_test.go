package agentenvadapter

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"reflect"
	"testing"

	"google.golang.org/grpc"
	api "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/core"
)

type captureOrderingClient struct {
	api.RuntimeStorageNodeClient
	calls       []string
	failPrepare string
}

func (c *captureOrderingClient) PrepareCapture(_ context.Context, q *api.DriverCaptureRequest, _ ...grpc.CallOption) (*api.DriverCaptureResult, error) {
	c.calls = append(c.calls, "prepare/"+q.BindingUid)
	if q.BindingUid == c.failPrepare {
		return nil, errors.New("capture interrupted")
	}
	return &api.DriverCaptureResult{BindingUid: q.BindingUid, CaptureId: core.Hash("checkpoint/" + q.BindingUid), BarrierId: "barrier", State: "Captured"}, nil
}
func (c *captureOrderingClient) PublishCapture(_ context.Context, q *api.DriverCaptureRequest, _ ...grpc.CallOption) (*api.DriverCaptureResult, error) {
	c.calls = append(c.calls, "publish/"+q.BindingUid)
	return nil, errors.New("upload interrupted")
}
func TestManagedCaptureSealsEveryParticipantBeforePublication(t *testing.T) {
	for _, fail := range []string{"", "b"} {
		t.Run("prepare_failure_"+fail, func(t *testing.T) {
			_, key, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			client := &captureOrderingClient{failPrepare: fail}
			control := &NativeControl{SigningKey: key, CaptureNode: func(context.Context, core.Placement) (api.RuntimeStorageNodeClient, error) { return client, nil }}
			bindings := []core.Binding{{ID: "a", Volume: core.Volume{Driver: "overlaybd.csi.ssf.dev"}}, {ID: "legacy", Volume: core.Volume{Driver: "other"}}, {ID: "b", Volume: core.Volume{Driver: "overlaybd.csi.ssf.dev"}}}
			err = control.captureParticipants(NativeDispatch{})(context.Background(), core.CaptureIntent{ID: "checkpoint", BarrierID: "barrier"}, bindings, "proof")
			if err == nil {
				t.Fatal("interruption ignored")
			}
			want := []string{"prepare/a", "prepare/b"}
			if fail == "" {
				want = append(want, "publish/a")
			}
			if !reflect.DeepEqual(client.calls, want) {
				t.Fatalf("calls=%v want=%v", client.calls, want)
			}
		})
	}
}
