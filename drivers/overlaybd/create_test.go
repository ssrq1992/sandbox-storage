package overlaybd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

type replyLostBackend struct {
	creates, inspects int
	receipt           *api.VolumeReceipt
}

func (b *replyLostBackend) EnsureVolume(_ context.Context, r *api.EnsureVolumeRequest, _ ...grpc.CallOption) (*api.VolumeReceipt, error) {
	b.creates++
	b.receipt = &api.VolumeReceipt{Operation: proto.Clone(r.Operation).(*api.OperationIdentity), State: "ContentReady", HeadRevision: strings.Repeat("a", 64), CapacityBytes: r.CapacityBytes, FsUuid: "9f0558e1-18cc-4a4a-adf9-51c6eb0efdb3", CapacityGeneration: 1, ContentDigest: strings.Repeat("a", 64)}
	return nil, context.DeadlineExceeded
}
func (b *replyLostBackend) InspectVolume(_ context.Context, r *api.InspectVolumeRequest, _ ...grpc.CallOption) (*api.VolumeReceipt, error) {
	b.inspects++
	if b.receipt == nil {
		return nil, fmt.Errorf("worker result unknown")
	}
	if !proto.Equal(r.Operation, b.receipt.Operation) {
		return nil, ErrConflict
	}
	return proto.Clone(b.receipt).(*api.VolumeReceipt), nil
}
func TestPostgresLostCreateResponseInspectsOriginalOperation(t *testing.T) {
	j := database(t)
	backend := new(replyLostBackend)
	creator := &Creator{Journal: j, Backend: backend}
	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	intent := CreateIntent{RepositoryID: "repo", CapacityBytes: 1 << 30, ProfileRevision: Profile}
	if _, err := creator.Ensure(context.Background(), name, intent); err == nil {
		t.Fatal("lost reply reported success")
	}
	// A fresh service instance must inspect, not invoke EnsureVolume again.
	creator = &Creator{Journal: j, Backend: backend}
	receipt, err := creator.Ensure(context.Background(), name, intent)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := creator.Ensure(context.Background(), name, intent)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(receipt, replay) || backend.creates != 1 || backend.inspects != 1 {
		t.Fatal("retry repeated native create or lost receipt")
	}
	intent.CapacityBytes *= 2
	if _, err := creator.Ensure(context.Background(), name, intent); err == nil {
		t.Fatal("changed capacity accepted")
	}
}

func TestPostgresClaimWithoutKnownDispatchNeverReformats(t *testing.T) {
	j := database(t)
	backend := new(replyLostBackend)
	intent := CreateIntent{RepositoryID: "repo", CapacityBytes: 1 << 30, ProfileRevision: Profile}
	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	operation, err := j.Reserve(context.Background(), "create", name, Identity("volume", name), intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.Finish(context.Background(), operation, Applying, nil, ""); err != nil {
		t.Fatal(err)
	}
	creator := &Creator{Journal: j, Backend: backend}
	for i := 0; i < 2; i++ {
		if _, err := creator.Ensure(context.Background(), name, intent); err == nil {
			t.Fatal("unproven operation succeeded")
		}
	}
	if backend.creates != 0 || backend.inspects != 2 {
		t.Fatal("unknown dispatch retried side effect")
	}
}

func TestNativeCreateIntentGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/create-intent.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Input  CreateIntent `json:"input"`
		Digest string       `json:"digest"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		raw, err := canonical(fixture.Input)
		if err != nil {
			t.Fatal(err)
		}
		if digest(raw) != fixture.Digest {
			t.Fatalf("cross-language identity mismatch: %s", raw)
		}
	}
}
