package overlaybd

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
)

type finalBackend struct {
	api.NativeStorageBackendClient
	captures, publishes, releases int
	losePublish                   bool
	expectedStopped               bool
}

func (b *finalBackend) CaptureDevice(_ context.Context, r *api.CaptureDeviceRequest, _ ...grpc.CallOption) (*api.CaptureReceipt, error) {
	b.captures++
	if b.expectedStopped && (!validDigest(r.StoppedEvidenceDigest) || r.StoppedEvidenceDigest != r.FreezeEvidenceDigest) {
		return nil, errors.New("stopped recovery lost authenticated evidence")
	}
	return &api.CaptureReceipt{Operation: proto.Clone(r.Operation).(*api.OperationIdentity), SessionId: r.SessionId, CaptureId: r.CaptureId, BarrierId: r.BarrierId, State: "Captured", ContentDigest: core.Hash("captured"), SealedRevision: core.Hash("captured")}, nil
}
func (b *finalBackend) PublishHead(_ context.Context, r *api.PublishHeadRequest, _ ...grpc.CallOption) (*api.VolumeReceipt, error) {
	b.publishes++
	if b.losePublish {
		b.losePublish = false
		return nil, context.DeadlineExceeded
	}
	return &api.VolumeReceipt{Operation: proto.Clone(r.Operation).(*api.OperationIdentity), State: "ContentReady", HeadRevision: core.Hash("published"), ContentDigest: core.Hash("published"), CapacityBytes: 4096, CapacityGeneration: 1, FsUuid: "fs"}, nil
}
func (b *finalBackend) ReleaseDevice(_ context.Context, r *api.ReleaseDeviceRequest, _ ...grpc.CallOption) (*api.OperationReceipt, error) {
	b.releases++
	if r.FinalCaptureId == "" || r.PublishedHeadRevision != core.Hash("published") {
		return nil, errors.New("missing final content")
	}
	return &api.OperationReceipt{Operation: proto.Clone(r.Operation).(*api.OperationIdentity), State: "Released"}, nil
}

type stoppedVerifier struct{}

func (stoppedVerifier) Verify(_ context.Context, run, action, proof string) error {
	if run != "run" || action != "detached" || proof != "verified-stop" {
		return errors.New("not stopped")
	}
	return nil
}
func TestFinalizationPinsLostPublicationBeforeReturningBorrow(t *testing.T) {
	testFinalization(t, false, false)
}
func TestNodeDrainKeepsServingFinalPublicationAndRejectsNewAdmissions(t *testing.T) {
	testFinalization(t, true, false)
}
func TestStoppedUnknownSessionPublishesAndReleasesWithoutRestaging(t *testing.T) {
	testFinalization(t, false, true)
}
func testFinalization(t *testing.T, drain, recovery bool) {
	wal, err := OpenSessions(filepath.Join(t.TempDir(), "wal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	i := intent()
	i.HeadRevision = core.Hash("old-head")
	i.AuthorizationDigest = core.Hash("auth")
	i.RunID = "run"
	s, err := wal.Plan(i)
	if err != nil {
		t.Fatal(err)
	}
	s, err = wal.BeginCreate(s)
	if err != nil {
		t.Fatal(err)
	}
	s, err = wal.RecordDevice(s, DeviceEvidence{SessionID: i.ID, OperationID: i.OperationID, Epoch: i.Epoch, HeadRevision: i.HeadRevision, Readonly: i.Readonly, NodeBootID: "boot", DaemonInstanceID: "daemon", DeviceIdentity: "device", Path: "/dev/ublkb4", CapacityBytes: 4096, FSUUID: "fs"})
	if err != nil {
		t.Fatal(err)
	}
	s, err = wal.RecordPublish(s, "/private/target")
	if err != nil {
		t.Fatal(err)
	}
	s, err = wal.Borrow(s, "/private/target")
	if err != nil {
		t.Fatal(err)
	}
	if recovery {
		s, err = wal.MarkUnknown(s)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = wal.Borrow(s, "/private/target"); err == nil {
			t.Fatal("unknown device reborrowed")
		}
		if _, err = wal.pinCapture(i.ID, CaptureProof{CaptureID: core.Hash("live"), BarrierID: "live", FreezeDigest: core.Hash("freeze"), ExpectedHead: i.HeadRevision}); err == nil {
			t.Fatal("unknown live session captured")
		}
	}
	backend := &finalBackend{losePublish: true, expectedStopped: recovery}
	n := CSINode{Operations: &NodeOperations{Sessions: wal, Backend: backend}, Evidence: stoppedVerifier{}}
	if drain {
		n.BeginDrain()
		if pending, err := n.DrainPending(); err != nil || pending == 0 {
			t.Fatalf("live borrow drain: %d %v", pending, err)
		}
		if _, err := n.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{}); status.Code(err) != codes.Unavailable {
			t.Fatalf("stage during drain: %v", err)
		}
		if _, err := n.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{}); status.Code(err) != codes.Unavailable {
			t.Fatalf("publish during drain: %v", err)
		}
	}
	head := core.Hash("old-head")
	if err = n.FinalizeStopped(context.Background(), i.ID, "forged", head); err == nil || backend.captures != 0 {
		t.Fatal("unverified capture")
	}
	if err = n.FinalizeStopped(context.Background(), i.ID, "verified-stop", head); err == nil {
		t.Fatal("lost publication reported success")
	}
	s, _ = wal.Get(i.ID)
	if !s.Publishes["/private/target"].Borrowed {
		t.Fatal("borrow returned before durable publication")
	}
	if _, err = n.Operations.Prepare(context.Background(), i); err == nil {
		t.Fatal("finalizing session restarted")
	}
	if err = n.FinalizeStopped(context.Background(), i.ID, "verified-stop", core.Hash("changed")); err == nil {
		t.Fatal("changed final head accepted")
	}
	if err = n.FinalizeStopped(context.Background(), i.ID, "verified-stop", head); err != nil {
		t.Fatal(err)
	}
	s, _ = wal.Get(i.ID)
	if s.Publishes["/private/target"].Borrowed {
		t.Fatal("borrow not returned")
	}
	s, err = wal.RemovePublish(s, "/private/target")
	if err != nil {
		t.Fatal(err)
	}
	if err = n.finishStopped(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	s, _ = wal.Get(i.ID)
	if s.State != "Released" || backend.releases != 1 {
		t.Fatalf("incomplete release: %+v", s)
	}
	if err = n.FinalizeStopped(context.Background(), i.ID, "verified-stop", head); err != nil {
		t.Fatal(err)
	}
	if drain {
		if pending, err := n.DrainPending(); err != nil || pending != 0 {
			t.Fatalf("completed drain: %d %v", pending, err)
		}
	}
	if backend.captures != 1 || backend.publishes != 2 {
		t.Fatal("repeated finalization recaptured published data")
	}
}
