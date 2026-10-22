package overlaybd

import (
	"context"
	"google.golang.org/grpc"
	"path/filepath"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	"testing"
)

type readonlyCaptureBackend struct {
	finalBackend
	wrong    bool
	resolves int
}

func (b *readonlyCaptureBackend) ResolveVolume(_ context.Context, r *api.ResolveVolumeRequest, _ ...grpc.CallOption) (*api.VolumeReceipt, error) {
	b.resolves++
	head := r.HeadRevision
	if b.wrong {
		head = core.Hash("latest-but-wrong")
	}
	return &api.VolumeReceipt{Operation: &api.OperationIdentity{RepositoryId: r.RepositoryId, VolumeId: r.VolumeId}, State: "ContentReady", HeadRevision: head, ContentDigest: head, CapacityBytes: 4096, CapacityGeneration: 1, FsUuid: "fs"}, nil
}
func TestReadonlyCapturePinsHeadWithoutPublication(t *testing.T) {
	wal, err := OpenSessions(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	i := intent()
	i.Readonly = true
	i.HeadRevision = core.Hash("immutable")
	i.AuthorizationDigest = core.Hash("grant")
	s, err := wal.Plan(i)
	s = mustSession(t, s, err)
	s, err = wal.BeginCreate(s)
	s = mustSession(t, s, err)
	e := evidence()
	e.Readonly = true
	e.HeadRevision = i.HeadRevision
	e.CapacityBytes = 4096
	e.FSUUID = "fs"
	s, err = wal.RecordDevice(s, e)
	mustSession(t, s, err)
	backend := &readonlyCaptureBackend{wrong: true}
	n := NodeOperations{Sessions: wal, Backend: backend}
	proof := CaptureProof{CaptureID: "capture", BarrierID: "barrier", FreezeDigest: core.Hash("freeze"), ExpectedHead: i.HeadRevision}
	if _, err = n.Capture(context.Background(), i.ID, proof); err != nil {
		t.Fatal(err)
	}
	if _, err = n.PublishCaptured(context.Background(), i.ID, proof.CaptureID); err == nil {
		t.Fatal("readonly source head advanced")
	}
	backend.wrong = false
	if _, err = n.PublishCaptured(context.Background(), i.ID, proof.CaptureID); err != nil {
		t.Fatal(err)
	}
	if backend.publishes != 0 || backend.resolves != 2 {
		t.Fatal("readonly capture mutated native head")
	}
}
