package overlaybd

import (
	"context"
	"encoding/json"
	"errors"

	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/ledger"
)

// IdleSnapshots captures an immutable published head under a public ledger
// fence. It is not a VMM barrier, and never substitutes for a live capture.
// Kubernetes identity resolution/Guard authorization must precede this call.
type IdleSnapshots struct {
	Ledger   *ledger.Store
	Journal  *Journal
	Backends map[string]HeadBackend
}

type IdleSnapshotRequest struct {
	Name            string
	Volume          core.Volume
	SnapshotUID     string
	ContentUID      string
	CreatedUnixNano int64
}

// Capture fixes the source and Kubernetes identity before touching the backend.
// A retained public source reference spans crashes between capture and Driver
// registration. Ready replay does not read a newer head or contact the backend.
func (s *IdleSnapshots) Capture(ctx context.Context, q IdleSnapshotRequest) (SnapshotPin, error) {
	var result SnapshotPin
	if s.Ledger == nil || s.Journal == nil || q.Name == "" || q.Volume.Driver != DriverName || q.SnapshotUID == "" || q.ContentUID == "" || q.CreatedUnixNano <= 0 {
		return result, errors.New("verified idle snapshot dependencies and identity required")
	}
	volume, err := s.Journal.Volume(ctx, q.Volume.Handle)
	if err != nil {
		return result, err
	}
	id := Identity("snapshot", q.Name)
	record, err := s.Ledger.BeginPublishedCapture(ctx, id, q.Volume, q)
	if err != nil {
		return result, err
	}
	if record.State == "Ready" {
		if err = json.Unmarshal(record.Receipt, &result); err != nil {
			return result, err
		}
		if result.ID != id || result.RequestName != q.Name || result.SourceHandle != q.Volume.Handle || result.SnapshotUID != q.SnapshotUID || result.ContentUID != q.ContentUID || result.CreatedUnixNano != q.CreatedUnixNano {
			return result, core.ErrConflict
		}
		return result, result.validate()
	}
	backend := s.Backends[volume.RepositoryID]
	if backend == nil {
		return result, errors.New("native head backend unavailable")
	}
	head, err := backend.ResolveVolume(ctx, &api.ResolveVolumeRequest{RepositoryId: volume.RepositoryID, VolumeId: volume.NativeID})
	if err != nil {
		return result, err
	}
	if head == nil || !validOperation(head.Operation) || head.Operation.RepositoryId != volume.RepositoryID || head.Operation.VolumeId != volume.NativeID || head.State != "ContentReady" || head.HeadRevision != head.ContentDigest || !validDigest(head.HeadRevision) || head.FsUuid != volume.FSUUID || head.CapacityBytes != volume.CapacityBytes || !sameNodes(head.AccessibleNodes, volume.AccessibleNodes) {
		return result, errors.New("published idle head identity mismatch")
	}
	result = SnapshotPin{ID: id, RequestName: q.Name, SourceHandle: q.Volume.Handle, HeadRevision: head.HeadRevision, CapacityBytes: head.CapacityBytes, FSUUID: head.FsUuid, SnapshotUID: q.SnapshotUID, ContentUID: q.ContentUID, CreatedUnixNano: q.CreatedUnixNano,
		CaptureDigest: core.Digest(struct {
			Kind     string
			Source   string
			Head     string
			UUID     string
			Capacity uint64
		}{"idle-published-head/v1", q.Volume.Key(), head.HeadRevision, head.FsUuid, head.CapacityBytes})}
	if err = result.validate(); err != nil {
		return result, err
	}
	if err = s.Ledger.CommitIdleCapture(ctx, id, record.Digest, result); err != nil {
		return result, err
	}
	return result, nil
}
