package overlaybd

import (
	"context"
	"encoding/json"
	"errors"

	control "ssf.dev/control"
	"ssf.dev/ssf/core"
)

var ErrIdleCaptureEligible = errors.New("no live capture selected")

// LiveSnapshotSource asks the runtime through the common coordinator. It never
// calls the VMM or mints a runtime Ready/freeze receipt from the CSI controller.
type LiveSnapshotSource interface {
	Capture(context.Context, core.DirectSnapshotRef) (core.SnapshotCapture, error)
}
type RemoteLiveSnapshots struct {
	Client  *control.Client
	Runtime string
}

func (s *RemoteLiveSnapshots) Capture(ctx context.Context, q core.DirectSnapshotRef) (core.SnapshotCapture, error) {
	var out core.SnapshotCapture
	if s.Client == nil || s.Runtime == "" {
		return out, core.ErrPending
	}
	body, _ := json.Marshal(map[string]string{"id": q.ID})
	response, err := s.Client.Call(ctx, "CaptureVolume", control.Request{Runtime: s.Runtime, Envelope: body})
	if err != nil {
		var remote *control.Error
		if errors.As(err, &remote) && remote.Code == "pending" {
			return out, core.ErrPending
		}
		return out, err
	}
	if response.State == "Idle" {
		return out, ErrIdleCaptureEligible
	}
	if err = json.Unmarshal(response.Envelope, &out); err != nil {
		return out, err
	}
	if out.PVC != q.Volume.PVC || out.Driver != q.Volume.Driver || out.Handle != q.Volume.Handle || out.CheckpointID == "" || out.BindingID == "" || out.CaptureID == "" || !validDigest(out.CaptureDigest) || !validDigest(out.HeadRevision) || !validFilesystemUUID(out.FilesystemUUID) || out.CapacityBytes == 0 {
		return out, core.ErrConflict
	}
	return out, nil
}
