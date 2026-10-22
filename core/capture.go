package core

import (
	"encoding/hex"
	"errors"
)

const CaptureVersion = "ssf.capture.v1"

// CaptureIntent identifies one immutable native memory/rootfs capture and every
// CSI volume in that same barrier. The runtime must keep its VMM paused until
// Advance succeeds or it abandons the checkpoint. Proofs are never persisted.
type CaptureIntent struct {
	Version        string            `json:"version"`
	ID             string            `json:"id"`
	Operation      string            `json:"operation"`
	OwnerUID       string            `json:"owner_uid"`
	RunUID         string            `json:"run_uid"`
	BarrierID      string            `json:"barrier_id"`
	NativeArtifact string            `json:"native_artifact"`
	NativeDigest   string            `json:"native_digest"`
	Classes        map[string]string `json:"classes"` // binding ID -> native VolumeSnapshotClass
}

func (q CaptureIntent) Validate() error {
	digest, err := hex.DecodeString(q.NativeDigest)
	if q.Version != CaptureVersion || q.ID == "" || q.Operation == "" || q.OwnerUID == "" || q.RunUID == "" || q.BarrierID == "" || q.NativeArtifact == "" || err != nil || len(digest) != 32 || len(q.Classes) == 0 || len(q.Classes) > 24 {
		return errors.New("invalid joint capture identity or native artifact digest")
	}
	for binding, class := range q.Classes {
		if binding == "" || class == "" {
			return errors.New("snapshot class required for every binding")
		}
	}
	return nil
}
