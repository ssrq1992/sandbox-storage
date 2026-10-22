package core

// SnapshotCapture is the durable handoff from a verified runtime capture to
// Kubernetes snapshot provisioning. It contains no host paths or credentials.
type SnapshotCapture struct {
	CheckpointID   string `json:"checkpoint_id"`
	BindingID      string `json:"binding_id"`
	Driver         string `json:"driver"`
	Handle         string `json:"handle"`
	PVC            PVCRef `json:"pvc"`
	SnapshotName   string `json:"snapshot_name"`
	CaptureID      string `json:"capture_id"`
	CaptureDigest  string `json:"capture_digest"`
	HeadRevision   string `json:"head_revision"`
	CapacityBytes  uint64 `json:"capacity_bytes"`
	FilesystemUUID string `json:"filesystem_uuid"`
}
