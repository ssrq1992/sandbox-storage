package core

// DirectSnapshotRef fixes native Kubernetes identities independently of a VM
// checkpoint. It authorizes no device access and contains no credentials.
type DirectSnapshotRef struct {
	ID              string `json:"id"`
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
	SnapshotUID     string `json:"snapshot_uid"`
	ContentName     string `json:"content_name"`
	ContentUID      string `json:"content_uid"`
	Volume          Volume `json:"volume"`
	CreatedUnixNano int64  `json:"created_unix_nano"`
}
