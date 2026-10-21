package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
)

// checkpointManifest accepts only manifests produced by an authenticated run
// of this tenant. A checkpoint ID supplied by a caller is never authorization.
// Retained references deliberately keep the original capture descriptor.
func (c *NativeControl) checkpointManifest(ctx context.Context, id string) (consume.CaptureManifest, error) {
	var m consume.CaptureManifest
	cp, err := c.Ledger.Checkpoint(ctx, id)
	if err != nil {
		return m, err
	}
	if cp.State != "Ready" {
		return m, core.ErrConflict
	}
	if err = json.Unmarshal(cp.Descriptor, &m); err != nil {
		return m, err
	}
	if err = m.Intent.Validate(); err != nil {
		return m, err
	}
	var raw []byte
	var owner string
	if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT intent,owner_uid FROM agentenv_native_runs WHERE run_uid=$1`, m.Intent.RunUID).Scan(&raw, &owner); err != nil {
		return m, err
	}
	var intent NativeIntent
	if err = json.Unmarshal(raw, &intent); err != nil {
		return m, err
	}
	if c.Principal.Tenant == "" || intent.Tenant != c.Principal.Tenant || owner != m.Intent.OwnerUID || len(m.Members) != len(m.Intent.Classes) || m.NativeResourceUID == "" {
		return m, core.ErrConflict
	}
	seen := map[string]bool{}
	for _, member := range m.Members {
		if seen[member.BindingID] || m.Intent.Classes[member.BindingID] == "" || member.PVC.UID == "" || !c.Principal.Allows(member.PVC.Namespace) || member.SnapshotUID == "" || member.ContentUID == "" {
			return m, core.ErrConflict
		}
		seen[member.BindingID] = true
	}
	return m, nil
}

// RetainNativeCheckpoint protects publication/restore independently of the
// parent's lifetime. The immutable consumer ID is assigned by native runtime,
// never a mutable template alias. Replay must use the same source and consumer.
func (c *NativeControl) retainNativeCheckpoint(ctx context.Context, source, kind, consumer string) (string, error) {
	if !safeID.MatchString(consumer) || (kind != "template" && kind != "restore" && kind != "fork") {
		return "", errors.New("invalid native checkpoint consumer")
	}
	ref := "agentenv/ref/" + core.Hash(c.Principal.Tenant+"/"+kind+"/"+consumer)
	if source == ref {
		if _, err := c.checkpointManifest(ctx, ref); err != nil {
			return "", err
		}
		cp, err := c.Ledger.Checkpoint(ctx, ref)
		if err != nil {
			return "", err
		}
		if cp.OwnerUID != c.Principal.Tenant+"/"+kind+"/"+consumer {
			return "", core.ErrConflict
		}
		return ref, nil
	}
	// A ready reference is authoritative on retry even after its parent was
	// released. RetainCheckpoint checks its immutable source digest atomically.
	_, err := c.checkpointManifest(ctx, ref)
	if err != nil {
		if _, err = c.checkpointManifest(ctx, source); err != nil {
			return "", err
		}
	}
	err = c.Ledger.RetainCheckpoint(ctx, source, ref, c.Principal.Tenant+"/"+kind+"/"+consumer)
	return ref, err
}

type NativeCapturedVolume struct {
	MountName string       `json:"mount_name,omitempty"`
	Volume    NativeVolume `json:"volume"`
	Index     int          `json:"index"`
}

type NativeCloneMember struct {
	Index   int          `json:"index"`
	Mount   NativeMount  `json:"mount"`
	MountID string       `json:"mount_id"`
	Volume  NativeVolume `json:"volume"`
}

// PublishNativeCheckpoint returns an opaque provider descriptor for the native
// manifest. A repository write with an unknown outcome keeps this reference;
// only confirmed deletion of the immutable native ID may release it.
func (c *NativeControl) PublishNativeCheckpoint(ctx context.Context, checkpoint json.RawMessage, nativeID string) (map[string]any, error) {
	var supplied struct {
		Reference string                `json:"reference"`
		Intent    consume.CaptureIntent `json:"intent"`
	}
	if err := json.Unmarshal(checkpoint, &supplied); err != nil {
		return nil, err
	}
	source := supplied.Reference
	if source == "" {
		source = supplied.Intent.ID
	}
	ref, err := c.RetainNativeCheckpoint(ctx, source, "template", nativeID)
	if err != nil {
		return nil, err
	}
	m, err := c.checkpointManifest(ctx, ref)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	out["reference"] = ref
	return out, nil
}

// CloneNativeCheckpoint provisions writable children through native PVC
// dataSource. Fork shares read-only sources while template restores create
// independent read-only PVCs from snapshots, so source deletion is harmless. The
// caller must retain a reference before calling and retain it through native
// artifact consumption; PVC restore pins alone do not protect native memory.
func (c *NativeControl) CloneNativeCheckpoint(ctx context.Context, reference, child string) ([]NativeCloneMember, error) {
	return c.cloneNativeCheckpoint(ctx, reference, child, false)
}

func (c *NativeControl) cloneNativeCheckpoint(ctx context.Context, reference, child string, privatize bool) ([]NativeCloneMember, error) {
	if !safeID.MatchString(child) {
		return nil, errors.New("invalid native child identity")
	}
	cp, err := c.Ledger.Checkpoint(ctx, reference)
	if err != nil {
		return nil, err
	}
	if cp.OwnerUID != c.Principal.Tenant+"/fork/"+child && cp.OwnerUID != c.Principal.Tenant+"/restore/"+child {
		return nil, core.ErrConflict
	}
	m, err := c.checkpointManifest(ctx, reference)
	if err != nil {
		return nil, err
	}
	out := make([]NativeCloneMember, 0, len(m.Members))
	for _, member := range m.Members {
		// Capture metadata is fixed before snapshot publication; do not consult a
		// subsequently edited/deleted source volume descriptor for clone settings.
		var captured NativeCapturedVolume
		if err = c.Ledger.ReadStep(ctx, "agentenv/capture-volume/"+m.Intent.ID+"/"+member.BindingID, &captured); err != nil {
			return nil, err
		}
		source := captured.Volume
		if captured.Index < 0 || captured.Index >= len(m.Members) {
			return nil, core.ErrConflict
		}
		if source.PVC != member.PVC || source.ReadOnly != member.ReadOnly || source.Tenant != c.Principal.Tenant {
			return nil, core.ErrConflict
		}
		volume := source
		if source.ReadOnly && cp.OwnerUID == c.Principal.Tenant+"/fork/"+child {
			volume, err = c.Volumes.Get(ctx, c.Principal.Tenant, source.Name)
			if err != nil {
				return nil, err
			}
			if volume.PVC != source.PVC || !volume.ReadOnly || volume.State == "Deleted" || volume.State == "Deleting" {
				return nil, core.ErrConflict
			}
		} else {
			volume, err = c.Volumes.Create(ctx, c.Principal, NativeVolumeRequest{internalPVCSource: source.PVCSource, internalPrivate: source.PVCSource || privatize || strings.HasPrefix(member.GuestPath, NativeRetainedPrefix), internalOwner: child, Provider: "ssf", Name: "clone-" + core.Hash(reference + "/" + child + "/" + member.BindingID)[:40], Namespace: source.PVC.Namespace, StorageClass: source.StorageClass, SnapshotClass: source.SnapshotClass, Bytes: source.Bytes, ReadOnly: source.ReadOnly, Snapshot: member.Snapshot, SnapshotUID: member.SnapshotUID, ContentUID: member.ContentUID})
			if err != nil {
				return nil, err
			}
		}
		mountName := captured.MountName
		if mountName == "" {
			mountName = source.Name
		}
		out = append(out, NativeCloneMember{Index: captured.Index, Mount: NativeMount{Name: mountName, Path: member.GuestPath}, MountID: member.MountID, Volume: volume})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	for i, m := range out {
		if m.Index != i {
			return nil, core.ErrConflict
		}
	}
	return out, nil
}
