package overlaybd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/jackc/pgx/v5"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

// Handles encode repository identity, not a host path. Every supplied handle
// must also exist in the driver catalog; syntax alone never grants adoption.
func VolumeHandle(repository, nativeID string) (string, error) {
	if repository == "" || nativeID == "" || len(repository) > 256 || len(nativeID) > 256 || strings.ContainsAny(repository+nativeID, "\x00/\\") {
		return "", errors.New("invalid native backend identity")
	}
	encode := base64.RawURLEncoding.EncodeToString
	return "obd1." + encode([]byte(repository)) + "." + encode([]byte(nativeID)), nil
}
func ParseVolumeHandle(handle string) (repository, nativeID string, err error) {
	pieces := strings.Split(handle, ".")
	if len(pieces) != 3 || pieces[0] != "obd1" {
		return "", "", errors.New("invalid OverlayBD handle")
	}
	repo, e1 := base64.RawURLEncoding.DecodeString(pieces[1])
	id, e2 := base64.RawURLEncoding.DecodeString(pieces[2])
	if e1 != nil || e2 != nil {
		return "", "", errors.New("invalid OverlayBD handle encoding")
	}
	canonical, err := VolumeHandle(string(repo), string(id))
	if err != nil || canonical != handle {
		return "", "", errors.New("noncanonical OverlayBD handle")
	}
	return string(repo), string(id), nil
}

type CatalogVolume struct {
	SourceSnapshot  string   `json:"source_snapshot,omitempty"`
	SourceVolume    string   `json:"source_volume,omitempty"`
	Handle          string   `json:"handle"`
	RepositoryID    string   `json:"repository_id"`
	NativeID        string   `json:"native_id"`
	RequestName     string   `json:"request_name"`
	ProfileRevision string   `json:"profile_revision"`
	PVCUID          string   `json:"pvc_uid"`
	Readonly        bool     `json:"readonly"`
	CapacityBytes   uint64   `json:"capacity_bytes"`
	FSUUID          string   `json:"fs_uuid"`
	InitialHead     string   `json:"initial_head"`
	AccessibleNodes []string `json:"accessible_nodes"`
}

const catalogSchema = `
CREATE TABLE IF NOT EXISTS ssf_overlaybd.volumes (
 handle text PRIMARY KEY, repository_id text NOT NULL, native_id text NOT NULL,
 request_name text NOT NULL UNIQUE, identity jsonb NOT NULL,
 state text NOT NULL DEFAULT 'Ready' CHECK(state IN ('Ready','Deleting','Deleted')),
 UNIQUE(repository_id,native_id)
);
CREATE TABLE IF NOT EXISTS ssf_overlaybd.snapshot_pins (
 id text PRIMARY KEY, request_name text NOT NULL UNIQUE,
 source_handle text NOT NULL REFERENCES ssf_overlaybd.volumes(handle),
 head_revision text NOT NULL, identity jsonb NOT NULL,
 state text NOT NULL DEFAULT 'Ready' CHECK(state IN ('Ready','Deleting','Deleted'))
);
CREATE TABLE IF NOT EXISTS ssf_overlaybd.attachments (
 handle text NOT NULL REFERENCES ssf_overlaybd.volumes(handle),node_id text NOT NULL,
 grant_digest text NOT NULL,publish_context jsonb NOT NULL,
 state text NOT NULL CHECK(state IN ('Attached','Detached')),PRIMARY KEY(handle,node_id)
);
INSERT INTO ssf_overlaybd.schema_versions VALUES (2),(3) ON CONFLICT DO NOTHING;
`

func (j *Journal) RegisterVolume(ctx context.Context, name, pvcUID, profile string, readonly bool, receipt *api.VolumeReceipt, source ...*csi.VolumeContentSource) (CatalogVolume, error) {
	if name == "" || profile == "" || receipt == nil || !validOperation(receipt.Operation) || receipt.State != "ContentReady" || !validFilesystemUUID(receipt.FsUuid) || !validDigest(receipt.HeadRevision) || receipt.ContentDigest != receipt.HeadRevision || receipt.CapacityBytes == 0 {
		return CatalogVolume{}, errors.New("verified native volume receipt required")
	}
	handle, err := VolumeHandle(receipt.Operation.RepositoryId, receipt.Operation.VolumeId)
	if err != nil {
		return CatalogVolume{}, err
	}
	volume := CatalogVolume{Handle: handle, RepositoryID: receipt.Operation.RepositoryId, NativeID: receipt.Operation.VolumeId,
		RequestName: name, PVCUID: pvcUID, ProfileRevision: profile, Readonly: readonly,
		CapacityBytes: receipt.CapacityBytes, FSUUID: receipt.FsUuid, InitialHead: receipt.HeadRevision, AccessibleNodes: receipt.AccessibleNodes}
	if len(source) > 1 {
		return CatalogVolume{}, errors.New("at most one content source allowed")
	}
	if len(source) == 1 && source[0] != nil {
		if snapshot := source[0].GetSnapshot(); snapshot != nil {
			volume.SourceSnapshot = snapshot.SnapshotId
		}
		if original := source[0].GetVolume(); original != nil {
			volume.SourceVolume = original.VolumeId
		}
		if (volume.SourceSnapshot == "") == (volume.SourceVolume == "") {
			return CatalogVolume{}, errors.New("exactly one nonempty source required")
		}
	}
	raw, err := canonical(volume)
	if err != nil {
		return CatalogVolume{}, err
	}
	tag, err := j.Pool.Exec(ctx, `INSERT INTO ssf_overlaybd.volumes(handle,repository_id,native_id,request_name,identity)
        VALUES($1,$2,$3,$4,$5) ON CONFLICT(handle) DO UPDATE SET handle=excluded.handle
        WHERE ssf_overlaybd.volumes.state='Ready' AND (
          ssf_overlaybd.volumes.identity=excluded.identity OR (
            (ssf_overlaybd.volumes.identity-'capacity_bytes')=(excluded.identity-'capacity_bytes')
            AND (ssf_overlaybd.volumes.identity->>'capacity_bytes')::numeric > (excluded.identity->>'capacity_bytes')::numeric
            AND EXISTS(SELECT 1 FROM ssf_overlaybd.operations o WHERE o.kind='expand' AND o.state='Succeeded'
              AND o.input->'volume'->>'handle'=ssf_overlaybd.volumes.handle
              AND (o.result->>'capacity_bytes')::numeric=(ssf_overlaybd.volumes.identity->>'capacity_bytes')::numeric)
          ))`, handle, volume.RepositoryID, volume.NativeID, name, raw)
	if err != nil {
		return CatalogVolume{}, err
	}
	if tag.RowsAffected() != 1 {
		return CatalogVolume{}, ErrConflict
	}
	return j.Volume(ctx, handle)
}
func (j *Journal) Volume(ctx context.Context, handle string) (CatalogVolume, error) {
	repo, id, err := ParseVolumeHandle(handle)
	if err != nil {
		return CatalogVolume{}, err
	}
	var raw []byte
	err = j.Pool.QueryRow(ctx, `SELECT identity FROM ssf_overlaybd.volumes WHERE handle=$1 AND state='Ready'`, handle).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return CatalogVolume{}, ErrNotFound
	}
	if err != nil {
		return CatalogVolume{}, err
	}
	var volume CatalogVolume
	if err = json.Unmarshal(raw, &volume); err != nil {
		return CatalogVolume{}, err
	}
	if volume.Handle != handle || volume.RepositoryID != repo || volume.NativeID != id {
		return CatalogVolume{}, errors.New("corrupt driver volume identity")
	}
	return volume, nil
}
