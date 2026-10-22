package overlaybd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func catalogCSIVolume(v CatalogVolume) (*csi.Volume, error) {
	if v.CapacityBytes == 0 || v.CapacityBytes > math.MaxInt64 {
		return nil, status.Error(codes.DataLoss, "invalid catalog capacity")
	}
	result := &csi.Volume{VolumeId: v.Handle, CapacityBytes: int64(v.CapacityBytes), VolumeContext: map[string]string{"ssf.dev/volume-profile": v.ProfileRevision, "overlaybd.ssf.dev/repository": v.RepositoryID}}
	if v.SourceSnapshot != "" {
		result.ContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: v.SourceSnapshot}}}
	}
	if v.SourceVolume != "" {
		result.ContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: v.SourceVolume}}}
	}
	for _, node := range v.AccessibleNodes {
		result.AccessibleTopology = append(result.AccessibleTopology, &csi.Topology{Segments: map[string]string{TopologyNodeKey: node}})
	}
	return result, nil
}
func (j *Journal) publishedNodes(ctx context.Context, handle string) ([]string, error) {
	rows, err := j.Pool.Query(ctx, `SELECT node_id FROM ssf_overlaybd.attachments WHERE handle=$1 AND state='Attached' ORDER BY node_id`, handle)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []string
	for rows.Next() {
		var node string
		if err = rows.Scan(&node); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}
func (c *Controller) ControllerGetVolume(ctx context.Context, r *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	if r == nil || r.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID required")
	}
	v, err := c.Creator.Journal.Volume(ctx, r.VolumeId)
	if errors.Is(err, ErrNotFound) {
		return nil, status.Error(codes.NotFound, "registered native volume missing")
	}
	if err != nil {
		return nil, err
	}
	volume, err := catalogCSIVolume(v)
	if err != nil {
		return nil, err
	}
	nodes, err := c.Creator.Journal.publishedNodes(ctx, v.Handle)
	if err != nil {
		return nil, err
	}
	return &csi.ControllerGetVolumeResponse{Volume: volume, Status: &csi.ControllerGetVolumeResponse_VolumeStatus{PublishedNodeIds: nodes}}, nil
}

func decodeVolumeCursor(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != token {
		return "", status.Error(codes.Aborted, "invalid volume listing token")
	}
	handle := string(raw)
	if _, _, err = ParseVolumeHandle(handle); err != nil {
		return "", status.Error(codes.Aborted, "invalid volume listing cursor")
	}
	return handle, nil
}
func (c *Controller) ListVolumes(ctx context.Context, r *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	if r == nil || r.MaxEntries < 0 {
		return nil, status.Error(codes.InvalidArgument, "nonnegative max entries required")
	}
	after, err := decodeVolumeCursor(r.StartingToken)
	if err != nil {
		return nil, err
	}
	limit := r.MaxEntries
	if limit == 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := c.Creator.Journal.Pool.Query(ctx, `SELECT identity FROM ssf_overlaybd.volumes WHERE state='Ready' AND handle>$1 ORDER BY handle LIMIT $2`, after, limit+1)
	if err != nil {
		return nil, err
	}
	// Release the query connection before reading attachment status, including
	// configurations with a single database connection.
	var volumes []CatalogVolume
	for rows.Next() {
		var raw []byte
		var v CatalogVolume
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &v)
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		volumes = append(volumes, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	response := &csi.ListVolumesResponse{}
	if len(volumes) > int(limit) {
		volumes = volumes[:limit]
		response.NextToken = base64.RawURLEncoding.EncodeToString([]byte(volumes[len(volumes)-1].Handle))
	}
	for _, v := range volumes {
		volume, err := catalogCSIVolume(v)
		if err != nil {
			return nil, err
		}
		nodes, err := c.Creator.Journal.publishedNodes(ctx, v.Handle)
		if err != nil {
			return nil, err
		}
		response.Entries = append(response.Entries, &csi.ListVolumesResponse_Entry{Volume: volume, Status: &csi.ListVolumesResponse_VolumeStatus{PublishedNodeIds: nodes}})
	}
	return response, nil
}
