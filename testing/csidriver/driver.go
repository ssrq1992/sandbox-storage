// Package csidriver is a Linux integration fixture, NOT a production backend.
// File mode uses bind mounts. Block mode uses loop devices backed by sparse files.
package csidriver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"os/exec"
	"path/filepath"
	"ssf.dev/ssf/core"
	"strings"
	"sync"
	"time"
)

type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
	mu                 sync.Mutex
	Root, NodeID, Mode string
	Attach             bool
}
type metadata struct {
	ID   string
	Size int64
	Node string
	Mode string
}

func (d *Driver) Name() string { return "test." + d.Mode + ".ssf.dev" }
func (d *Driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: d.Name(), VendorVersion: "0.1.0"}, nil
}
func (d *Driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_CONTROLLER_SERVICE}}}}}, nil
}
func (d *Driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}
func (d *Driver) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	types := []csi.ControllerServiceCapability_RPC_Type{csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME, csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT, csi.ControllerServiceCapability_RPC_CLONE_VOLUME, csi.ControllerServiceCapability_RPC_EXPAND_VOLUME}
	if d.Attach {
		types = append(types, csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME)
	}
	r := &csi.ControllerGetCapabilitiesResponse{}
	for _, t := range types {
		r.Capabilities = append(r.Capabilities, &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: t}}})
	}
	return r, nil
}
func (d *Driver) path(id string) (string, error) {
	if len(id) != 64 || strings.Trim(id, "0123456789abcdef") != "" {
		return "", status.Error(codes.InvalidArgument, "invalid fixture handle")
	}
	return filepath.Join(d.Root, id), nil
}
func (d *Driver) load(id string) (metadata, string, error) {
	p, e := d.path(id)
	if e != nil {
		return metadata{}, "", e
	}
	b, e := os.ReadFile(p + ".json")
	if os.IsNotExist(e) {
		return metadata{}, p, status.Error(codes.NotFound, "volume missing")
	}
	if e != nil {
		return metadata{}, p, e
	}
	var m metadata
	e = json.Unmarshal(b, &m)
	return m, p, e
}
func save(p string, m metadata) error {
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	if e = os.WriteFile(p+".tmp", b, 0600); e != nil {
		return e
	}
	return os.Rename(p+".tmp", p+".json")
}
func run(ctx context.Context, name string, args ...string) (string, error) {
	b, e := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if e != nil {
		return "", fmt.Errorf("%s: %w: %s", name, e, b)
	}
	return strings.TrimSpace(string(b)), nil
}
func (d *Driver) CreateVolume(ctx context.Context, q *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if q.Name == "" || len(q.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "name/capability required")
	}
	id := core.Hash(q.Name)
	p, _ := d.path(id)
	size := int64(64 << 20)
	if q.CapacityRange != nil && q.CapacityRange.RequiredBytes > size {
		size = q.CapacityRange.RequiredBytes
	}
	if e := os.MkdirAll(d.Root, 0700); e != nil {
		return nil, e
	}
	m, _, e := d.load(id)
	if e == nil {
		if m.Size < size {
			return nil, status.Error(codes.AlreadyExists, "capacity differs")
		}
		size = m.Size
	} else {
		if status.Code(e) != codes.NotFound {
			return nil, e
		}
		if d.Mode == "block" {
			f, e := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0600)
			if e != nil {
				return nil, e
			}
			e = f.Truncate(size)
			f.Close()
			if e != nil {
				return nil, e
			}
		} else {
			if e = os.MkdirAll(p, 0700); e != nil {
				return nil, e
			}
		}
		if src := q.VolumeContentSource; src != nil {
			source := ""
			if v := src.GetVolume(); v != nil {
				source = v.VolumeId
			}
			if s := src.GetSnapshot(); s != nil {
				source = s.SnapshotId
			}
			sp, e := d.path(source)
			if e != nil {
				return nil, e
			}
			if _, e = run(ctx, "cp", "-a", sp+"/.", p); e != nil && d.Mode == "file" {
				return nil, e
			}
			if d.Mode == "block" {
				if _, e = run(ctx, "cp", sp, p); e != nil {
					return nil, e
				}
			}
		}
		if e = save(p, metadata{ID: id, Size: size, Mode: d.Mode}); e != nil {
			return nil, e
		}
	}
	v := &csi.Volume{VolumeId: id, CapacityBytes: size, ContentSource: q.VolumeContentSource}
	if q.AccessibilityRequirements != nil && len(q.AccessibilityRequirements.Preferred) > 0 {
		v.AccessibleTopology = q.AccessibilityRequirements.Preferred[:1]
	}
	return &csi.CreateVolumeResponse{Volume: v}, nil
}
func (d *Driver) DeleteVolume(_ context.Context, q *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	m, p, e := d.load(q.VolumeId)
	if status.Code(e) == codes.NotFound {
		return &csi.DeleteVolumeResponse{}, nil
	}
	if e != nil {
		return nil, e
	}
	if m.Node != "" {
		return nil, status.Error(codes.FailedPrecondition, "still attached")
	}
	if e = os.RemoveAll(p); e != nil {
		return nil, e
	}
	if e = os.Remove(p + ".json"); e != nil && !os.IsNotExist(e) {
		return nil, e
	}
	return &csi.DeleteVolumeResponse{}, nil
}
func (d *Driver) ControllerPublishVolume(_ context.Context, q *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	m, p, e := d.load(q.VolumeId)
	if e != nil {
		return nil, e
	}
	if m.Node != "" && m.Node != q.NodeId {
		return nil, status.Error(codes.FailedPrecondition, "attached elsewhere")
	}
	m.Node = q.NodeId
	if e = save(p, m); e != nil {
		return nil, e
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{"fixture": "attached", "node": q.NodeId}}, nil
}
func (d *Driver) ControllerUnpublishVolume(_ context.Context, q *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	m, p, e := d.load(q.VolumeId)
	if status.Code(e) == codes.NotFound {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if e != nil {
		return nil, e
	}
	if m.Node == q.NodeId {
		m.Node = ""
		e = save(p, m)
	}
	return &csi.ControllerUnpublishVolumeResponse{}, e
}
func (d *Driver) ValidateVolumeCapabilities(_ context.Context, q *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	for _, c := range q.VolumeCapabilities {
		if (d.Mode == "block") != (c.GetBlock() != nil) {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: "fixture mode mismatch"}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: q.VolumeCapabilities}}, nil
}
func (d *Driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: d.NodeID, AccessibleTopology: &csi.Topology{Segments: map[string]string{"ssf.dev/node": d.NodeID}}}, nil
}
func (d *Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	r := &csi.NodeGetCapabilitiesResponse{}
	for _, t := range []csi.NodeServiceCapability_RPC_Type{csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME, csi.NodeServiceCapability_RPC_EXPAND_VOLUME} {
		r.Capabilities = append(r.Capabilities, &csi.NodeServiceCapability{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: t}}})
	}
	return r, nil
}
func mounted(ctx context.Context, p string) bool {
	_, e := run(ctx, "mountpoint", "-q", p)
	return e == nil
}
func (d *Driver) NodeStageVolume(ctx context.Context, q *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, p, e := d.load(q.VolumeId)
	if e != nil {
		return nil, e
	}
	if d.Attach && q.PublishContext["node"] != d.NodeID {
		return nil, status.Error(codes.FailedPrecondition, "attachment context required")
	}
	if e = os.MkdirAll(q.StagingTargetPath, 0700); e != nil {
		return nil, e
	}
	if d.Mode == "file" {
		if !mounted(ctx, q.StagingTargetPath) {
			_, e = run(ctx, "mount", "--bind", p, q.StagingTargetPath)
		}
	} else {
		device := ""
		out, e := run(ctx, "losetup", "-j", p)
		if e != nil {
			return nil, e
		}
		if out != "" {
			device = strings.SplitN(out, ":", 2)[0]
		} else {
			device, e = run(ctx, "losetup", "--find", "--show", p)
			if e != nil {
				return nil, e
			}
		}
		if e = os.WriteFile(filepath.Join(q.StagingTargetPath, "device"), []byte(device), 0600); e != nil {
			return nil, e
		}
	}
	return &csi.NodeStageVolumeResponse{}, e
}
func (d *Driver) NodePublishVolume(ctx context.Context, q *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	source := q.StagingTargetPath
	if d.Mode == "block" {
		b, e := os.ReadFile(filepath.Join(source, "device"))
		if e != nil {
			return nil, e
		}
		source = string(b)
	}
	if !mounted(ctx, q.TargetPath) {
		if _, e := run(ctx, "mount", "--bind", source, q.TargetPath); e != nil {
			return nil, e
		}
	}
	if q.Readonly {
		if _, e := run(ctx, "mount", "-o", "remount,bind,ro", q.TargetPath); e != nil {
			return nil, e
		}
	}
	return &csi.NodePublishVolumeResponse{}, nil
}
func (d *Driver) NodeUnpublishVolume(ctx context.Context, q *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if mounted(ctx, q.TargetPath) {
		if _, e := run(ctx, "umount", q.TargetPath); e != nil {
			return nil, e
		}
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
func (d *Driver) NodeUnstageVolume(ctx context.Context, q *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Mode == "file" {
		if mounted(ctx, q.StagingTargetPath) {
			if _, e := run(ctx, "umount", q.StagingTargetPath); e != nil {
				return nil, e
			}
		}
	} else {
		b, e := os.ReadFile(filepath.Join(q.StagingTargetPath, "device"))
		if e == nil {
			if _, e = run(ctx, "losetup", "-d", string(b)); e != nil {
				return nil, e
			}
			os.Remove(filepath.Join(q.StagingTargetPath, "device"))
		} else if !os.IsNotExist(e) {
			return nil, e
		}
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}
func (d *Driver) ControllerExpandVolume(_ context.Context, q *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	m, p, e := d.load(q.VolumeId)
	if e != nil {
		return nil, e
	}
	size := q.GetCapacityRange().GetRequiredBytes()
	if size < m.Size {
		size = m.Size
	}
	if d.Mode == "block" {
		if e = os.Truncate(p, size); e != nil {
			return nil, e
		}
	}
	m.Size = size
	if e = save(p, m); e != nil {
		return nil, e
	}
	return &csi.ControllerExpandVolumeResponse{CapacityBytes: size, NodeExpansionRequired: d.Mode == "block"}, nil
}
func (d *Driver) NodeExpandVolume(ctx context.Context, q *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if d.Mode == "block" {
		b, e := os.ReadFile(filepath.Join(q.StagingTargetPath, "device"))
		if e != nil {
			return nil, e
		}
		if _, e = run(ctx, "losetup", "-c", string(b)); e != nil {
			return nil, e
		}
	}
	return &csi.NodeExpandVolumeResponse{CapacityBytes: q.GetCapacityRange().GetRequiredBytes()}, nil
}
func (d *Driver) CreateSnapshot(ctx context.Context, q *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	m, p, e := d.load(q.SourceVolumeId)
	if e != nil {
		return nil, e
	}
	id := core.Hash("snapshot/" + q.Name)
	dest, _ := d.path(id)
	if _, e = os.Stat(dest); os.IsNotExist(e) {
		if _, e = run(ctx, "cp", "-a", p, dest); e != nil {
			return nil, e
		}
	}
	if e = save(dest, metadata{ID: id, Size: m.Size, Mode: m.Mode}); e != nil {
		return nil, e
	}
	return &csi.CreateSnapshotResponse{Snapshot: &csi.Snapshot{SnapshotId: id, SourceVolumeId: q.SourceVolumeId, SizeBytes: m.Size, CreationTime: timestamppb.New(time.Now()), ReadyToUse: true}}, nil
}
func (d *Driver) DeleteSnapshot(ctx context.Context, q *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	_, e := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: q.SnapshotId})
	return &csi.DeleteSnapshotResponse{}, e
}
