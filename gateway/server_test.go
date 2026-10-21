package gateway

import (
	"context"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestManagementRPCsDenied(t *testing.T) {
	s := &Server{}
	ctx := context.Background()
	_, a := s.CreateVolume(ctx, &csi.CreateVolumeRequest{})
	_, b := s.DeleteVolume(ctx, &csi.DeleteVolumeRequest{})
	_, c := s.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{})
	_, d := s.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{})
	for _, e := range []error{a, b, c, d} {
		if status.Code(e) != codes.Unimplemented {
			t.Fatalf("management RPC enabled: %v", e)
		}
	}
}
