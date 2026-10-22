package overlaybd

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

type NodeIdentity struct {
	csi.UnimplementedIdentityServer
	Node *CSINode
}

func (i *NodeIdentity) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	manifest := map[string]string{"native-backend-protocol": BackendProtocol, "ssf.dev/managed-runtime": "v1", "ssf.dev/stage-scope": "binding-v1"}
	if i.Node != nil && i.Node.CaptureEvidence != nil {
		manifest["ssf.dev/managed-capture"] = "v1"
	}
	return &csi.GetPluginInfoResponse{Name: DriverName, VendorVersion: "0.1.0", Manifest: manifest}, nil
}
func (i *NodeIdentity) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS}}}}}, nil
}
func (i *NodeIdentity) Probe(ctx context.Context, _ *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	if i.Node == nil || i.Node.Operations == nil || i.Node.Operations.Backend == nil {
		return nil, status.Error(codes.FailedPrecondition, "native backend unavailable")
	}
	if i.Node.draining.Load() {
		return &csi.ProbeResponse{Ready: wrapperspb.Bool(false)}, nil
	}
	identity, err := i.Node.Operations.Backend.Describe(ctx, &api.DescribeRequest{})
	if err != nil {
		return nil, err
	}
	if err = ValidateBackend(identity); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
