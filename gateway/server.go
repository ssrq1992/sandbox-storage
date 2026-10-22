// Package gateway exposes a strict standard-CSI consumption subset over mTLS.
package gateway

import (
	"context"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Server struct {
	csi.UnimplementedControllerServer
	csi.UnimplementedIdentityServer
	Controller csi.ControllerClient
	Identity   csi.IdentityClient
	Authorizer interface {
		Publish(context.Context, *csi.ControllerPublishVolumeRequest) error
		Unpublish(context.Context, *csi.ControllerUnpublishVolumeRequest) error
	}
}

func (s *Server) GetPluginInfo(c context.Context, r *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return s.Identity.GetPluginInfo(c, r)
}
func (s *Server) GetPluginCapabilities(c context.Context, r *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return s.Identity.GetPluginCapabilities(c, r)
}
func (s *Server) Probe(c context.Context, r *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return s.Identity.Probe(c, r)
}
func (s *Server) ControllerGetCapabilities(c context.Context, r *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	resp, e := s.Controller.ControllerGetCapabilities(c, r)
	if e != nil {
		return nil, e
	}
	out := &csi.ControllerGetCapabilitiesResponse{}
	for _, cap := range resp.Capabilities {
		if cap.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME {
			out.Capabilities = append(out.Capabilities, cap)
		}
	}
	return out, nil
}
func (s *Server) ValidateVolumeCapabilities(c context.Context, r *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return s.Controller.ValidateVolumeCapabilities(c, r)
}
func (s *Server) ControllerPublishVolume(c context.Context, r *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if s.Authorizer == nil {
		return nil, status.Error(codes.PermissionDenied, "consumption authorizer required")
	}
	if e := s.Authorizer.Publish(c, r); e != nil {
		return nil, status.Error(codes.PermissionDenied, e.Error())
	}
	return s.Controller.ControllerPublishVolume(forwardBinding(c), r)
}
func (s *Server) ControllerUnpublishVolume(c context.Context, r *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if s.Authorizer == nil {
		return nil, status.Error(codes.PermissionDenied, "consumption authorizer required")
	}
	if e := s.Authorizer.Unpublish(c, r); e != nil {
		return nil, status.Error(codes.PermissionDenied, e.Error())
	}
	return s.Controller.ControllerUnpublishVolume(forwardBinding(c), r)
}

// Forward only the binding identity after Authorizer accepted it. Private
// driver extensions may independently validate the same ledger intent; inbound
// credentials and unrelated metadata are never copied to the CSI socket.
func forwardBinding(ctx context.Context) context.Context {
	incoming, _ := metadata.FromIncomingContext(ctx)
	if ids := incoming.Get("ssf-binding-id"); len(ids) == 1 {
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		outgoing = outgoing.Copy()
		outgoing.Set("ssf-binding-id", ids[0])
		return metadata.NewOutgoingContext(ctx, outgoing)
	}
	return ctx
}
