package overlaybd

import (
	"context"
	"crypto/tls"
	"errors"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

// BackendWorker exposes only repository operations to the authenticated CSI
// Controller. It never forwards Node/device or VMM operations across nodes.
type BackendWorker struct {
	api.UnimplementedNativeStorageBackendServer
	RepositoryID string
	Backend      api.NativeStorageBackendClient
}

func (w *BackendWorker) authorize(op *api.OperationIdentity) error {
	if w.Backend == nil || w.RepositoryID == "" {
		return status.Error(codes.FailedPrecondition, "worker backend unavailable")
	}
	if !validOperation(op) || op.RepositoryId != w.RepositoryID {
		return status.Error(codes.PermissionDenied, "operation outside worker repository")
	}
	return nil
}
func (w *BackendWorker) Describe(ctx context.Context, _ *api.DescribeRequest) (*api.BackendIdentity, error) {
	if w.Backend == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker backend unavailable")
	}
	identity, err := w.Backend.Describe(ctx, &api.DescribeRequest{})
	if err != nil {
		return nil, err
	}
	if err = ValidateBackend(identity, CapabilityNativeVolume); err != nil {
		return nil, err
	}
	if !slices.Contains(identity.RepositoryIds, w.RepositoryID) {
		return nil, status.Error(codes.FailedPrecondition, "worker repository not served by native host")
	}
	capabilities := []string{CapabilityNativeVolume}
	if slices.Contains(identity.Capabilities, CapabilityStaticRegistration) {
		capabilities = append(capabilities, CapabilityStaticRegistration)
	}
	if slices.Contains(identity.Capabilities, CapabilityOfflineExpansion) && slices.Contains(identity.Capabilities, CapabilityDurableRestack) {
		capabilities = append(capabilities, CapabilityOfflineExpansion, CapabilityDurableRestack)
	}
	return &api.BackendIdentity{Protocol: identity.Protocol, InstanceId: identity.InstanceId, RepositoryIds: []string{w.RepositoryID}, Capabilities: capabilities}, nil
}
func (w *BackendWorker) EnsureVolume(ctx context.Context, q *api.EnsureVolumeRequest) (*api.VolumeReceipt, error) {
	if q == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	if err := w.authorize(q.Operation); err != nil {
		return nil, err
	}
	return w.Backend.EnsureVolume(ctx, q)
}
func (w *BackendWorker) InspectVolume(ctx context.Context, q *api.InspectVolumeRequest) (*api.VolumeReceipt, error) {
	if q == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	if err := w.authorize(q.Operation); err != nil {
		return nil, err
	}
	return w.Backend.InspectVolume(ctx, q)
}
func (w *BackendWorker) ResolveVolume(ctx context.Context, q *api.ResolveVolumeRequest) (*api.VolumeReceipt, error) {
	if w.Backend == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker backend unavailable")
	}
	if q == nil || q.RepositoryId != w.RepositoryID || q.VolumeId == "" {
		return nil, status.Error(codes.PermissionDenied, "source outside worker repository")
	}
	return w.Backend.ResolveVolume(ctx, q)
}
func (w *BackendWorker) DeleteVolume(ctx context.Context, q *api.DeleteVolumeRequest) (*api.OperationReceipt, error) {
	if q == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	if err := w.authorize(q.Operation); err != nil {
		return nil, err
	}
	return w.Backend.DeleteVolume(ctx, q)
}

func (w *BackendWorker) ExpandVolume(ctx context.Context, q *api.ExpandVolumeRequest) (*api.VolumeReceipt, error) {
	if q == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	if err := w.authorize(q.Operation); err != nil {
		return nil, err
	}
	if !validDigest(q.ExpectedHeadRevision) || !validDigest(q.MaintenanceAuthorizationDigest) || q.CapacityBytes == 0 || q.CapacityBytes%(1<<30) != 0 {
		return nil, status.Error(codes.InvalidArgument, "pinned head, maintenance authorization and GiB-aligned capacity required")
	}
	identity, err := w.Backend.Describe(ctx, &api.DescribeRequest{})
	if err != nil {
		return nil, err
	}
	if err = ValidateBackend(identity, CapabilityOfflineExpansion); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return w.Backend.ExpandVolume(ctx, q)
}
func DialWorker(ctx context.Context, endpoint string, t *tls.Config) (*grpc.ClientConn, error) {
	if endpoint == "" || t == nil || t.InsecureSkipVerify || len(t.Certificates) == 0 || t.RootCAs == nil {
		return nil, errors.New("authenticated worker TLS configuration required")
	}
	connection, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(t)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20), grpc.MaxCallSendMsgSize(4<<20)))
	if err != nil {
		return nil, err
	}
	check, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	identity, err := api.NewNativeStorageBackendClient(connection).Describe(check, &api.DescribeRequest{})
	if err == nil {
		err = ValidateBackend(identity, CapabilityNativeVolume)
	}
	if err != nil {
		connection.Close()
		return nil, err
	}
	return connection, nil
}
