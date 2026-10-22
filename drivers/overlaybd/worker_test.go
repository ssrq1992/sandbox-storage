package overlaybd

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
)

type repositoryBackendFixture struct {
	api.NativeStorageBackendClient
	calls        int
	capabilities []string
}

func (b *repositoryBackendFixture) Describe(context.Context, *api.DescribeRequest, ...grpc.CallOption) (*api.BackendIdentity, error) {
	capabilities := b.capabilities
	if capabilities == nil {
		capabilities = []string{CapabilityNativeVolume, CapabilityDeviceSession}
	}
	return &api.BackendIdentity{Protocol: BackendProtocol, InstanceId: "native", RepositoryIds: []string{"repo", "private"}, Capabilities: capabilities}, nil
}
func (b *repositoryBackendFixture) EnsureVolume(_ context.Context, r *api.EnsureVolumeRequest, _ ...grpc.CallOption) (*api.VolumeReceipt, error) {
	b.calls++
	return &api.VolumeReceipt{Operation: r.Operation}, nil
}
func TestRepositoryWorkerDoesNotExposeNodeOrForeignRepository(t *testing.T) {
	backend := new(repositoryBackendFixture)
	worker := &BackendWorker{RepositoryID: "repo", Backend: backend}
	ctx := context.Background()
	identity, err := worker.Describe(ctx, &api.DescribeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(identity.RepositoryIds) != 1 || identity.RepositoryIds[0] != "repo" || len(identity.Capabilities) != 1 || identity.Capabilities[0] != CapabilityNativeVolume {
		t.Fatal("worker exposed unrelated capabilities")
	}
	q := &api.EnsureVolumeRequest{Operation: &api.OperationIdentity{OperationId: "op", InputDigest: core.Hash("intent"), RepositoryId: "private", VolumeId: "volume"}}
	if _, err = worker.EnsureVolume(ctx, q); status.Code(err) != codes.PermissionDenied || backend.calls != 0 {
		t.Fatal("foreign repository reached host")
	}
	q.Operation.RepositoryId = "repo"
	if _, err = worker.EnsureVolume(ctx, q); err != nil || backend.calls != 1 {
		t.Fatal("repository operation not forwarded", err)
	}
	if _, err = worker.ExpandVolume(ctx, &api.ExpandVolumeRequest{Operation: q.Operation, ExpectedHeadRevision: core.Hash("head"), MaintenanceAuthorizationDigest: core.Hash("maintenance"), CapacityBytes: 2 << 30}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old host silently accepted expansion: %v", err)
	}
	if _, err = worker.PrepareDevice(ctx, &api.PrepareDeviceRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatal("worker exposed device operations")
	}
	if _, err = worker.CaptureDevice(ctx, &api.CaptureDeviceRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatal("worker exposed runtime capture")
	}
	if _, err = DialWorker(ctx, "localhost:1234", nil); err == nil {
		t.Fatal("unauthenticated worker allowed")
	}
}

func TestWorkerDoesNotAdvertiseUnsafeExpansionFromOldHost(t *testing.T) {
	backend := &repositoryBackendFixture{capabilities: []string{CapabilityNativeVolume, CapabilityOfflineExpansion}}
	worker := &BackendWorker{RepositoryID: "repo", Backend: backend}
	identity, err := worker.Describe(context.Background(), &api.DescribeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if ValidateBackend(identity, CapabilityOfflineExpansion) == nil {
		t.Fatal("worker advertised non-durable expansion")
	}
	backend.capabilities = append(backend.capabilities, CapabilityDurableRestack)
	identity, err = worker.Describe(context.Background(), &api.DescribeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBackend(identity, CapabilityOfflineExpansion); err != nil {
		t.Fatal(err)
	}
}
