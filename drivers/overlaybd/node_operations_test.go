package overlaybd

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

type deviceBackend struct {
	api.NativeStorageBackendClient
	creates      int
	result       *api.DeviceReceipt
	inspectError error
	wrongPending bool
}

func (b *deviceBackend) PrepareDevice(_ context.Context, r *api.PrepareDeviceRequest, _ ...grpc.CallOption) (*api.DeviceReceipt, error) {
	b.creates++
	b.result = &api.DeviceReceipt{Operation: proto.Clone(r.Operation).(*api.OperationIdentity),
		Grant: proto.Clone(r.Grant).(*api.UseGrant), SessionId: r.SessionId, State: "Staged",
		DevicePath: "/dev/ublkb4", Major: 252, Minor: 4, CapacityBytes: 1 << 30,
		FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a", NodeBootId: "boot", DaemonInstanceId: "daemon", DeviceIdentity: "device"}
	return nil, context.DeadlineExceeded // Native creation succeeded; RPC reply was lost.
}
func (b *deviceBackend) InspectDevice(_ context.Context, r *api.InspectDeviceRequest, _ ...grpc.CallOption) (*api.DeviceReceipt, error) {
	if b.inspectError != nil {
		return nil, b.inspectError
	}
	if b.result != nil {
		return proto.Clone(b.result).(*api.DeviceReceipt), nil
	}
	result := &api.DeviceReceipt{Operation: proto.Clone(r.Operation).(*api.OperationIdentity), SessionId: r.SessionId, State: "Pending"}
	if b.wrongPending {
		result.Operation.OperationId = "another-operation"
	}
	return result, nil
}
func TestDeviceResponseLossAndWALRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.db")
	wal, err := OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	input := intent()
	input.AuthorizationDigest = strings.Repeat("a", 64)
	backend := new(deviceBackend)
	node := &NodeOperations{Sessions: wal, Backend: backend}
	if _, err = node.Prepare(context.Background(), input); err == nil {
		t.Fatal("lost reply reported success")
	}
	wal.Close()
	wal, err = OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	node = &NodeOperations{Sessions: wal, Backend: backend}
	session, err := node.Prepare(context.Background(), input)
	if err != nil || session.State != "Staged" || backend.creates != 1 {
		t.Fatalf("unsafe recovery: %+v %v creates=%d", session, err, backend.creates)
	}
	backend.result.DeviceIdentity = "reused-device-number"
	if _, err = node.Prepare(context.Background(), input); err == nil {
		t.Fatal("device identity replacement accepted")
	}
}
func TestDeviceUnknownDispatchRequiresExactPendingReceipt(t *testing.T) {
	for _, scenario := range []string{"missing", "wrong-operation", "pending"} {
		t.Run(scenario, func(t *testing.T) {
			wal, err := OpenSessions(filepath.Join(t.TempDir(), "sessions.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			input := intent()
			input.AuthorizationDigest = strings.Repeat("a", 64)
			session, err := wal.Plan(input)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = wal.BeginCreate(session); err != nil {
				t.Fatal(err)
			}
			backend := new(deviceBackend)
			if scenario == "missing" {
				backend.inspectError = context.DeadlineExceeded
			}
			backend.wrongPending = scenario == "wrong-operation"
			node := &NodeOperations{Sessions: wal, Backend: backend}
			if _, err = node.Prepare(context.Background(), input); err == nil {
				t.Fatal("unknown result reported success")
			}
			want := 0
			if scenario == "pending" {
				want = 1
			}
			if backend.creates != want {
				t.Fatalf("creates=%d want=%d", backend.creates, want)
			}
		})
	}
}

type releaseBackend struct {
	api.NativeStorageBackendClient
	calls   int
	request *api.ReleaseDeviceRequest
}

func (b *releaseBackend) ReleaseDevice(_ context.Context, request *api.ReleaseDeviceRequest, _ ...grpc.CallOption) (*api.OperationReceipt, error) {
	b.calls++
	if b.request == nil {
		b.request = proto.Clone(request).(*api.ReleaseDeviceRequest)
		return nil, context.DeadlineExceeded
	}
	if !proto.Equal(request, b.request) {
		return nil, ErrConflict
	}
	return &api.OperationReceipt{Operation: request.Operation, State: "Released"}, nil
}
func TestReleaseResponseLossKeepsPublicationAndStopIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.db")
	wal, err := OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	input := intent()
	input.AuthorizationDigest = strings.Repeat("a", 64)
	session, err := wal.Plan(input)
	session = mustSession(t, session, err)
	session, err = wal.BeginCreate(session)
	session = mustSession(t, session, err)
	session, err = wal.RecordDevice(session, evidence())
	session = mustSession(t, session, err)
	stop := StopProof{SessionID: input.ID, RunID: input.RunID, Epoch: input.Epoch, DeviceIdentity: evidence().DeviceIdentity, Digest: strings.Repeat("b", 64)}
	session, err = wal.BeginRelease(session, stop)
	session = mustSession(t, session, err)
	release := ReleaseIntent{StopDigest: stop.Digest, BorrowedRefsDigest: strings.Repeat("c", 64), CaptureID: "final", PublishedHead: strings.Repeat("d", 64)}
	backend := new(releaseBackend)
	node := &NodeOperations{Sessions: wal, Backend: backend}
	if _, err = node.Release(context.Background(), input.ID, release); err == nil {
		t.Fatal("lost release reply reported success")
	}
	wal.Close()
	wal, err = OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	node.Sessions = wal
	changed := release
	changed.PublishedHead = strings.Repeat("e", 64)
	if _, err = node.Release(context.Background(), input.ID, changed); err == nil || backend.calls != 1 {
		t.Fatal("release proof changed on recovery")
	}
	session, err = node.Release(context.Background(), input.ID, release)
	if err != nil || session.State != "Released" {
		t.Fatalf("release recovery failed: %v", err)
	}
	if _, err = node.Release(context.Background(), input.ID, release); err != nil || backend.calls != 2 {
		t.Fatal("completed release dispatched again")
	}
}
func TestWritableReleaseCannotSkipFinalPublication(t *testing.T) {
	wal, err := OpenSessions(filepath.Join(t.TempDir(), "sessions.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	input := intent()
	input.AuthorizationDigest = strings.Repeat("a", 64)
	session, err := wal.Plan(input)
	session = mustSession(t, session, err)
	session, err = wal.BeginCreate(session)
	session = mustSession(t, session, err)
	session, err = wal.RecordDevice(session, evidence())
	session = mustSession(t, session, err)
	proof := StopProof{SessionID: input.ID, RunID: input.RunID, Epoch: input.Epoch, DeviceIdentity: evidence().DeviceIdentity, Digest: strings.Repeat("b", 64)}
	session, err = wal.BeginRelease(session, proof)
	session = mustSession(t, session, err)
	backend := new(releaseBackend)
	node := &NodeOperations{Sessions: wal, Backend: backend}
	if _, err = node.Release(context.Background(), input.ID, ReleaseIntent{StopDigest: proof.Digest, BorrowedRefsDigest: strings.Repeat("c", 64)}); err == nil || backend.calls != 0 {
		t.Fatal("writable device released without a final head")
	}
}
