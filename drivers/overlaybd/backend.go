package overlaybd

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && strings.ToLower(value) == value
}

func validOperation(operation *api.OperationIdentity) bool {
	return operation != nil && operation.OperationId != "" && operation.RepositoryId != "" && operation.VolumeId != "" && validDigest(operation.InputDigest)
}

func validGrant(grant *api.UseGrant) bool {
	return grant != nil && (grant.Purpose == "runtime" || grant.Purpose == "preparation") && grant.OwnerId != "" && grant.RunId != "" && grant.Epoch > 0 && grant.NodeId != "" && grant.HeadRevision != "" && validDigest(grant.AuthorizationDigest)
}

// DeviceInputDigest binds the daemon request to the full use incarnation.
// There are no maps in this protobuf, so prost and Go serialize the same bytes.
func DeviceInputDigest(request *api.PrepareDeviceRequest) (string, error) {
	if request == nil || request.Operation == nil {
		return "", errors.New("device operation identity required")
	}
	unsigned := proto.Clone(request).(*api.PrepareDeviceRequest)
	unsigned.Operation.InputDigest = ""
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(unsigned)
	if err != nil {
		return "", err
	}
	return digest(raw), nil
}

func validFilesystemUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

const (
	CapabilityStaticRegistration = "static-registration-v1"
	CapabilityNativeVolume       = "native-volume-v1"
	CapabilityDeviceSession      = "device-session-v1"
	CapabilityLayerCapture       = "layer-capture-v1"
	CapabilityHeadPublication    = "head-publication-v1"
	CapabilityDurableRestack     = "durable-restack-v1"
)

// DialBackend only accepts a local Unix socket. Cross-node backend workers use
// the authenticated controller channel, never an unauthenticated TCP fallback.
func DialBackend(ctx context.Context, socket string, required ...string) (*grpc.ClientConn, error) {
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket || strings.ContainsRune(socket, 0) {
		return nil, errors.New("canonical absolute backend Unix socket required")
	}
	connection, err := grpc.NewClient("unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20), grpc.MaxCallSendMsgSize(4<<20)))
	if err != nil {
		return nil, err
	}
	check, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	identity, err := api.NewNativeStorageBackendClient(connection).Describe(check, &api.DescribeRequest{})
	if err == nil {
		err = ValidateBackend(identity, required...)
	}
	if err != nil {
		connection.Close()
		return nil, err
	}
	return connection, nil
}

func ValidateBackend(identity *api.BackendIdentity, required ...string) error {
	if identity == nil || identity.Protocol != BackendProtocol || identity.InstanceId == "" || len(identity.RepositoryIds) == 0 {
		return errors.New("incompatible native storage backend")
	}
	if len(required) == 0 {
		required = []string{CapabilityNativeVolume, CapabilityDeviceSession, CapabilityLayerCapture, CapabilityHeadPublication}
	}
	if slices.Contains(required, CapabilityLayerCapture) || slices.Contains(required, CapabilityOfflineExpansion) {
		required = append(append([]string{}, required...), CapabilityDurableRestack)
	}

	for _, required := range required {
		if !slices.Contains(identity.Capabilities, required) {
			return fmt.Errorf("native backend missing %s", required)
		}
	}
	return nil
}

// ValidateVolumeReceipt binds a successful backend result to the exact intent.
// The caller must additionally verify profile/source and accessible topology.
func ValidateVolumeReceipt(request *api.EnsureVolumeRequest, receipt *api.VolumeReceipt) error {
	if request == nil || !validOperation(request.Operation) || request.CapacityBytes == 0 || request.ProfileRevision == "" || receipt == nil || !proto.Equal(request.Operation, receipt.Operation) {
		return errors.New("native volume receipt identity mismatch")
	}
	if receipt.State != "ContentReady" || !validDigest(receipt.HeadRevision) || receipt.HeadRevision != receipt.ContentDigest || !validFilesystemUUID(receipt.FsUuid) || !validDigest(receipt.ContentDigest) || receipt.CapacityGeneration == 0 || receipt.CapacityBytes < request.CapacityBytes || receipt.CapacityBytes%512 != 0 {
		return errors.New("native volume content is not proven ready")
	}
	return nil
}

func ValidateDeviceReceipt(request *api.PrepareDeviceRequest, receipt *api.DeviceReceipt) error {
	if request == nil || !validOperation(request.Operation) || !validGrant(request.Grant) || request.SessionId == "" || receipt == nil || !proto.Equal(request.Operation, receipt.Operation) || !proto.Equal(request.Grant, receipt.Grant) || request.SessionId != receipt.SessionId {
		return errors.New("native device receipt identity mismatch")
	}
	input, err := DeviceInputDigest(request)
	if err != nil || input != request.Operation.InputDigest {
		return errors.New("native device request input digest mismatch")
	}
	if receipt.State != "Staged" || receipt.NodeBootId == "" || receipt.DaemonInstanceId == "" || receipt.DeviceIdentity == "" || receipt.Major == 0 || receipt.CapacityBytes == 0 || receipt.CapacityBytes%512 != 0 || !validFilesystemUUID(receipt.FsUuid) || !filepath.IsAbs(receipt.DevicePath) || filepath.Clean(receipt.DevicePath) != receipt.DevicePath || strings.ContainsRune(receipt.DevicePath, 0) {
		return errors.New("native device receipt is incomplete")
	}
	return nil
}
