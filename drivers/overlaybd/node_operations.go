package overlaybd

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	api "ssf.dev/ssf/api/overlaybd/v1"
)

// NodeOperations is the persistent native-device boundary behind CSI Node.
// Its caller must authenticate the signed SSF plan before supplying an intent.
// It does not create VMs, grant a writer, or infer a stop from a missing process.
type NodeOperations struct {
	Sessions *Sessions
	Backend  api.NativeStorageBackendClient
}

func DeviceRequest(intent SessionIntent) (*api.PrepareDeviceRequest, error) {
	if !validIntent(intent) {
		return nil, errors.New("incomplete authorized native session intent")
	}
	request := &api.PrepareDeviceRequest{Operation: &api.OperationIdentity{OperationId: intent.OperationID, RepositoryId: intent.RepositoryID, VolumeId: intent.VolumeID}, SessionId: intent.ID, Grant: &api.UseGrant{OwnerId: intent.OwnerID, RunId: intent.RunID, Epoch: intent.Epoch, NodeId: intent.NodeID, HeadRevision: intent.HeadRevision, AuthorizationDigest: intent.AuthorizationDigest, Readonly: intent.Readonly, Purpose: intent.Purpose}}
	if !validGrant(request.Grant) {
		return nil, errors.New("native use grant is invalid")
	}
	hash, err := DeviceInputDigest(request)
	if err != nil {
		return nil, err
	}
	request.Operation.InputDigest = hash
	return request, nil
}

func (n *NodeOperations) Prepare(ctx context.Context, intent SessionIntent) (Session, error) {
	if n.Sessions == nil || n.Backend == nil {
		return Session{}, errors.New("native device dependencies unavailable")
	}
	if _, err := n.Sessions.finalization(intent.ID); err == nil {
		return Session{}, ErrTransition
	} else if !errors.Is(err, ErrNotFound) {
		return Session{}, err
	}
	request, err := DeviceRequest(intent)
	if err != nil {
		return Session{}, err
	}
	session, err := n.Sessions.Plan(intent)
	if err != nil {
		return Session{}, err
	}
	var receipt *api.DeviceReceipt
	switch session.State {
	case "Planned":
		session, err = n.Sessions.BeginCreate(session)
		if err != nil {
			return Session{}, err
		}
		receipt, err = n.Backend.PrepareDevice(ctx, request)
	case "Creating", "NeedsRecovery", "Staged":
		if session.ReleaseStarted || session.ReleaseProof != "" || session.PreparationRelease != nil {
			return Session{}, ErrTransition
		}
		receipt, err = n.Backend.InspectDevice(ctx, &api.InspectDeviceRequest{Operation: request.Operation, SessionId: request.SessionId})
		if err == nil && receipt != nil && receipt.State == "Pending" && session.Evidence == nil {
			if !proto.Equal(receipt.Operation, request.Operation) || receipt.SessionId != request.SessionId {
				return Session{}, errors.New("pending device receipt belongs to another operation")
			}
			receipt, err = n.Backend.PrepareDevice(ctx, request)
		}
	default:
		return Session{}, ErrTransition
	}
	if err == nil {
		err = ValidateDeviceReceipt(request, receipt)
	}
	if err != nil {
		if session.State != "NeedsRecovery" {
			_, saveErr := n.Sessions.MarkUnknown(session)
			err = errors.Join(err, saveErr)
		}
		return Session{}, fmt.Errorf("native device requires same-session recovery: %w", err)
	}
	evidence := DeviceEvidence{SessionID: intent.ID, OperationID: intent.OperationID, Epoch: intent.Epoch, HeadRevision: intent.HeadRevision, NodeBootID: receipt.NodeBootId, DaemonInstanceID: receipt.DaemonInstanceId, DeviceIdentity: receipt.DeviceIdentity, Path: receipt.DevicePath, Major: receipt.Major, Minor: receipt.Minor, CapacityBytes: receipt.CapacityBytes, FSUUID: receipt.FsUuid, Readonly: intent.Readonly}
	if session.State == "Staged" {
		if session.Evidence == nil || *session.Evidence != evidence {
			_, saveErr := n.Sessions.MarkUnknown(session)
			return Session{}, errors.Join(errors.New("staged native device identity changed"), saveErr)
		}
		return session, nil
	}
	return n.Sessions.RecordDevice(session, evidence)
}
