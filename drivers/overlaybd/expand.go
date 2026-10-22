package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

const CapabilityOfflineExpansion = "offline-expansion-v1"

type ExpansionBackend interface {
	ExpandVolume(context.Context, *api.ExpandVolumeRequest, ...grpc.CallOption) (*api.VolumeReceipt, error)
}
type ExpansionAuthorizer interface {
	AuthorizeNativeExpansion(context.Context, CatalogVolume, string, string, int64) (string, error)
	ReserveNativeExpansion(context.Context, CatalogVolume, string, int64, ...int64) error
	CompleteNativeExpansion(context.Context, CatalogVolume, string, int64) error
}

// ExpansionIntent fixes the original content generation before dispatch. Retry
// must load this intent, never resolve a newer head under the same operation.
type ExpansionIntent struct {
	Volume        CatalogVolume `json:"volume"`
	Head          string        `json:"head"`
	Generation    uint64        `json:"generation"`
	Capacity      uint64        `json:"capacity"`
	Authorization string        `json:"authorization"`
}

type Expander struct {
	Journal    *Journal
	Backend    ExpansionBackend
	Authorizer ExpansionAuthorizer
}

func validateExpansion(intent ExpansionIntent, request *api.ExpandVolumeRequest, receipt *api.VolumeReceipt) error {
	if receipt == nil || !proto.Equal(receipt.Operation, request.Operation) || receipt.State != "ContentReady" ||
		!validDigest(receipt.HeadRevision) || receipt.HeadRevision == intent.Head || receipt.HeadRevision != receipt.ContentDigest ||
		receipt.CapacityBytes != intent.Capacity || receipt.CapacityGeneration != intent.Generation+1 ||
		receipt.FsUuid != intent.Volume.FSUUID || !sameNodes(receipt.AccessibleNodes, intent.Volume.AccessibleNodes) {
		return errors.New("expanded content identity, capacity or generation mismatch")
	}
	return nil
}

// Ensure is the internal persistent driver path. Callers must negotiate the
// host capability before selecting it; it does not itself advertise CSI expand.
// The native operation is replayed with its original identity after a lost
// response. It must inspect its WAL rather than rerun an unknown resize child.
func (e *Expander) Ensure(ctx context.Context, name string, volume CatalogVolume, head string, generation, capacity uint64) (*api.VolumeReceipt, error) {
	if e.Journal == nil || e.Backend == nil || e.Authorizer == nil || name == "" || volume.Readonly ||
		!validDigest(head) || generation == 0 || generation == math.MaxUint64 || capacity > math.MaxInt64 ||
		capacity <= volume.CapacityBytes || capacity%(1<<30) != 0 {
		return nil, errors.New("offline expansion requires a growing GiB-aligned capacity and pinned writable head")
	}
	id := Identity("expand", name)
	authorization, err := e.Authorizer.AuthorizeNativeExpansion(ctx, volume, id, head, int64(capacity))
	if err != nil {
		return nil, err
	}
	if !validDigest(authorization) {
		return nil, errors.New("invalid offline maintenance authorization")
	}
	intent := ExpansionIntent{volume, head, generation, capacity, authorization}
	operation, err := e.Journal.Reserve(ctx, "expand", name, volume.NativeID, intent)
	if err != nil {
		return nil, err
	}
	request := &api.ExpandVolumeRequest{
		Operation:            &api.OperationIdentity{OperationId: operation.ID, InputDigest: operation.Digest, RepositoryId: volume.RepositoryID, VolumeId: volume.NativeID},
		ExpectedHeadRevision: head, CapacityBytes: capacity, MaintenanceAuthorizationDigest: authorization,
	}
	if operation.State == Succeeded {
		receipt := new(api.VolumeReceipt)
		if err = json.Unmarshal(operation.Result, receipt); err != nil {
			return nil, err
		}
		return receipt, validateExpansion(intent, request, receipt)
	}
	if operation.State == Failed {
		return nil, errors.New(operation.Reason)
	}
	if operation.State == Pending {
		operation, err = e.Journal.Finish(ctx, operation, Applying, nil, "")
		if err != nil {
			return nil, err
		}
	}
	receipt, callErr := e.Backend.ExpandVolume(ctx, request)
	if callErr == nil {
		callErr = validateExpansion(intent, request, receipt)
	}
	save, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if callErr != nil {
		if operation.State == Applying {
			_, err = e.Journal.Finish(save, operation, NeedsRecovery, nil, callErr.Error())
			callErr = errors.Join(callErr, err)
		}
		return nil, callErr
	}
	if _, err = e.Journal.Finish(save, operation, Succeeded, receipt, ""); err != nil {
		return nil, err
	}
	return receipt, nil
}
