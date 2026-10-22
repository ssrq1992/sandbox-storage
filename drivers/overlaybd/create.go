package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	api "ssf.dev/ssf/api/overlaybd/v1"
)

// VolumeBackend is the volume portion of the generated UDS client. It does not
// schedule compute, issue writer grants, or call CSI Node to provision storage.
type VolumeBackend interface {
	EnsureVolume(context.Context, *api.EnsureVolumeRequest, ...grpc.CallOption) (*api.VolumeReceipt, error)
	InspectVolume(context.Context, *api.InspectVolumeRequest, ...grpc.CallOption) (*api.VolumeReceipt, error)
}

// CreateIntent is fixed before contacting the native worker. RepositoryID must
// already be resolved from administrator configuration, not a client filesystem
// path. Source identifies an immutable, pinned native head, never a live alias.
type CreateIntent struct {
	RepositoryID    string            `json:"repository_id"`
	CapacityBytes   uint64            `json:"capacity_bytes"`
	Readonly        bool              `json:"readonly"`
	ProfileRevision string            `json:"profile_revision"`
	Source          *api.VolumeSource `json:"source,omitempty"`
}

// Creator coordinates durable create dispatch. Backend must be the worker for
// this repository's topology; it may not silently switch repositories on retry.
type Creator struct {
	Journal *Journal
	Backend VolumeBackend
}

func (c *Creator) Ensure(ctx context.Context, name string, intent CreateIntent) (*api.VolumeReceipt, error) {
	if c.Journal == nil || c.Backend == nil {
		return nil, errors.New("native create dependencies are unavailable")
	}
	if name == "" || intent.RepositoryID == "" || intent.CapacityBytes == 0 || intent.CapacityBytes%512 != 0 || intent.ProfileRevision == "" {
		return nil, errors.New("incomplete native create intent")
	}
	if intent.Source != nil && (intent.Source.VolumeId == "" || intent.Source.HeadRevision == "") {
		return nil, errors.New("restoration requires a pinned source volume and head")
	}
	if intent.Readonly && intent.Source == nil {
		return nil, errors.New("cannot initialize a readonly blank volume")
	}
	operation, err := c.Journal.Reserve(ctx, "create", name, Identity("volume", name), intent)
	if err != nil {
		return nil, err
	}
	request := &api.EnsureVolumeRequest{Operation: &api.OperationIdentity{OperationId: operation.ID, InputDigest: operation.Digest, RepositoryId: intent.RepositoryID, VolumeId: operation.VolumeID}, CapacityBytes: intent.CapacityBytes, Readonly: intent.Readonly, Source: intent.Source, ProfileRevision: intent.ProfileRevision}
	switch operation.State {
	case Succeeded:
		receipt := new(api.VolumeReceipt)
		if err := protojson.Unmarshal(operation.Result, receipt); err != nil {
			return nil, fmt.Errorf("decode stored native receipt: %w", err)
		}
		return receipt, ValidateVolumeReceipt(request, receipt)
	case Failed:
		return nil, fmt.Errorf("native operation %s failed: %s", operation.ID, operation.Reason)
	case Pending:
		claimed, err := c.Journal.Finish(ctx, operation, Applying, nil, "")
		if err != nil {
			return nil, err
		}
		receipt, callErr := c.Backend.EnsureVolume(ctx, request)
		return c.record(ctx, claimed, request, receipt, callErr)
	case Applying, NeedsRecovery:
		// This also handles a process dying between claiming and sending. A missing
		// worker receipt remains unknown: absence alone cannot authorize reformatting.
		receipt, callErr := c.Backend.InspectVolume(ctx, &api.InspectVolumeRequest{Operation: request.Operation})
		// A backend's explicit Pending receipt is different from a missing or
		// unknown result. Ensure uses the same immutable operation and its native
		// lock; it cannot allocate another formatter or another volume identity.
		if callErr == nil && receipt != nil && receipt.State == "Pending" && proto.Equal(receipt.Operation, request.Operation) {
			receipt, callErr = c.Backend.EnsureVolume(ctx, request)
		}
		return c.record(ctx, operation, request, receipt, callErr)
	default:
		return nil, errors.New("invalid native operation state")
	}
}

func (c *Creator) record(ctx context.Context, operation Operation, request *api.EnsureVolumeRequest, receipt *api.VolumeReceipt, callErr error) (*api.VolumeReceipt, error) {
	if callErr == nil {
		callErr = ValidateVolumeReceipt(request, receipt)
	}
	// A caller timeout must not prevent recording an unknown native side effect.
	save, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if callErr != nil {
		if operation.State == Applying {
			if _, err := c.Journal.Finish(save, operation, NeedsRecovery, nil, callErr.Error()); err != nil {
				return nil, errors.Join(callErr, err)
			}
		}
		return nil, fmt.Errorf("native create requires inspection of %s: %w", operation.ID, callErr)
	}
	raw, err := protojson.Marshal(receipt)
	if err != nil {
		return nil, err
	}
	_, err = c.Journal.Finish(save, operation, Succeeded, json.RawMessage(raw), "")
	if err != nil {
		return nil, err
	}
	return receipt, nil
}
