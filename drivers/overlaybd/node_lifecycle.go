package overlaybd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/auth"
	"ssf.dev/ssf/core"
)

// ManagedNodeService is registered only on the driver's private CSI endpoint.
// Runtime evidence is independently verified even on this trusted transport.
type ManagedNodeService struct {
	v1.UnimplementedManagedStorageDriverServer
	Node *CSINode
}

func (s *ManagedNodeService) FinalizeRuntime(ctx context.Context, q *v1.DriverFinalizeRequest) (*v1.Operation, error) {
	if q == nil || q.BindingUid == "" || q.DetachedEvidence == "" {
		return nil, status.Error(codes.InvalidArgument, "signed binding and detached evidence required")
	}
	n := s.Node
	p, err := auth.VerifyRevocation(q.SignedNodePlan, n.Authorization.Key, n.Authorization.Node, n.Authorization.NodeUID, n.Authorization.Boot)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	if p.Purpose != "" || p.NodeIDs[DriverName] != n.Authorization.CSINode {
		return nil, status.Error(codes.PermissionDenied, "runtime node scope mismatch")
	}
	count := 0
	var binding core.Binding
	for _, b := range p.Bindings {
		if b.ID == q.BindingUid {
			binding = b
			count++
		}
	}
	if count != 1 || binding.Volume.Driver != DriverName || binding.Node != p.Node || binding.Tenant != p.Tenant || binding.Sandbox != p.Sandbox {
		return nil, status.Error(codes.PermissionDenied, "binding scope mismatch")
	}
	id := core.Hash("overlaybd/session/" + binding.ID)
	session, err := n.Operations.Sessions.Get(id)
	if errors.Is(err, ErrNotFound) {
		return &v1.Operation{OperationId: q.BindingUid, State: "Unstarted"}, nil
	}
	if err != nil {
		return nil, err
	}
	p.ID = ""
	p.Expires = time.Time{}
	if session.Intent.AuthorizationDigest != core.Digest(p) || session.Intent.RunID != binding.RunUID || session.Intent.OwnerID != binding.OwnerUID || session.Intent.Epoch != binding.Epoch {
		return nil, status.Error(codes.PermissionDenied, "session grant differs from signed plan")
	}
	if session.Evidence == nil {
		session, err = n.Operations.Prepare(ctx, session.Intent)
		if err != nil {
			return nil, err
		}
	}
	head := session.PublishedHead
	if head == "" {
		head = session.Intent.HeadRevision
	}
	if final, err := n.Operations.Sessions.finalization(id); err == nil {
		head = final.ExpectedHead
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if err = n.FinalizeStopped(ctx, id, q.DetachedEvidence, head); err != nil {
		return nil, err
	}
	return &v1.Operation{OperationId: q.BindingUid, State: "Finalized"}, nil
}
func RegisterNode(server *grpc.Server, node *CSINode) error {
	if node == nil || node.Operations == nil || node.Operations.Backend == nil || node.Operations.Sessions == nil || node.Mounts == nil || node.Evidence == nil {
		return errors.New("complete native node dependencies required")
	}
	csi.RegisterIdentityServer(server, &NodeIdentity{Node: node})
	csi.RegisterNodeServer(server, node)
	v1.RegisterManagedStorageDriverServer(server, &ManagedNodeService{Node: node})
	return nil
}

// FinalizePreparation accepts the public Node Agent's durable release receipt
// on the private driver endpoint. Access to this endpoint is a node authority:
// it must never be exposed through an untrusted kubelet/public TCP listener.
func (s *ManagedNodeService) FinalizePreparation(ctx context.Context, q *v1.DriverPreparationReleaseRequest) (*v1.Operation, error) {
	if q == nil || q.BindingUid == "" || len(q.ReceiptJson) > 65536 {
		return nil, status.Error(codes.InvalidArgument, "preparation identity required")
	}
	n := s.Node
	n.mu.Lock()
	defer n.mu.Unlock()
	p, err := auth.VerifyRevocation(q.SignedNodePlan, n.Authorization.Key, n.Authorization.Node, n.Authorization.NodeUID, n.Authorization.Boot)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	if p.Purpose != core.PurposePreparation || p.NodeIDs[DriverName] != n.Authorization.CSINode || len(p.Bindings) != 1 || p.Bindings[0].ID != q.BindingUid || p.Bindings[0].Volume.Driver != DriverName {
		return nil, status.Error(codes.PermissionDenied, "preparation scope mismatch")
	}
	b := p.Bindings[0]
	var receipt core.PreparationReceipt
	if err = json.Unmarshal([]byte(q.ReceiptJson), &receipt); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid preparation receipt")
	}
	if receipt.Protocol != core.PreparationProtocol || !receipt.Released || receipt.Operation != p.Operation || receipt.PlanDigest != core.PreparationDigest(p) || receipt.BindingID != b.ID || receipt.VolumeUID != b.Volume.PVC.UID || receipt.NodeUID != p.NodeUID || receipt.BootID != p.BootID || receipt.Epoch != b.Epoch {
		return nil, status.Error(codes.PermissionDenied, "preparation receipt differs from signed operation")
	}
	id := core.Hash("overlaybd/session/" + b.ID)
	session, err := n.Operations.Sessions.Get(id)
	if errors.Is(err, ErrNotFound) {
		return &v1.Operation{OperationId: q.BindingUid, State: "Unstarted"}, nil
	}
	if err != nil {
		return nil, err
	}
	if session.Intent.AuthorizationDigest != core.PreparationDigest(p) || session.Intent.Purpose != "preparation" {
		return nil, status.Error(codes.PermissionDenied, "preparation session grant mismatch")
	}
	if session.Evidence == nil {
		session, err = n.Operations.Prepare(ctx, session.Intent)
		if err != nil {
			return nil, err
		}
	}
	if !receipt.Rejected && (receipt.UUID != session.Evidence.FSUUID || receipt.DeviceIdentity != fmt.Sprintf("%d:%d", session.Evidence.Major, session.Evidence.Minor)) {
		return nil, status.Error(codes.PermissionDenied, "preparation device receipt mismatch")
	}
	proof := PreparationProof{SessionID: id, DeviceIdentity: session.Evidence.DeviceIdentity, NodeBootID: session.Evidence.NodeBootID, DaemonInstanceID: session.Evidence.DaemonInstanceID, Epoch: session.Intent.Epoch, Digest: core.Digest(receipt)}
	if _, err = n.Operations.Sessions.PinPreparationRelease(session, proof); err != nil {
		return nil, err
	}
	return &v1.Operation{OperationId: q.BindingUid, State: "Finalized"}, nil
}
func (n *CSINode) finishPreparation(ctx context.Context, s Session) error {
	if s.PreparationRelease == nil {
		return errors.New("preparer release evidence missing")
	}
	var err error
	if s.State == "Staged" {
		s, err = n.Operations.Sessions.BeginPreparationRelease(s, *s.PreparationRelease)
		if err != nil {
			return err
		}
	}
	_, err = n.releaseSwept(ctx, s, ReleaseIntent{PreparationDigest: s.PreparationRelease.Digest})
	return err
}
