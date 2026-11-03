package sandboxschedadapter

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"time"

	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
)

func (n *Node) supportedProfile(p core.VolumeProfile) bool {
	return p.Runtime == n.Config.PreparationRuntime && p.Version == core.PreparationProtocol && p.VolumeMode == "Block" && p.Filesystem == "ext4"
}

// validatePreparation mirrors the agentenv adapter's request validation.
func (n *Node) validatePreparation(q runtime.PreparationRequest) error {
	if !safeID.MatchString(q.Operation) || q.PlanDigest == "" || !n.supportedProfile(q.Spec.Profile) ||
		q.Endpoint.Kind != "BlockDevice" || q.Spec.PVC.UID != q.Endpoint.VolumeUID ||
		q.Spec.PVUID == "" || q.Spec.SourceRevision == "" || q.Endpoint.NodeUID != n.NodeUID ||
		q.Endpoint.NodeBootID != n.BootID || q.Endpoint.Epoch == 0 ||
		(q.Spec.Initialize && q.Endpoint.ReadOnly) {
		return errors.New("invalid sandboxsched volume preparation request")
	}
	return nil
}

// PrepareVolume initializes the volume filesystem directly on the published
// block device. This replaces the agentenv throwaway-sandbox formatter with a
// privileged node-local mkfs, guarded by the same crash-safe intent journal.
func (n *Node) PrepareVolume(ctx context.Context, q runtime.PreparationRequest) (core.PreparationReceipt, error) {
	n.contentMu.Lock()
	defer n.contentMu.Unlock()
	var empty core.PreparationReceipt
	if err := n.validatePreparation(q); err != nil {
		return empty, err
	}
	record, err := n.Journal.updatePrep(q.Operation, func(r *contentRecord) error {
		if r.Request.Operation != "" && core.Digest(r.Request) != core.Digest(q) {
			// Kernel device numbers may change across node boots; logical
			// preparation identity must not.
			previous, next := r.Request, q
			previous.PlanDigest, next.PlanDigest = "", ""
			previous.Endpoint.NodeBootID, next.Endpoint.NodeBootID = "", ""
			previous.Endpoint.Identity, next.Endpoint.Identity = "", ""
			previous.Endpoint.Major, next.Endpoint.Major = 0, 0
			previous.Endpoint.Minor, next.Endpoint.Minor = 0, 0
			if core.Digest(previous) != core.Digest(next) {
				return core.ErrConflict
			}
		}
		if r.Receipt.Released {
			return core.ErrConflict
		}
		r.Request = q
		return nil
	})
	if err != nil {
		return empty, err
	}
	if err = verifyDevice(q.Endpoint.Path, q.Endpoint.Major, q.Endpoint.Minor, n.Config.DeviceRoot); err != nil {
		return empty, err
	}
	uuid := record.Receipt.UUID
	if uuid == "" {
		intent := record.FormatIntent
		if intent == "" {
			ticket, err := newTicket()
			if err != nil {
				return empty, err
			}
			// UUID format for deterministic retry after a crash.
			intent = strings.ToUpper(ticket[:8] + "-" + ticket[8:12] + "-" + ticket[12:16] + "-" + ticket[16:20] + "-" + ticket[20:32])
			if _, err = n.Journal.updatePrep(q.Operation, func(r *contentRecord) error {
				if r.FormatIntent != "" || r.Receipt.UUID != "" || r.Receipt.Released {
					return core.ErrConflict
				}
				r.FormatIntent = intent
				return nil
			}); err != nil {
				return empty, err
			}
		}
		if q.Spec.Initialize {
			formatCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			cmd := exec.CommandContext(formatCtx, "mkfs.ext4", "-q", "-F", "-U", intent, q.Endpoint.Path)
			out, err := cmd.CombinedOutput()
			cancel()
			if err != nil {
				return empty, errors.New("mkfs.ext4 failed: " + string(out))
			}
		}
		readCtx, cancel := context.WithTimeout(ctx, time.Minute)
		cmd := exec.CommandContext(readCtx, "blkid", "-s", "UUID", "-o", "value", q.Endpoint.Path)
		out, err := cmd.Output()
		cancel()
		if err != nil {
			return empty, errors.New("blkid failed: " + string(out))
		}
		read := strings.TrimSpace(string(out))
		if read == "" {
			return empty, errors.New("filesystem UUID unavailable")
		}
		if q.Spec.Initialize && read != strings.ToLower(record.FormatIntent) && read != record.FormatIntent {
			return empty, core.ErrConflict
		}
		uuid = read
	}
	if record.Receipt.UUID != "" && record.Receipt.UUID != uuid {
		return empty, core.ErrConflict
	}
	if err = verifyDevice(q.Endpoint.Path, q.Endpoint.Major, q.Endpoint.Minor, n.Config.DeviceRoot); err != nil {
		return empty, err
	}
	receipt := core.PreparationReceipt{Protocol: core.PreparationProtocol, Operation: q.Operation, PlanDigest: q.PlanDigest,
		BindingID: q.Endpoint.BindingID, VolumeUID: q.Endpoint.VolumeUID, NodeUID: q.Endpoint.NodeUID,
		BootID: q.Endpoint.NodeBootID, Epoch: q.Endpoint.Epoch, UUID: uuid, DeviceIdentity: q.Endpoint.Identity}
	_, err = n.Journal.updatePrep(q.Operation, func(r *contentRecord) error {
		if r.Receipt.Released {
			return core.ErrConflict
		}
		r.Receipt = receipt
		return nil
	})
	return receipt, err
}

// InspectVolumePreparation returns the durable preparation receipt.
func (n *Node) InspectVolumePreparation(_ context.Context, q runtime.PreparationRequest) (core.PreparationReceipt, error) {
	n.contentMu.Lock()
	defer n.contentMu.Unlock()
	r, err := n.Journal.prep(q.Operation)
	if err != nil {
		return core.PreparationReceipt{}, err
	}
	if core.Digest(r.Request) != core.Digest(q) {
		return core.PreparationReceipt{}, core.ErrConflict
	}
	return r.Receipt, nil
}

// ReleaseVolumePreparation retires preparation ownership of the volume.
func (n *Node) ReleaseVolumePreparation(_ context.Context, q runtime.PreparationRequest) (core.PreparationReceipt, error) {
	n.contentMu.Lock()
	defer n.contentMu.Unlock()
	if err := n.validatePreparation(q); err != nil {
		return core.PreparationReceipt{}, err
	}
	if err := verifyDevice(q.Endpoint.Path, q.Endpoint.Major, q.Endpoint.Minor, n.Config.DeviceRoot); err != nil {
		return core.PreparationReceipt{}, err
	}
	r, err := n.Journal.updatePrep(q.Operation, func(r *contentRecord) error {
		if r.Request.Operation == "" {
			r.Request = q
		}
		if core.Digest(r.Request) != core.Digest(q) {
			return core.ErrConflict
		}
		if r.Receipt.Released {
			return nil
		}
		if r.Receipt.UUID == "" {
			if r.FormatIntent != "" {
				return errors.New("unknown formatter outcome retains preparation ownership")
			}
			r.Receipt = core.PreparationReceipt{Protocol: core.PreparationProtocol, Operation: q.Operation, PlanDigest: q.PlanDigest,
				BindingID: q.Endpoint.BindingID, VolumeUID: q.Endpoint.VolumeUID, NodeUID: q.Endpoint.NodeUID, BootID: q.Endpoint.NodeBootID,
				Epoch: q.Endpoint.Epoch, DeviceIdentity: q.Endpoint.Identity, Rejected: true}
		}
		r.Receipt.Released = true
		return nil
	})
	return r.Receipt, err
}
