package agentenvadapter

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"slices"

	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
)

// NativeCapture is a generic runtime receipt, containing neither CSI options nor
// host paths. It is emitted only after filesystem freeze, VMM pause and durable
// native capture. A later capturing/ready/stopped event invalidates old proofs.
type NativeCapture struct {
	BarrierID string `json:"barrier_id"`
	Artifact  string `json:"artifact"`
	Digest    string `json:"digest"`
}

func (c NativeCapture) valid() bool {
	return safeID.MatchString(c.BarrierID) && safeID.MatchString(c.Artifact) && len(c.Digest) == 64 && isHex(c.Digest)
}
func isHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

type captureEvidenceRequest struct {
	Intent   consume.CaptureIntent `json:"intent"`
	Bindings []core.Binding        `json:"bindings"`
	Proof    string                `json:"proof"`
}
type nativeCaptureVerifier struct {
	HTTP *HTTPClient
	URL  string
}

func (v nativeCaptureVerifier) VerifyCapture(ctx context.Context, q consume.CaptureIntent, bs []core.Binding, proof string) error {
	if v.HTTP == nil || v.URL == "" {
		return errors.New("capture evidence endpoint unavailable")
	}
	return v.HTTP.Call(ctx, "POST", base(v.URL)+"/v1/capture/evidence", captureEvidenceRequest{q, bs, proof}, nil)
}

func (c *NativeControl) Capture(ctx context.Context, id, nodeID string, receipt NativeCapture, proof string) (consume.CaptureManifest, error) {
	return c.capturePhase(ctx, id, nodeID, receipt, proof, false)
}

func (c *NativeControl) capturePhase(ctx context.Context, id, nodeID string, receipt NativeCapture, proof string, sealOnly bool) (consume.CaptureManifest, error) {
	var result consume.CaptureManifest
	if !receipt.valid() {
		return result, errors.New("invalid native capture receipt")
	}
	err := c.Ledger.Exclusive(ctx, "agentenv/native-run/"+id, func(ctx context.Context) error {
		var retired bool
		if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agentenv_native_retired_captures WHERE request_id=$1 AND barrier_id=$2)`, id, receipt.BarrierID).Scan(&retired); err != nil {
			return err
		}
		if retired {
			return core.ErrConflict
		}
		d, err := c.Dispatch(ctx, id)
		if err != nil {
			return err
		}
		var state string
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, id).Scan(&state); err != nil {
			return err
		}
		if state != "Ready" && state != "Pausing" {
			return core.ErrConflict
		}
		if nodeID != d.Candidate.ID || d.Result.Operation == "" {
			return core.ErrConflict
		}
		candidate, err := c.stopCandidate(ctx, d.Candidate, nodeID)
		if err != nil {
			return err
		}
		// A reboot is not a capture barrier: only the original run's host boot may
		// finish a checkpoint. Stop/fencing has a separate, broader identity policy.
		if candidate.BootID != d.Candidate.BootID {
			return core.ErrConflict
		}
		q := consume.CaptureIntent{Version: consume.CaptureVersion, ID: core.Hash(id + "/capture/" + receipt.BarrierID), Operation: d.Result.Operation, OwnerUID: d.OwnerUID, RunUID: d.RunUID, BarrierID: receipt.BarrierID, NativeArtifact: receipt.Artifact, NativeDigest: receipt.Digest, Classes: map[string]string{}}
		for _, mount := range d.ManagedMounts {
			volumeName := mount.Name
			if mapped := d.VolumeNames[mount.Name]; mapped != "" {
				volumeName = mapped
			}
			mountID := d.MountIDs[mount.Name]
			if mountID == "" {
				mountID = "ext_" + core.Hash(mount.Name)[:16]
			}
			volume, err := c.Volumes.Observe(ctx, c.Principal.Tenant, volumeName)
			if err != nil {
				return err
			}
			found := false
			for index, b := range d.Result.Plan.Bindings {
				if b.MountID == mountID && b.Volume.PVC.UID == volume.PVC.UID {
					if volume.PVCSource && volume.SnapshotClass == "" {
						volume.SnapshotClass, err = c.Kube.DefaultSnapshotClass(ctx, b.Volume.Driver)
						if err != nil {
							return err
						}
					}
					q.Classes[b.ID] = volume.SnapshotClass
					var captured NativeCapturedVolume
					if err := c.Ledger.Step(ctx, "agentenv/capture-volume/"+q.ID+"/"+b.ID, volume.PVC.UID, func(context.Context) (any, error) {
						return NativeCapturedVolume{Volume: volume, Index: index, MountName: mount.Name}, nil
					}, &captured); err != nil {
						return err
					}
					if captured.Volume.PVC != volume.PVC || captured.Volume.SnapshotClass != volume.SnapshotClass || captured.Index != index || captured.MountName != mount.Name {
						return core.ErrConflict
					}
					found = true
					break
				}
			}
			if !found {
				return core.ErrConflict
			}
		}
		checkpoints := consume.Checkpoints{CaptureParticipants: c.captureParticipants(d), PrepareParticipants: c.managedCapture(d, true, false), PublishParticipants: c.managedCapture(d, false, true), SealedParticipants: func(b core.Binding) bool { return b.Volume.Driver == "overlaybd.csi.ssf.dev" }, Ledger: c.Ledger, Kube: c.Kube, Evidence: nativeCaptureVerifier{HTTP: c.HTTP, URL: candidate.PluginURL}}
		if sealOnly {
			result, err = checkpoints.Seal(ctx, q, proof)
		} else {
			result, err = checkpoints.Advance(ctx, q, proof)
		}
		return err
	})
	return result, err
}

func (n *Node) captureProof(r NodeRecord, c NativeCapture) string {
	return n.proof(r, "capture/"+core.Digest(c))
}
func (n *Node) verifyCapture(record NodeRecord, q captureEvidenceRequest) error {
	if record.Native == nil || record.Runtime.Capture == nil || record.Stop || record.DeleteRequested || record.State != "Ready" || record.BootID != n.BootID || record.PIDNamespace != n.PIDNamespace {
		return core.ErrConflict
	}
	receipt := *record.Runtime.Capture
	d := record.Native
	if !receipt.valid() || q.Intent.ID != core.Hash(record.Request.ID+"/capture/"+receipt.BarrierID) || q.Intent.Operation != d.Result.Operation || q.Intent.OwnerUID != d.OwnerUID || q.Intent.RunUID != d.RunUID || q.Intent.BarrierID != receipt.BarrierID || q.Intent.NativeArtifact != receipt.Artifact || q.Intent.NativeDigest != receipt.Digest || subtle.ConstantTimeCompare([]byte(q.Proof), []byte(n.captureProof(record, receipt))) != 1 {
		return core.ErrConflict
	}
	if len(q.Bindings) != len(d.Result.Plan.Bindings) {
		return core.ErrConflict
	}
	expected := map[string]core.Binding{}
	for _, b := range d.Result.Plan.Bindings {
		expected[b.ID] = b
	}
	for _, b := range q.Bindings {
		p, ok := expected[b.ID]
		if !ok || p.Epoch != b.Epoch || p.Volume.PVC.UID != b.Volume.PVC.UID || p.Volume.Key() != b.Volume.Key() || b.Owner() != d.OwnerUID || b.Run() != d.RunUID {
			return core.ErrConflict
		}
		delete(expected, b.ID)
	}
	// Read the authoritative native outbox again: HTTP acceptance alone is stale
	// as soon as the runtime has started thaw or another capture.
	ev, err := n.nativeEvent(record.Runtime.InstanceID)
	if err != nil {
		return err
	}
	if ev.Event != "capturing" || ev.Capture == nil || core.Digest(ev) != core.Digest(record.Runtime) || ev.Ticket != record.Ticket || ev.BootID != n.BootID || core.Digest(ev.Capture) != core.Digest(receipt) || !slices.Equal(ev.Devices, record.Devices) {
		return core.ErrConflict
	}
	return verifyProcessDevices(ev, record.Devices)
}
func (n *Node) captureEvidence(w http.ResponseWriter, r *http.Request) {
	if !allowed(identity(r), n.Config.EvidencePeers) {
		problem(w, 403, errors.New("capture evidence peer forbidden"))
		return
	}
	var q captureEvidenceRequest
	if err := decode(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	ids, err := n.Journal.Pending()
	if err != nil {
		problem(w, 503, err)
		return
	}
	for _, id := range ids {
		record, err := n.Journal.Get(id)
		if err == nil && record.Native != nil && record.Native.RunUID == q.Intent.RunUID {
			if err = n.verifyCapture(record, q); err != nil {
				problem(w, 409, err)
				return
			}
			w.WriteHeader(204)
			return
		}
	}
	problem(w, 403, errors.New("unknown capture run"))
}
