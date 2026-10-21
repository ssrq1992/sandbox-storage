package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	bolt "go.etcd.io/bbolt"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
)

const AgentENVBlockProfile = "agentenv-block-ext4-v1"

func DescribeVolumeProfile() core.VolumeProfile {
	return core.VolumeProfile{Name: AgentENVBlockProfile, Runtime: "agentenv", Version: core.PreparationProtocol, VolumeMode: "Block", Filesystem: "ext4", InitializeBlank: true}
}

type contentRecord struct {
	Request      runtime.PreparationRequest `json:"request"`
	FormatIntent string                     `json:"formatIntent,omitempty"`
	Receipt      core.PreparationReceipt    `json:"receipt"`
}

func (j *Journal) content(id string) (contentRecord, error) {
	var r contentRecord
	err := j.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("volume-preparations")).Get([]byte(id))
		if b == nil {
			return ErrNotFound
		}
		return json.Unmarshal(b, &r)
	})
	return r, err
}
func (j *Journal) pendingContent() (int, error) {
	count := 0
	err := j.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("volume-preparations")).ForEach(func(_, raw []byte) error {
			var r contentRecord
			if err := json.Unmarshal(raw, &r); err != nil {
				return err
			}
			if !r.Receipt.Released {
				count++
			}
			return nil
		})
	})
	return count, err
}
func (j *Journal) updateContent(id string, change func(*contentRecord) error) (contentRecord, error) {
	var r contentRecord
	err := j.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("volume-preparations"))
		raw := bucket.Get([]byte(id))
		if raw != nil {
			if e := json.Unmarshal(raw, &r); e != nil {
				return e
			}
		}
		if e := change(&r); e != nil {
			return e
		}
		raw, e := json.Marshal(r)
		if e != nil {
			return e
		}
		return bucket.Put([]byte(id), raw)
	})
	return r, err
}

func (n *Node) validateContent(q runtime.PreparationRequest) error {
	if _, err := n.Plan(context.Background(), q.Spec); err != nil {
		return err
	}
	if !safeID.MatchString(q.Operation) || q.PlanDigest == "" || q.Spec.Profile != DescribeVolumeProfile() ||
		q.Endpoint.Kind != "BlockDevice" || q.Spec.PVC.UID != q.Endpoint.VolumeUID ||
		q.Spec.PVUID == "" || q.Spec.SourceRevision == "" || q.Endpoint.NodeUID != n.Config.NodeUID ||
		q.Endpoint.NodeBootID != n.BootID || q.Endpoint.Epoch == 0 ||
		(q.Spec.Initialize && q.Endpoint.ReadOnly) {
		return errors.New("invalid AgentENV volume preparation request")
	}
	return nil
}

var _ runtime.VolumePreparationPlanner = (*Node)(nil)

func (n *Node) Describe(context.Context) ([]core.VolumeProfile, error) {
	return []core.VolumeProfile{DescribeVolumeProfile()}, nil
}

func (n *Node) Plan(_ context.Context, spec core.PreparationSpec) (core.PreparationSpec, error) {
	if spec.Profile != DescribeVolumeProfile() || spec.PVC.UID == "" ||
		spec.PVUID == "" || spec.SourceRevision == "" {
		return spec, errors.New("unsupported or unpinned AgentENV content intent")
	}
	if spec.Initialize && spec.SourceRevision != core.Digest("blank") {
		return spec, errors.New("restored content cannot authorize initialization")
	}
	return spec, nil
}
func contentDevice(q runtime.PreparationRequest) Device {
	e := q.Endpoint
	return Device{ID: e.BindingID, Path: e.Path, ReadOnly: e.ReadOnly, Major: e.Major, Minor: e.Minor, Bytes: e.Size}
}

func preparationRequestReboot(previous, next runtime.PreparationRequest) bool {
	if previous.Endpoint.NodeUID == "" || previous.Endpoint.NodeBootID == "" ||
		next.Endpoint.NodeBootID == "" || previous.Endpoint.NodeBootID == next.Endpoint.NodeBootID {
		return false
	}
	// The public Node Agent republishes the exact signed backend binding.
	// Kernel device numbers may change across boots; logical identity may not.
	previous.PlanDigest, next.PlanDigest = "", ""
	previous.Endpoint.NodeBootID, next.Endpoint.NodeBootID = "", ""
	previous.Endpoint.Identity, next.Endpoint.Identity = "", ""
	previous.Endpoint.Major, next.Endpoint.Major = 0, 0
	previous.Endpoint.Minor, next.Endpoint.Minor = 0, 0
	return core.Digest(previous) == core.Digest(next)
}

func (n *Node) PrepareVolume(ctx context.Context, q runtime.PreparationRequest) (core.PreparationReceipt, error) {
	n.contentMu.Lock()
	defer n.contentMu.Unlock()
	var empty core.PreparationReceipt
	if err := n.validateContent(q); err != nil {
		return empty, err
	}
	n.workMu.Lock()
	if n.draining {
		n.workMu.Unlock()
		return empty, errors.New("preparation node draining")
	}
	n.workers.Add(1)
	n.workMu.Unlock()
	defer n.workers.Done()
	record, err := n.Journal.updateContent(q.Operation, func(r *contentRecord) error {
		if r.Request.Operation != "" && core.Digest(r.Request) != core.Digest(q) && !preparationRequestReboot(r.Request, q) {
			return core.ErrConflict
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
	d := contentDevice(q)
	if err = verifyDevice(d, n.Config.DeviceRoot); err != nil {
		return empty, err
	}
	uuid, err := prepareFilesystem(ctx, q.Operation, d, q.Spec.Initialize, record.FormatIntent != "" && record.Receipt.UUID == "", func() (string, error) {
		r, e := n.Journal.content(q.Operation)
		expected := r.FormatIntent
		if expected == "" {
			expected = r.Receipt.UUID
		}
		if expected == "" {
			expected = q.Spec.ExpectedUUID
		}
		return expected, e
	}, func(uuid string) error {
		_, e := n.Journal.updateContent(q.Operation, func(r *contentRecord) error {
			if r.FormatIntent != "" || r.Receipt.UUID != "" || r.Receipt.Released {
				return core.ErrConflict
			}
			r.FormatIntent = uuid
			return nil
		})
		return e
	})
	if err != nil {
		return empty, err
	}
	if err = verifyDevice(d, n.Config.DeviceRoot); err != nil {
		return empty, err
	}
	if record.Receipt.UUID != "" && record.Receipt.UUID != uuid {
		return empty, core.ErrConflict
	}
	receipt := core.PreparationReceipt{Protocol: core.PreparationProtocol, Operation: q.Operation, PlanDigest: q.PlanDigest,
		BindingID: q.Endpoint.BindingID, VolumeUID: q.Endpoint.VolumeUID, NodeUID: q.Endpoint.NodeUID,
		BootID: q.Endpoint.NodeBootID, Epoch: q.Endpoint.Epoch, UUID: uuid, DeviceIdentity: q.Endpoint.Identity}
	_, err = n.Journal.updateContent(q.Operation, func(r *contentRecord) error { r.Receipt = receipt; return nil })
	return receipt, err
}
func (n *Node) InspectVolumePreparation(ctx context.Context, q runtime.PreparationRequest) (core.PreparationReceipt, error) {
	n.contentMu.Lock()
	defer n.contentMu.Unlock()
	r, err := n.Journal.content(q.Operation)
	if err == nil && core.Digest(r.Request) != core.Digest(q) {
		err = core.ErrConflict
	}
	return r.Receipt, err
}
func (n *Node) ReleasePreparation(ctx context.Context, q runtime.PreparationRequest) (core.PreparationReceipt, error) {
	// Acquiring the same lock waits for all synchronous formatter processes and
	// their CommandContext Wait/FD closure. This adapter creates no host mount.
	n.contentMu.Lock()
	defer n.contentMu.Unlock()
	validation := q
	rebooted := q.Endpoint.NodeBootID != "" && q.Endpoint.NodeBootID != n.BootID
	if rebooted {
		validation.Endpoint.NodeBootID = n.BootID
	}
	if err := n.validateContent(validation); err != nil {
		return core.PreparationReceipt{}, err
	}
	r, err := n.Journal.updateContent(q.Operation, func(r *contentRecord) error {
		if r.Request.Operation == "" {
			r.Request = q
		}
		if core.Digest(r.Request) != core.Digest(q) {
			return core.ErrConflict
		}
		if r.Receipt.Released {
			return nil
		}
		if !rebooted {
			if err := verifyPreparationIdle(contentDevice(q)); err != nil {
				return err
			}
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
func (n *Node) volumePreparationHook(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Action  string                     `json:"action"`
		Request runtime.PreparationRequest `json:"request"`
	}
	if err := decode(w, r, &q); err != nil {
		problem(w, 400, err)
		return
	}
	var receipt core.PreparationReceipt
	var err error
	switch q.Action {
	case "prepare":
		receipt, err = n.PrepareVolume(r.Context(), q.Request)
	case "inspect":
		receipt, err = n.InspectVolumePreparation(r.Context(), q.Request)
	case "release":
		receipt, err = n.ReleasePreparation(r.Context(), q.Request)
	default:
		err = errors.New("unknown volume preparation action")
	}
	if err != nil {
		problem(w, 409, err)
		return
	}
	respond(w, 200, receipt)
}
