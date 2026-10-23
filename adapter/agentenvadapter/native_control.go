package agentenvadapter

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
	"ssf.dev/ssf/preparation"
	"ssf.dev/ssf/runtime"
)

type NativeMount struct {
	Name string           `json:"name,omitempty"`
	PVC  *NativePVCSource `json:"pvc,omitempty"`
	Path string           `json:"path"`
}
type NativePVCSource struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}
type NativeTemplate struct {
	ID           string        `json:"id"`
	Resources    Resources     `json:"resources"`
	Mounts       []NativeMount `json:"volumeMounts"`
	CheckpointID string        `json:"checkpoint_id,omitempty"`
}
type NativeIntent struct {
	ComputeCheckpoint string `json:"compute_checkpoint,omitempty"`
	ForkOf            string `json:"fork_of,omitempty"`
	ForkCheckpoint    string `json:"fork_checkpoint,omitempty"`
	ExpectedNativeID  string `json:"expected_native_id,omitempty"`
	ResumeOf          string `json:"resume_of,omitempty"`
	RequestID         string `json:"request_id"`
	Tenant            string `json:"tenant"`
	Path              string `json:"path"`
	Body              []byte `json:"body"`
	BodyDigest        string `json:"body_digest"`
	OwnerUID          string `json:"owner_uid,omitempty"`
	FixedNode         string `json:"fixed_node,omitempty"`
}
type NativeRelocation struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

const NativeRetainedPrefix = "/.agentenv-storage-retained/"

type NativeDispatch struct {
	ReplaceMounts       bool               `json:"replace_mounts,omitempty"`
	Relocations         []NativeRelocation `json:"relocations,omitempty"`
	Expand              map[string]int64   `json:"expand,omitempty"`
	ExpectedNativeID    string             `json:"expected_native_id,omitempty"`
	CheckpointSource    string             `json:"checkpoint_source,omitempty"`
	CheckpointReference string             `json:"checkpoint_reference,omitempty"`
	VolumeNames         map[string]string  `json:"volume_names,omitempty"`
	MountIDs            map[string]string  `json:"mount_ids,omitempty"`
	RequestID           string             `json:"request_id"`
	OwnerUID            string             `json:"owner_uid"`
	RunUID              string             `json:"run_uid"`
	BodyDigest          string             `json:"body_digest"`
	TemplatePin         string             `json:"template_pin,omitempty"`
	Resources           Resources          `json:"resources"`
	Candidate           NativeCandidate    `json:"candidate"`
	ManagedMounts       []NativeMount      `json:"managed_mounts"`
	Initialize          map[string]bool    `json:"initialize"`
	Result              consume.Result     `json:"result"`
	Reservation         core.Placement     `json:"reservation"`
}

type NativeControl struct {
	CaptureNode      func(context.Context, core.Placement) (api.RuntimeStorageNodeClient, error)
	DefaultNamespace string
	Preparation      *preparation.Coordinator
	Ledger           *ledger.Store
	Kube             *kube.Client
	Drivers          consume.Controllers
	SigningKey       ed25519.PrivateKey
	Volumes          *NativeVolumes
	Principal        Principal
	HTTP             *HTTPClient
	ProjectTemplate  func(context.Context, string, []NativeCandidate) (NativeTemplate, error)
	Refresh          func(context.Context, NativeCandidate) (NativeCandidate, error)
	RefreshStopped   func(context.Context, NativeCandidate) (NativeCandidate, error)
	RecoverStopped   func(context.Context, NativeCandidate) (NativeCandidate, error)
}

func (c *NativeControl) coordinator(a *NativeAdmission, endpoint string) *consume.Coordinator {
	co := &consume.Coordinator{Ledger: c.Ledger, Kube: c.Kube, Drivers: c.Drivers, Placement: a, SigningKey: c.SigningKey, Authorizer: namespaceAuth{c.Principal}, PlanTTL: 5 * time.Minute}
	if c.Preparation != nil {
		co.EnsureVolumeReady = c.Preparation.Ensure
	}
	if endpoint != "" && c.HTTP != nil {
		co.Evidence = &runtime.RemoteEvidence{URL: base(endpoint) + "/v1/evidence", Client: c.HTTP.Client}
	}
	return co
}
func (c *NativeControl) Prepare(ctx context.Context, q NativeIntent, candidates []NativeCandidate, choose func([]NativeCandidate) (NativeCandidate, error)) (NativeDispatch, error) {
	return c.prepareLaunch(ctx, q, candidates, choose, false)
}
func (c *NativeControl) prepareLaunch(ctx context.Context, q NativeIntent, candidates []NativeCandidate, choose func([]NativeCandidate) (NativeCandidate, error), resolveOnly bool) (NativeDispatch, error) {
	var out NativeDispatch
	if q.RequestID == "" || q.Tenant != c.Principal.Tenant || q.BodyDigest != core.Hash(string(q.Body)) {
		return out, errors.New("invalid authenticated native intent")
	}
	// NativeID is assigned by the original Orchestrator. Owner/run are storage
	// identities and must never replace the public native sandbox ID.
	err := c.Ledger.Exclusive(ctx, "agentenv/native-run/"+q.RequestID, func(ctx context.Context) error {
		var digest, owner, run, state string
		var raw []byte
		err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT digest,owner_uid,run_uid,state,dispatch FROM agentenv_native_runs WHERE id=$1`, q.RequestID).Scan(&digest, &owner, &run, &state, &raw)
		if errors.Is(err, pgx.ErrNoRows) {
			owner = q.OwnerUID
			if owner == "" {
				owner = newID()
			}
			run = newID()
			intent, e := json.Marshal(q)
			if e != nil {
				return e
			}
			_, err = c.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_runs(id,digest,owner_uid,run_uid,intent) VALUES($1,$2,$3,$4,$5)`, q.RequestID, core.Digest(q), owner, run, intent)
		} else if err == nil {
			if digest != core.Digest(q) {
				return core.ErrConflict
			}
			switch state {
			case "Dispatched", "Ready":
				return json.Unmarshal(raw, &out)
			case "Unstarted":
				// Recovery proved that this attempt never obtained placement,
				// so no node could receive its run. Retry with a fresh run ID.
				run = newID()
				_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET run_uid=$2,state='Intent',dispatch='{}',updated=now() WHERE id=$1`, q.RequestID, run)
			case "Intent":
				// Only an unfinished preparation may advance to dispatch.
			default:
				return core.ErrConflict
			}
		}
		if err != nil {
			return err
		}
		var resolved nativeResolved
		err = c.Ledger.Step(ctx, "agentenv/resolved/"+q.RequestID, core.Digest(q), func(ctx context.Context) (any, error) { return c.resolveNative(ctx, q, owner, run, candidates) }, &resolved)
		if err != nil {
			if errors.Is(err, core.ErrConflict) {
				err = errors.Join(err, c.compensateUndispatched(ctx, q, owner, run))
			}
			return err
		}
		out = resolved.Dispatch
		out.OwnerUID, out.RunUID = owner, run
		coreMounts, volumes := resolved.Mounts, resolved.Volumes
		if resolveOnly {
			return nil
		}
		if q.FixedNode != "" {
			filtered := []NativeCandidate{}
			for _, n := range candidates {
				if n.ID == q.FixedNode {
					filtered = append(filtered, n)
				}
			}
			candidates = filtered
		}
		admission := &NativeAdmission{Ledger: c.Ledger, Kube: c.Kube, Candidates: candidates, Resources: out.Resources, Choose: choose, Refresh: c.Refresh}
		// A permanent conflict must not leave an undischarged operation, an
		// active run row or an acquired binding behind: this run was never
		// dispatched, so no node can have started it and immediate
		// compensation is always safe. Transient errors keep their retry
		// semantics through the recovery loop.
		conflict := func(err error) error {
			if !errors.Is(err, core.ErrConflict) {
				return err
			}
			return errors.Join(err, c.compensateUndispatched(ctx, q, owner, run))
		}
		if len(coreMounts) > 0 {
			out.Result, err = c.coordinator(admission, "").Prepare(ctx, consume.Request{ID: q.RequestID, Tenant: q.Tenant, Sandbox: run, OwnerUID: owner, RunUID: run, Mounts: coreMounts})
			if err != nil {
				return conflict(err)
			}
			out.Reservation = out.Result.Placement
			for _, b := range out.Result.Plan.Bindings {
				var boundVolume NativeVolume
				for _, volume := range volumes {
					if volume.PVC == b.Volume.PVC {
						boundVolume = volume
						break
					}
				}
				if boundVolume.Name == "" {
					return conflict(core.ErrConflict)
				}
				var initialize bool
				var e error
				if _, prepared := out.Result.Plan.PreparedVolumes[b.ID]; !prepared {
					initialize, e = c.Volumes.AuthorizeInitialization(ctx, q.Tenant, boundVolume.Name, out.Result.Operation)
				}
				if e != nil {
					return conflict(e)
				}
				out.Initialize[b.ID] = initialize
				current, e := c.Volumes.Get(ctx, q.Tenant, boundVolume.Name)
				if e != nil {
					return conflict(e)
				}
				if current.PVC != boundVolume.PVC || current.State == "Expanding" {
					return core.ErrPending
				}
				if current.ExpansionTarget > 0 {
					out.Expand[b.ID] = current.ExpansionTarget
				}
			}
		} else {
			out.Reservation, err = admission.Reserve(ctx, "native/"+q.RequestID, nil)
			if err != nil {
				return err
			}
		}
		reservation, err := admission.load(ctx, out.Reservation.ID)
		if err != nil {
			return err
		}
		out.Candidate = reservation.Candidate
		out.Resources = reservation.Resources
		raw, err = json.Marshal(out)
		if err != nil {
			return err
		}
		if len(raw) > NativeMaxPlanBytes {
			// Persist a no-dispatch tombstone before compensation. Unlike a
			// failed HTTP request, this branch provably never returned a plan.
			_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Unpublished',dispatch=$2,updated=now() WHERE id=$1`, q.RequestID, raw)
			if err != nil {
				return err
			}
			if err = c.abortNativeUnpublished(ctx, q.RequestID); err != nil {
				return err
			}
			return errors.New("native storage plan exceeds negotiated size limit")
		}
		// Returning a dispatch crosses an uncertain transport boundary. Never
		// classify it as unstarted merely because Gateway disconnected or timed out.
		_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Dispatched',dispatch=$2,updated=now() WHERE id=$1`, q.RequestID, raw)
		return err
	})
	return out, err
}

// compensateUndispatched reclaims a run whose preparation ended in a permanent
// conflict before any dispatch was persisted. The caller must hold the
// native-run lock and the run must still be in its pre-dispatch state, so no
// node can have started this run and immediate compensation is safe. It
// mirrors abortNativeUnpublished for the conflict path: bindings acquired by
// the operation are released, the run is stopped, the placement reservation
// is returned and the run is revoked (or reset for a resume intent).
func (c *NativeControl) compensateUndispatched(ctx context.Context, q NativeIntent, owner, run string) error {
	op := core.Hash(q.Tenant + "\x00" + run + "\x00" + q.RequestID)
	if err := c.coordinator(nil, "").AbortUnpublished(ctx, op, func(ctx context.Context) error {
		var state string
		if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, q.RequestID).Scan(&state); err != nil {
			return err
		}
		if state != "Intent" {
			return core.ErrConflict
		}
		return nil
	}); err != nil {
		return err
	}
	if err := c.Ledger.StopRun(ctx, owner, run); err != nil {
		return err
	}
	admission := NativeAdmission{Ledger: c.Ledger}
	if reservation, err := admission.load(ctx, op); err == nil {
		if err = admission.Release(ctx, reservation.Placement); err != nil {
			return err
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	next := "Unstarted"
	if q.ResumeOf == "" {
		if err := c.Ledger.ReleaseUse(ctx, owner); err != nil {
			return err
		}
		next = "Revoked"
	}
	_, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state=$2,updated=now() WHERE id=$1 AND state='Intent'`, q.RequestID, next)
	return err
}

// Immutable resolution is persisted separately from compute admission. Both RPC
// phases and local lifecycle admission use exactly this implementation.
type nativeResolved struct {
	Dispatch NativeDispatch
	Mounts   []core.Mount
	Volumes  []NativeVolume
}

func (c *NativeControl) resolveNative(ctx context.Context, q NativeIntent, owner, run string, candidates []NativeCandidate) (nativeResolved, error) {
	var resolved nativeResolved
	var out NativeDispatch
	var err error
	resolve := func() error {
		var body struct {
			CPU      *int64          `json:"cpuCount"`
			Memory   *int64          `json:"memoryMB"`
			Template string          `json:"templateID"`
			Mounts   json.RawMessage `json:"volumeMounts"`
		}
		if err = json.Unmarshal(q.Body, &body); err != nil {
			return err
		}
		var requestedMounts []NativeMount
		if body.Mounts != nil && q.ResumeOf == "" && q.ForkOf == "" && q.ComputeCheckpoint == "" {
			if string(body.Mounts) == "null" {
				return errors.New("volumeMounts must be an array")
			}
			if err = json.Unmarshal(body.Mounts, &requestedMounts); err != nil {
				return err
			}
			paths, names := map[string]bool{}, map[string]bool{}
			for index := range requestedMounts {
				mount := requestedMounts[index]
				if (mount.Name == "") == (mount.PVC == nil) {
					return errors.New("volumeMounts requires exactly one of name and pvc")
				}
				if mount.PVC != nil {
					mount, err = c.resolvePVCMount(ctx, q.RequestID, index, mount)
					if err != nil {
						return err
					}
					requestedMounts[index] = mount
				} else if strings.HasPrefix(mount.Name, "pvc/") {
					return errors.New("internal PVC identity cannot be used as a volume name")
				}
				if mount.Name == "" || names[mount.Name] || paths[mount.Path] || !path.IsAbs(mount.Path) || path.Clean(mount.Path) != mount.Path || mount.Path == "/" {
					return errors.New("invalid native mount")
				}
				if strings.HasPrefix(mount.Path, NativeRetainedPrefix) || mount.Path == strings.TrimSuffix(NativeRetainedPrefix, "/") {
					return errors.New("mount path reserved for retained native filesystems")
				}
				paths[mount.Path], names[mount.Name] = true, true
			}
		}
		resources := Resources{}
		mounts := []NativeMount{}
		pin := ""
		checkpointReference := ""
		checkpointSource := ""
		replaceMounts := (q.Path == "/sandboxes" || q.Path == "/v2/sandboxes") && q.ResumeOf == "" && body.Mounts != nil
		relocations := []NativeRelocation{}
		volumeNames := map[string]string{}
		mountIDs := map[string]string{}
		bindCheckpoint := func(source, kind string) error {
			retained, e := c.RetainNativeCheckpoint(ctx, source, kind, q.RequestID)
			if e != nil {
				return e
			}
			clones, e := c.cloneNativeCheckpoint(ctx, retained, q.RequestID, replaceMounts)
			if e != nil {
				return e
			}
			checkpointSource, checkpointReference = source, retained
			mounts = []NativeMount{}
			for _, clone := range clones {
				mount := clone.Mount
				if replaceMounts && !strings.HasPrefix(mount.Path, NativeRetainedPrefix) {
					hidden := NativeRetainedPrefix + core.Hash(q.RequestID)[:16] + "/external-" + core.Hash(mount.Path)[:16]
					relocations = append(relocations, NativeRelocation{Source: mount.Path, Target: hidden})
					mount.Path = hidden
				}
				mount.Name = clone.Volume.Name
				if clone.Volume.PVCSource {
					mount.PVC = &NativePVCSource{Namespace: clone.Volume.PVC.Namespace, Name: clone.Volume.PVC.Name}
				}
				mounts = append(mounts, mount)
				volumeNames[mount.Name] = clone.Volume.Name
				mountIDs[mount.Name] = clone.MountID
			}
			return nil
		}
		if q.ForkOf != "" {
			if q.Path != "/internal/fork" || q.ResumeOf != "" || !safeID.MatchString(q.ExpectedNativeID) {
				return core.ErrConflict
			}
			parent, e := c.Dispatch(ctx, q.ForkOf)
			if e != nil {
				return e
			}
			var parentState string
			if e = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, q.ForkOf).Scan(&parentState); e != nil {
				return e
			}
			if parentState != "Ready" || parent.Candidate.ID != q.FixedNode || owner == parent.OwnerUID {
				return core.ErrConflict
			}
			resources, pin = parent.Resources, parent.TemplatePin
			if len(parent.ManagedMounts) > 0 {
				checkpoint, e := c.checkpointManifest(ctx, q.ForkCheckpoint)
				if e != nil {
					return e
				}
				if checkpoint.Intent.RunUID != parent.RunUID || checkpoint.Intent.OwnerUID != parent.OwnerUID {
					return core.ErrConflict
				}
				if err = bindCheckpoint(q.ForkCheckpoint, "fork"); err != nil {
					return err
				}
			} else if q.ForkCheckpoint != "" {
				return core.ErrConflict
			}
		} else if q.ResumeOf != "" {
			prior, e := c.Dispatch(ctx, q.ResumeOf)
			if e != nil {
				return e
			}
			if prior.OwnerUID != owner {
				return core.ErrConflict
			}
			resources = prior.Resources
			pin = prior.TemplatePin
			mounts = prior.ManagedMounts
			checkpointReference = prior.CheckpointReference
			checkpointSource = prior.CheckpointSource
			volumeNames = prior.VolumeNames
			mountIDs = prior.MountIDs
		} else {
			switch q.Path {
			case "/sandboxes-cold", "/internal/compute":
				if body.CPU != nil {
					if *body.CPU <= 0 {
						return errors.New("cpuCount must be positive")
					}
					resources.CPU = *body.CPU
				}
				if body.Memory != nil {
					if *body.Memory < 128 {
						return errors.New("memoryMB must be at least 128")
					}
					resources.MemoryMiB = *body.Memory
				}
			case "/sandboxes", "/v2/sandboxes":
				if c.ProjectTemplate == nil {
					return errors.New("native template projection unavailable")
				}
				var template NativeTemplate
				err = c.Ledger.Step(ctx, "agentenv/template/"+q.RequestID, core.Digest(body.Template), func(ctx context.Context) (any, error) { return c.ProjectTemplate(ctx, body.Template, candidates) }, &template)
				if err != nil {
					return err
				}
				pin = template.ID
				resources = template.Resources
				mounts = template.Mounts
				if template.CheckpointID != "" {
					checkpointSource = template.CheckpointID
					if err = bindCheckpoint(template.CheckpointID, "restore"); err != nil {
						return err
					}
				}
			default:
				return errors.New("unknown native launch kind")
			}
		}
		if q.ComputeCheckpoint != "" {
			if q.Path != "/internal/compute" || q.ResumeOf != "" || q.ForkOf != "" {
				return core.ErrConflict
			}
			if err = bindCheckpoint(q.ComputeCheckpoint, "restore"); err != nil {
				return err
			}
		}
		if body.Mounts != nil && q.ResumeOf == "" && q.ForkOf == "" && q.ComputeCheckpoint == "" {
			retainedMounts := mounts
			// Parsing precedes any clone side effect. Omission inherits; [] replaces.
			mounts = append([]NativeMount{}, requestedMounts...)
			for _, mount := range mounts {
				if strings.HasPrefix(mount.Path, NativeRetainedPrefix) {
					return errors.New("mount path reserved for native retained filesystems")
				}
				if replaceMounts {
					mountIDs[mount.Name] = "ext_new_" + core.Hash(q.RequestID + "/" + mount.Name)[:16]
				}
			}
			if replaceMounts {
				mounts = append(retainedMounts, mounts...)
			}
		}
		if resources.CPU < 0 || (resources.MemoryMiB != 0 && resources.MemoryMiB < 128) || ((q.Path != "/sandboxes-cold" || q.ResumeOf != "") && (resources.CPU == 0 || resources.MemoryMiB == 0)) {
			return errors.New("invalid native VM resources")
		}
		managed := []NativeMount{}
		volumes := []NativeVolume{}
		coreMounts := []core.Mount{}
		paths := map[string]bool{}
		names := map[string]bool{}
		pvcUIDs := map[string]bool{}
		for _, mount := range mounts {
			if mount.Name == "" || paths[mount.Path] || names[mount.Name] {
				return errors.New("duplicate or empty native mount")
			}
			paths[mount.Path] = true
			names[mount.Name] = true
			volumeName := mount.Name
			if mapped := volumeNames[mount.Name]; mapped != "" {
				volumeName = mapped
			}
			v, e := c.Volumes.Get(ctx, q.Tenant, volumeName)
			if errors.Is(e, pgx.ErrNoRows) && volumeNames[mount.Name] == "" {
				continue
			} // positively absent registry entry is legacy
			if e != nil {
				return e
			}
			if v.PVCSource && c.Preparation == nil {
				return errors.New("PVC source requires volume preparation capability")
			}
			if pvcUIDs[v.PVC.UID] {
				return errors.New("same PVC referenced more than once")
			}
			pvcUIDs[v.PVC.UID] = true
			if v.State == "Deleted" || v.State == "Deleting" || v.State == "Reclaiming" || v.State == "Expanding" {
				return core.ErrConflict
			}
			managed = append(managed, mount)
			volumes = append(volumes, v)
			mountID := mountIDs[mount.Name]
			if mountID == "" {
				mountID = "ext_" + core.Hash(mount.Name)[:16]
			}
			coreMounts = append(coreMounts, core.Mount{PVC: v.PVC, MountID: mountID, GuestPath: mount.Path, ReadOnly: v.ReadOnly})
		}
		if q.FixedNode != "" {
			filtered := []NativeCandidate{}
			for _, n := range candidates {
				if n.ID == q.FixedNode {
					filtered = append(filtered, n)
				}
			}
			candidates = filtered
		}
		out = NativeDispatch{ReplaceMounts: replaceMounts, Relocations: relocations, ExpectedNativeID: q.ExpectedNativeID, CheckpointSource: checkpointSource, CheckpointReference: checkpointReference, VolumeNames: volumeNames, MountIDs: mountIDs, RequestID: q.RequestID, OwnerUID: owner, RunUID: run, BodyDigest: q.BodyDigest, TemplatePin: pin, Resources: resources, ManagedMounts: managed, Initialize: map[string]bool{}, Expand: map[string]int64{}}

		resolved = nativeResolved{Dispatch: out, Mounts: coreMounts, Volumes: volumes}
		return nil

	}
	err = resolve()
	return resolved, err
}

func (c *NativeControl) Dispatch(ctx context.Context, requestID string) (NativeDispatch, error) {
	var out NativeDispatch
	var raw []byte
	err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT dispatch FROM agentenv_native_runs WHERE id=$1`, requestID).Scan(&raw)
	if err == nil {
		err = json.Unmarshal(raw, &out)
	}
	return out, err
}

// Ready is called from the authenticated native node plugin after Node Ready.
// It never derives readiness from the public 201 response or a heartbeat.
func (c *NativeControl) Ready(ctx context.Context, requestID, nodeID, nativeID, proof string) error {
	return c.Ledger.Exclusive(ctx, "agentenv/native-run/"+requestID, func(ctx context.Context) error {
		d, err := c.Dispatch(ctx, requestID)
		if err != nil {
			return err
		}
		if d.Candidate.ID != nodeID || nativeID == "" || (d.ExpectedNativeID != "" && d.ExpectedNativeID != nativeID) {
			return core.ErrConflict
		}
		var state string
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, requestID).Scan(&state); err != nil {
			return err
		}
		if state != "Dispatched" && state != "Ready" {
			return core.ErrConflict
		}
		var record NodeRecord
		if err = c.HTTP.Call(ctx, "GET", base(d.Candidate.PluginURL)+"/v1/operations/"+d.RequestID, nil, &record); err != nil {
			return err
		}
		if record.State != "Ready" || record.Request.Instance != d.RunUID || record.Runtime.SandboxID != nativeID {
			return errors.New("trusted Node Ready receipt does not match native run")
		}
		a := &NativeAdmission{Ledger: c.Ledger, Refresh: c.Refresh}
		if err = a.BindNativeID(ctx, d.Reservation.ID, nativeID); err != nil {
			return err
		}
		if len(d.ManagedMounts) > 0 {
			if record.Request.Operation != d.Result.Operation {
				return core.ErrConflict
			}
			co := c.coordinator(a, d.Candidate.PluginURL)
			if err = co.ConfirmReady(ctx, d.Result, d.RunUID, proof); err != nil {
				return err
			}
			if err = co.Commit(ctx, d.Result); err != nil {
				return err
			}
		} else {
			evidence := &runtime.RemoteEvidence{URL: base(d.Candidate.PluginURL) + "/v1/evidence", Client: c.HTTP.Client}
			if err = evidence.Verify(ctx, d.RunUID, "ready", proof); err != nil {
				return err
			}
			if err = a.Commit(ctx, d.Reservation, d.RunUID); err != nil {
				return err
			}
		}
		tag, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Ready',ever_ready=true,native_id=$2,updated=now() WHERE id=$1 AND state IN ('Dispatched','Ready') AND (native_id='' OR native_id=$2)`, requestID, nativeID)
		if err == nil && tag.RowsAffected() != 1 {
			return fmt.Errorf("native ready association: %w", core.ErrConflict)
		}
		if err != nil {
			return err
		}
		return c.settleNativeTemplateConsumer(ctx, requestID)

	})
}

func (c *NativeControl) RetainPause(ctx context.Context, requestID, nodeID string) error {
	return c.Ledger.Exclusive(ctx, "agentenv/native-run/"+requestID, func(ctx context.Context) error {
		d, err := c.Dispatch(ctx, requestID)
		if err != nil {
			return err
		}
		if d.Candidate.ID != nodeID {
			return core.ErrConflict
		}
		var state string
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, requestID).Scan(&state); err != nil {
			return err
		}
		if state != "Ready" && state != "Pausing" {
			return core.ErrConflict
		}
		if len(d.ManagedMounts) > 0 {
			if err = c.Ledger.RetainUse(ctx, d.Result.Operation, "paused"); err != nil {
				return err
			}
		}
		_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Pausing',updated=now() WHERE id=$1`, requestID)
		return err
	})
}

func (c *NativeControl) Stopped(ctx context.Context, requestID, nodeID, proof string) error {
	return c.Ledger.Exclusive(ctx, "agentenv/native-run/"+requestID, func(ctx context.Context) error {
		d, err := c.Dispatch(ctx, requestID)
		if err != nil {
			return err
		}
		candidate, err := c.stopCandidate(ctx, d.Candidate, nodeID)
		if err != nil {
			return err
		}
		var state string
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, requestID).Scan(&state); err != nil {
			return err
		}
		if state == "Released" || state == "Paused" || state == "Resumed" || state == "FailedResume" {
			return c.settleNativeTemplateConsumer(ctx, requestID)
		}
		a := &NativeAdmission{Ledger: c.Ledger, Refresh: c.Refresh}
		co := c.coordinator(a, candidate.PluginURL)
		if len(d.ManagedMounts) > 0 {
			if err = co.ConfirmStopped(ctx, d.Result, proof); err != nil {
				return err
			}
			if err = co.Release(ctx, d.Result.Operation, func(ctx context.Context, _ string) error {
				var r NodeRecord
				if err := c.HTTP.Call(ctx, "POST", base(candidate.PluginURL)+"/v1/operations/"+d.RequestID+"/release", struct{}{}, &r); err != nil {
					return err
				}
				if r.State != "Released" {
					return core.ErrPending
				}
				return nil
			}); err != nil {
				return err
			}
		} else {
			evidence := &runtime.RemoteEvidence{URL: base(candidate.PluginURL) + "/v1/evidence", Client: c.HTTP.Client}
			if err = evidence.Verify(ctx, d.RunUID, "detached", proof); err != nil {
				return err
			}
		}
		if err = a.Release(ctx, d.Reservation); err != nil {
			return err
		}
		var intentRaw []byte
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT intent FROM agentenv_native_runs WHERE id=$1`, requestID).Scan(&intentRaw); err != nil {
			return err
		}
		var intent NativeIntent
		if err = json.Unmarshal(intentRaw, &intent); err != nil {
			return err
		}
		next := "Released"
		if intent.ResumeOf != "" && state != "Ready" && state != "Pausing" && state != "Deleting" {
			// The old paused memory must not be replayed after an uncertain
			// resumed guest may have written its disks. Keep the logical hold.
			next = "FailedResume"
		} else if state == "Pausing" {
			next = "Paused"
		} else if err = c.Ledger.ReleaseUse(ctx, d.OwnerUID); err != nil {
			return err
		}
		_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state=$2,updated=now() WHERE id=$1`, requestID, next)
		if err != nil {
			return err
		}
		return c.settleNativeTemplateConsumer(ctx, requestID)
	})
}

func (c *NativeControl) Resume(ctx context.Context, oldRequest, nodeID, nativeID string, candidates []NativeCandidate, choose func([]NativeCandidate) (NativeCandidate, error)) (NativeDispatch, error) {
	var out NativeDispatch
	err := c.Ledger.Exclusive(ctx, "agentenv/native-run/"+oldRequest, func(ctx context.Context) error {
		var q NativeIntent
		var raw []byte
		var state, owner, originalNative string
		err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT intent,state,owner_uid,native_id FROM agentenv_native_runs WHERE id=$1`, oldRequest).Scan(&raw, &state, &owner, &originalNative)
		if err != nil {
			return err
		}
		old, err := c.Dispatch(ctx, oldRequest)
		if err != nil {
			return err
		}
		if (state != "Paused" && state != "Resumed") || originalNative != nativeID {
			return core.ErrConflict
		}
		if old.Candidate.ID != nodeID {
			if _, err = c.physicalCandidate(ctx, old.Candidate, nodeID); err != nil {
				return err
			}
		}
		if err = json.Unmarshal(raw, &q); err != nil {
			return err
		}
		q.RequestID = core.Hash(oldRequest + "/resume")
		q.OwnerUID = owner
		q.FixedNode = nodeID
		q.ResumeOf = oldRequest
		q.ForkOf = ""
		q.ComputeCheckpoint = ""
		q.ForkCheckpoint = ""
		q.ExpectedNativeID = nativeID
		out, err = c.Prepare(ctx, q, candidates, choose)
		if err != nil {
			return err
		}
		_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Resumed',updated=now() WHERE id=$1`, oldRequest)
		return err

	})
	return out, err
}

func (c *NativeControl) CancelPause(ctx context.Context, requestID, nodeID string) error {
	return c.Ledger.Exclusive(ctx, "agentenv/native-run/"+requestID, func(ctx context.Context) error {
		d, err := c.Dispatch(ctx, requestID)
		if err != nil {
			return err
		}
		if d.Candidate.ID != nodeID {
			return core.ErrConflict
		}
		result, err := c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Ready',updated=now() WHERE id=$1 AND state IN ('Pausing','Ready')`, requestID)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return core.ErrConflict
		}
		// Keep the hold until final deletion, including a cancelled pause. The same
		// owner may continue using it; another writer must never inherit it.
		return nil
	})
}

// BeginDelete is durable intent, never proof of VMM exit. The original runtime
// still performs stop; only Stopped can release an active run's devices.
func (c *NativeControl) BeginDelete(ctx context.Context, requestID, nodeID string) error {
	return c.Ledger.Exclusive(ctx, "agentenv/native-run/"+requestID, func(ctx context.Context) error {
		d, err := c.Dispatch(ctx, requestID)
		if err != nil {
			return err
		}
		if _, err = c.physicalCandidate(ctx, d.Candidate, nodeID); err != nil {
			return err
		}
		var state string
		if err = c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state FROM agentenv_native_runs WHERE id=$1`, requestID).Scan(&state); err != nil {
			return err
		}
		switch state {
		case "Resumed":
			// Resume uses a deterministic successor; deletion through retained
			// paused metadata follows it without releasing the predecessor hold.
			return c.BeginDelete(ctx, core.Hash(requestID+"/resume"), nodeID)
		case "Released", "Deleting":
			return nil
		case "Paused", "FailedResume":
			// These states are written only after verified stop and run release.
			if err = c.Ledger.ReleaseUse(ctx, d.OwnerUID); err != nil {
				return err
			}
			_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Released',updated=now() WHERE id=$1`, requestID)
			return err
		case "Ready", "Dispatched", "Pausing", "Cancelling":
			_, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_runs SET state='Deleting',updated=now() WHERE id=$1`, requestID)
			return err
		default:
			return core.ErrConflict
		}
	})
}

// A replacement Pod may finish a stopped run only on the same physical K8s
// node once the previous Pod's runtimes provably exited. Never rewrite the
// original signed run plan.
func (c *NativeControl) stopCandidate(ctx context.Context, previous NativeCandidate, nodeID string) (NativeCandidate, error) {
	current, err := c.physicalCandidate(ctx, previous, nodeID)
	if err != nil {
		return current, err
	}
	if nodeID != previous.ID && current.BootID == previous.BootID {
		// Without a host reboot the old Pod's runtimes are only provably gone
		// once the previous Pod object itself no longer exists: its processes
		// were terminated at the latest by the Pod deletion grace period, and
		// the reporting node plugin reports such a stop only after verifying
		// the old PID namespace has no live member process. While the previous
		// Pod object still exists its runtimes may keep running.
		if c.Kube == nil || c.Kube.API == nil || c.DefaultNamespace == "" {
			return NativeCandidate{}, core.ErrConflict
		}
		if _, e := c.Kube.API.CoreV1().Pods(c.DefaultNamespace).Get(ctx, previous.ID, metav1.GetOptions{}); e == nil {
			return NativeCandidate{}, core.ErrConflict
		} else if !apierrors.IsNotFound(e) {
			return NativeCandidate{}, e
		}
	}
	return current, nil
}

// Already stopped paused runs may move to a replacement Pod on the same host
// without requiring another host reboot. This check alone is never stop proof.
func (c *NativeControl) physicalCandidate(ctx context.Context, previous NativeCandidate, nodeID string) (NativeCandidate, error) {
	refresh := c.RefreshStopped
	if refresh == nil {
		refresh = c.Refresh
	}
	if refresh == nil {
		if previous.ID != nodeID {
			return NativeCandidate{}, core.ErrConflict
		}
		return previous, nil
	}
	current, err := refresh(ctx, NativeCandidate{ID: nodeID})
	if err != nil {
		return NativeCandidate{}, err
	}
	if current.ID != nodeID || previous.NodeUID == "" || current.NodeUID != previous.NodeUID || current.KubeNode != previous.KubeNode || current.BootID == "" {
		return NativeCandidate{}, core.ErrConflict
	}
	return current, nil
}

type namespaceAuth struct{ Principal Principal }

func (a namespaceAuth) Authorize(_ context.Context, tenant string, ref core.PVCRef, _ bool) error {
	if tenant != a.Principal.Tenant || !a.Principal.Allows(ref.Namespace) {
		return errors.New("PVC not authorized")
	}
	return nil
}
