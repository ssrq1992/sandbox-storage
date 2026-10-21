package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

// NativeVolume is a named native /volumes provider descriptor, never a sandbox
// request wrapper. Names remain opaque to callers and may coexist with legacy.
type NativeVolume struct {
	DirectPVC           bool        `json:"directPVC,omitempty"`
	PVCSource           bool        `json:"pvcSource,omitempty"`
	Private             bool        `json:"private,omitempty"`
	InternalOwner       string      `json:"internalOwner,omitempty"`
	ReclaimPVName       string      `json:"reclaimPVName,omitempty"`
	ReclaimPVUID        string      `json:"reclaimPVUID,omitempty"`
	ExpansionRequested  int64       `json:"expansionRequested,omitempty"`
	ExpansionTarget     int64       `json:"expansionTarget,omitempty"`
	SourceSnapshotUID   string      `json:"sourceSnapshotUID,omitempty"`
	SourceContentUID    string      `json:"sourceContentUID,omitempty"`
	SourcePin           string      `json:"sourcePin,omitempty"`
	RestoreComplete     bool        `json:"restoreComplete,omitempty"`
	Provider            string      `json:"provider"`
	Name                string      `json:"name"`
	Tenant              string      `json:"tenant"`
	PVC                 core.PVCRef `json:"pvc"`
	StorageClass        string      `json:"storageClass"`
	SnapshotClass       string      `json:"snapshotClass,omitempty"`
	Bytes               int64       `json:"bytes"`
	ReadOnly            bool        `json:"readonly"`
	InitializeExt4      bool        `json:"initializeExt4"`
	InitializationOwner string      `json:"initializationOwner,omitempty"`
	IntentDigest        string      `json:"intentDigest"`
	State               string      `json:"state"`
}
type NativeVolumeRequest struct {
	internalPVCSource bool
	internalPrivate   bool
	internalOwner     string // assigned only by the native consumer, never decoded from API JSON

	SnapshotUID   string       `json:"snapshotUID,omitempty"`
	ContentUID    string       `json:"contentUID,omitempty"`
	Provider      string       `json:"provider,omitempty"`
	Name          string       `json:"name"`
	Namespace     string       `json:"namespace"`
	StorageClass  string       `json:"storageClass"`
	SnapshotClass string       `json:"snapshotClass,omitempty"`
	Bytes         int64        `json:"bytes"`
	ReadOnly      bool         `json:"readonly"`
	Import        *core.PVCRef `json:"import,omitempty"`
	Snapshot      string       `json:"snapshot,omitempty"`
}
type NativeVolumes struct {
	Ledger *ledger.Store
	Kube   *kube.Client
}

func (s *NativeVolumes) Get(ctx context.Context, tenant, name string) (NativeVolume, error) {
	var v NativeVolume
	var raw []byte
	err := s.Ledger.Session(ctx).QueryRow(ctx, `SELECT descriptor FROM agentenv_native_volumes WHERE tenant=$1 AND name=$2`, tenant, name).Scan(&raw)
	if err == nil {
		err = json.Unmarshal(raw, &v)
	}
	return v, err
}
func (s *NativeVolumes) save(ctx context.Context, v NativeVolume) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = s.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_volumes(tenant,name,descriptor) VALUES($1,$2,$3) ON CONFLICT(tenant,name) DO UPDATE SET descriptor=excluded.descriptor`, v.Tenant, v.Name, raw)
	return err
}
func (s *NativeVolumes) Create(ctx context.Context, p Principal, q NativeVolumeRequest) (NativeVolume, error) {
	var out NativeVolume
	if (q.Provider != "" && q.Provider != "ssf") || p.Tenant == "" || !safeID.MatchString(q.Name) || !p.Allows(q.Namespace) || q.Bytes <= 0 || q.StorageClass == "" {
		return out, errors.New("invalid native SSF volume request")
	}
	if q.ReadOnly && q.Import == nil && q.Snapshot == "" {
		return out, errors.New("read-only volume requires an initialized import or snapshot source")
	}
	if (q.SnapshotUID != "" || q.ContentUID != "") && (q.Snapshot == "" || q.SnapshotUID == "" || q.ContentUID == "") {
		return out, errors.New("snapshot identity requires name, Snapshot UID and Content UID")
	}
	if q.Import != nil && q.Snapshot != "" {
		return out, errors.New("only one volume source is allowed")
	}
	err := s.Ledger.Exclusive(ctx, "agentenv/volume/"+p.Tenant+"/"+q.Name, func(ctx context.Context) error {
		if err := s.claimName(ctx, p.Tenant, q.Name, "ssf"); err != nil {
			return err
		}
		v, err := s.Get(ctx, p.Tenant, q.Name)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil {
			if v.ExpansionRequested > 0 {
				return core.ErrPending
			}
			if v.PVCSource != q.internalPVCSource || v.Private != q.internalPrivate || v.InternalOwner != q.internalOwner || v.IntentDigest != core.Digest(q) || v.State == "Deleted" || v.State == "Reclaiming" || v.State == "Deleting" {
				return core.ErrConflict
			}
		} else {
			v = NativeVolume{Private: q.internalPrivate, InternalOwner: q.internalOwner, Provider: "ssf", Name: q.Name, Tenant: p.Tenant, PVC: core.PVCRef{Namespace: q.Namespace, Name: "aenv-" + core.Hash(p.Tenant + "/" + q.Name)[:40]}, StorageClass: q.StorageClass, SnapshotClass: q.SnapshotClass, Bytes: q.Bytes, ReadOnly: q.ReadOnly, InitializeExt4: q.Import == nil && q.Snapshot == "", IntentDigest: core.Digest(q), State: "Provisioning"}
			v.PVCSource = q.internalPVCSource
			if q.Import != nil {
				if q.Import.UID == "" || !p.Allows(q.Import.Namespace) || q.Import.Namespace != q.Namespace {
					return errors.New("import requires authorized namespace and exact PVC UID")
				}
				v.PVC = *q.Import
			}
			if err = s.save(ctx, v); err != nil {
				return err
			}
		}
		if q.Snapshot != "" && !v.RestoreComplete {
			source, err := s.Kube.SnapshotStatus(ctx, q.Namespace, q.Snapshot)
			if err != nil {
				return err
			}
			if q.SnapshotUID != "" && (source.UID != q.SnapshotUID || source.ContentUID != q.ContentUID) {
				return core.ErrConflict
			}
			if !source.Ready {
				return core.ErrPending
			}
			if source.SourceVolumeMode != string(v1.PersistentVolumeBlock) {
				return errors.New("snapshot restore requires a Block source")
			}
			if v.SourceSnapshotUID != "" && (v.SourceSnapshotUID != source.UID || v.SourceContentUID != source.ContentUID) {
				return core.ErrConflict
			}
			v.SourceSnapshotUID, v.SourceContentUID = source.UID, source.ContentUID
			v.SourcePin = "agentenv/restore/" + core.Hash(p.Tenant+"/"+q.Name+"/"+v.IntentDigest)
			if err = s.save(ctx, v); err != nil {
				return err
			}
			intent := map[string]string{"snapshot_uid": source.UID, "content_uid": source.ContentUID, "volume": v.PVC.Name}
			if err = s.Ledger.BeginCheckpoint(ctx, v.SourcePin, "restore/"+p.Tenant+"/"+q.Name, intent); err != nil {
				return err
			}
			if err = s.Ledger.PinCheckpoint(ctx, v.SourcePin, source.UID, source.ContentUID); err != nil {
				return err
			}
		}
		claim, err := s.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) && q.Import == nil {
			if v.PVC.UID != "" {
				return core.ErrConflict
			}
			mode := v1.PersistentVolumeBlock
			claim = &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: v.PVC.Namespace, Name: v.PVC.Name, Annotations: map[string]string{"agentenv.ssf.dev/intent": v.IntentDigest}}, Spec: v1.PersistentVolumeClaimSpec{StorageClassName: &q.StorageClass, VolumeMode: &mode, AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, Resources: v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(q.Bytes, resource.BinarySI)}}}}
			if q.ReadOnly {
				claim.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}
			}
			if q.Snapshot != "" {
				group := "snapshot.storage.k8s.io"
				claim.Spec.DataSource = &v1.TypedLocalObjectReference{APIGroup: &group, Kind: "VolumeSnapshot", Name: q.Snapshot}
			}
			claim, err = s.Kube.CreatePVC(ctx, claim)
			if apierrors.IsAlreadyExists(err) {
				claim, err = s.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
			}
		}
		if err != nil {
			return err
		}
		if claim.DeletionTimestamp != nil || claim.Labels[kube.ManagedLabel] != "true" || claim.Spec.VolumeMode == nil || *claim.Spec.VolumeMode != v1.PersistentVolumeBlock || claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != q.StorageClass {
			return errors.New("PVC must be protected Block storage with the requested class")
		}
		if v.PVC.UID != "" && v.PVC.UID != string(claim.UID) {
			return core.ErrConflict
		}
		if q.Import == nil && claim.Annotations["agentenv.ssf.dev/intent"] != v.IntentDigest {
			return core.ErrConflict
		}
		if q.Import != nil {
			// An already-running native Pod predates Guard admission. Never import its
			// device into an independent VM while that native consumer still exists.
			pods, err := s.Kube.API.CoreV1().Pods(v.PVC.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return err
			}
			for _, pod := range pods.Items {
				if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
					continue
				}
				for _, mount := range pod.Spec.Volumes {
					if mount.PersistentVolumeClaim != nil && mount.PersistentVolumeClaim.ClaimName == v.PVC.Name {
						return errors.New("imported PVC has a native Pod consumer")
					}
				}
			}
		}
		requested := claim.Spec.Resources.Requests[v1.ResourceStorage]
		if requested.Value() < q.Bytes {
			return errors.New("existing PVC smaller than requested native volume")
		}
		v.Bytes = max(v.Bytes, requested.Value(), claim.Status.Capacity.Storage().Value())
		v.PVC.UID = string(claim.UID)
		v.Bytes = max(v.Bytes, claim.Spec.Resources.Requests.Storage().Value(), claim.Status.Capacity.Storage().Value())
		v.State = string(claim.Status.Phase)
		if v.State == "" {
			v.State = "Pending"
		}
		if claim.Status.Phase == v1.ClaimBound && v.SourcePin != "" {
			v.RestoreComplete = true
		}
		if err = s.save(ctx, v); err != nil {
			return err
		}
		if v.RestoreComplete && v.SourcePin != "" {
			if err = s.Ledger.ReleaseCheckpoint(ctx, v.SourcePin); err != nil {
				return err
			}
			v.SourcePin = ""
			if err = s.save(ctx, v); err != nil {
				return err
			}
		}
		out = v
		return nil
	})
	return out, err
}
func (s *NativeVolumes) AuthorizeInitialization(ctx context.Context, tenant, name, operation string) (bool, error) {
	var authorized bool
	err := s.Ledger.Exclusive(ctx, "agentenv/volume/"+tenant+"/"+name, func(ctx context.Context) error {
		v, err := s.Get(ctx, tenant, name)
		if err != nil {
			return err
		}
		if !v.InitializeExt4 {
			return nil
		}
		if v.InitializationOwner != "" && v.InitializationOwner != operation {
			// A successor may retry a fresh blank claim only after the old
			// operation is conclusively released. Node still verifies ext4 or
			// scans the entire device for zeros before any new mkfs intent.
			prior, e := s.Ledger.Operation(ctx, v.InitializationOwner)
			if e != nil {
				return e
			}
			if prior.State != "Released" {
				return nil
			}
		}
		if operation == "" {
			return errors.New("initialization operation required")
		}
		v.InitializationOwner = operation
		if err = s.save(ctx, v); err != nil {
			return err
		}
		authorized = true
		return nil
	})
	return authorized, err
}
func (s *NativeVolumes) List(ctx context.Context, tenant string) ([]NativeVolume, error) {
	rows, err := s.Ledger.Session(ctx).Query(ctx, `SELECT descriptor FROM agentenv_native_volumes WHERE tenant=$1 ORDER BY name`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NativeVolume{}
	for rows.Next() {
		var raw []byte
		var v NativeVolume
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		if v.State != "Deleted" && !v.DirectPVC && !v.Private {
			out = append(out, v)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for i := range out {
		out[i], err = s.Observe(ctx, tenant, out[i].Name)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Lookup distinguishes an authoritative absence from a failed provider. The
// native mux may select legacy only after Found=false, never after an error.
type NativeVolumeLookup struct {
	Found  bool              `json:"found"`
	Volume *NativeVolumeView `json:"volume,omitempty"`
}

func (s *NativeVolumes) Lookup(ctx context.Context, tenant, name string) (NativeVolumeLookup, error) {
	v, err := s.Observe(ctx, tenant, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return NativeVolumeLookup{}, nil
	}
	if err != nil {
		return NativeVolumeLookup{}, err
	}
	if v.DirectPVC {
		return NativeVolumeLookup{}, errors.New("direct PVC references are not volume aliases")
	}
	view := v.View()
	return NativeVolumeLookup{Found: true, Volume: &view}, nil
}

func (s *NativeVolumes) claimName(ctx context.Context, tenant, name, provider string) error {
	if _, err := s.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_volume_names(tenant,name,provider) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, tenant, name, provider); err != nil {
		return err
	}
	var actual string
	if err := s.Ledger.Session(ctx).QueryRow(ctx, `SELECT provider FROM agentenv_native_volume_names WHERE tenant=$1 AND name=$2`, tenant, name).Scan(&actual); err != nil {
		return err
	}
	if actual != provider {
		return core.ErrConflict
	}
	return nil
}
func (s *NativeVolumes) ClaimLegacy(ctx context.Context, tenant, name string) error {
	if tenant == "" || !safeID.MatchString(name) {
		return errors.New("invalid legacy volume name")
	}
	return s.Ledger.Exclusive(ctx, "agentenv/volume/"+tenant+"/"+name, func(ctx context.Context) error { return s.claimName(ctx, tenant, name, "legacy") })
}

type NativeVolumeView struct {
	VolumeID     string          `json:"volumeID"`
	Name         string          `json:"name"`
	Mode         string          `json:"mode"`
	SizeMB       uint64          `json:"sizeMB"`
	Status       string          `json:"status"`
	Provider     string          `json:"provider"`
	Capabilities map[string]bool `json:"capabilities"`
}

func (v NativeVolume) View() NativeVolumeView {
	mode := "exclusive"
	if v.ReadOnly {
		mode = "ro"
	}
	status := "pending"
	switch v.State {
	case "Bound":
		status = "ready"
	case "Deleted":
		status = "deleted"
	case "Lost":
		status = "failed"
	}
	if v.ExpansionRequested > 0 {
		status = "pending"
	}
	sizeMB := uint64(v.Bytes / (1 << 20))
	if v.Bytes%(1<<20) != 0 {
		sizeMB++
	}
	return NativeVolumeView{VolumeID: v.Name, Name: v.Name, Mode: mode, SizeMB: sizeMB, Status: status, Provider: v.Provider, Capabilities: map[string]bool{"blockDevice": true, "readOnly": true, "offlineExpand": !v.ReadOnly, "snapshot": false, "clone": false}}
}

// Observe refreshes native provisioning status and releases restore-source pins
// only once Kubernetes confirms the independently provisioned target is Bound.
func (s *NativeVolumes) Observe(ctx context.Context, tenant, name string) (NativeVolume, error) {
	var v NativeVolume
	err := s.Ledger.Exclusive(ctx, "agentenv/volume/"+tenant+"/"+name, func(ctx context.Context) error {
		var err error
		v, err = s.Get(ctx, tenant, name)
		if err != nil {
			return err
		}
		if v.State == "Deleted" || v.State == "Reclaiming" || v.State == "Deleting" || v.ExpansionRequested > 0 || v.State == "Expanding" || v.PVC.UID == "" {
			return nil
		}
		claim, err := s.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if v.PVC.UID == "" || string(claim.UID) != v.PVC.UID {
			return core.ErrConflict
		}
		v.Bytes = max(v.Bytes, claim.Spec.Resources.Requests.Storage().Value(), claim.Status.Capacity.Storage().Value())
		v.State = string(claim.Status.Phase)
		if v.State == "" {
			v.State = "Pending"
		}
		if claim.Status.Phase == v1.ClaimBound && v.SourcePin != "" {
			v.RestoreComplete = true
		}
		if err = s.save(ctx, v); err != nil {
			return err
		}
		if v.RestoreComplete && v.SourcePin != "" {
			if err = s.Ledger.ReleaseCheckpoint(ctx, v.SourcePin); err != nil {
				return err
			}
			v.SourcePin = ""
			return s.save(ctx, v)
		}
		return nil
	})
	return v, err
}

// Delete removes the named provider volume through the native Kubernetes API.
// Retain is deliberate: the public delete cannot silently destroy the backing
// PV. Physical reclaim remains an explicit SSF administrator operation.
func (s *NativeVolumes) Delete(ctx context.Context, tenant, name string) error {
	return s.Ledger.Exclusive(ctx, "agentenv/volume/"+tenant+"/"+name, func(ctx context.Context) error {
		v, err := s.Get(ctx, tenant, name)
		if err != nil {
			return err
		}
		if v.DirectPVC {
			return errors.New("delete directly referenced PVC through Kubernetes")
		}
		if v.State == "Deleted" {
			return nil
		}
		if v.ExpansionRequested > 0 || v.State == "Reclaiming" {
			return core.ErrPending
		}
		if v.PVC.UID == "" {
			return errors.New("volume provisioning identity is unresolved; retry creation before deletion")
		}
		// Save intent before any external side effect; recovery continues this even
		// when the API caller disconnects. New consumers reject Deleting descriptors.
		v.State = "Deleting"
		if err = s.save(ctx, v); err != nil {
			return err
		}
		if err = s.Ledger.DrainIdentity(ctx, v.PVC); err != nil {
			return err
		}
		if err = s.Ledger.Permit(ctx, v.PVC.UID, "Retain", 5*time.Minute); err != nil {
			return err
		}
		claim, err := s.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
		if err == nil {
			if string(claim.UID) != v.PVC.UID {
				return core.ErrConflict
			}
			if claim.Labels[kube.ManagedLabel] != "true" {
				return errors.New("volume protection missing")
			}
			if claim.DeletionTimestamp == nil {
				if err = s.Kube.DeletePVC(ctx, v.PVC); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
			// A successful DELETE is only acceptance, not disappearance. Keep restore
			// source references while the PVC may still be provisioning or finalizing.
			return core.ErrPending
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
		if v.SourcePin != "" {
			if err = s.Ledger.ReleaseCheckpoint(ctx, v.SourcePin); err != nil {
				return err
			}
			v.SourcePin = ""
		}
		v.State = "Deleted"
		return s.save(ctx, v)
	})
}
