package overlaybd

import (
	"context"
	"errors"
	"math"
	"slices"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
)

type StaticRegistration struct {
	Repository, NativeID, ExpectedHead, PVName string
	Readonly                                   bool
}

func staticClaim(q StaticRegistration, claim *v1.PersistentVolumeClaim, class *storage.StorageClass) error {
	if _, err := VolumeHandle(q.Repository, q.NativeID); err != nil {
		return err
	}
	if !validDigest(q.ExpectedHead) || q.PVName == "" || claim == nil || class == nil || claim.UID == "" || claim.Namespace == "" || claim.DeletionTimestamp != nil || class.UID == "" || class.DeletionTimestamp != nil {
		return ErrConflict
	}
	profile := core.VolumeProfile{Name: Profile, Runtime: "agentenv", Version: core.PreparationProtocol, VolumeMode: "Block", Filesystem: "ext4", InitializeBlank: false}
	if claim.Spec.VolumeMode == nil || *claim.Spec.VolumeMode != v1.PersistentVolumeBlock || claim.Spec.VolumeName != q.PVName || claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != class.Name || claim.Spec.DataSource != nil || claim.Spec.DataSourceRef != nil {
		return ErrConflict
	}
	if class.Provisioner != DriverName || class.Parameters["repository"] != q.Repository || len(class.Parameters) != 1 || class.ReclaimPolicy == nil || *class.ReclaimPolicy != v1.PersistentVolumeReclaimRetain {
		return ErrConflict
	}
	if claim.Labels[kube.ManagedLabel] != "true" || !slices.Contains(claim.Finalizers, kube.VolumeProtectionFinalizer) || class.Annotations[kube.VolumeProfileAnnotation] != Profile || claim.Annotations[kube.VolumeProfileAnnotation] != Profile || claim.Annotations[kube.ProfileRevisionAnnotation] != core.Digest(profile) || claim.Annotations[kube.ProfileClassUIDAnnotation] != string(class.UID) {
		return ErrConflict
	}
	if len(claim.Spec.AccessModes) != 1 {
		return ErrConflict
	}
	mode := claim.Spec.AccessModes[0]
	if mode != v1.ReadWriteOnce && mode != v1.ReadWriteOncePod && mode != v1.ReadOnlyMany {
		return ErrConflict
	}
	if (mode == v1.ReadOnlyMany) != q.Readonly {
		return ErrConflict
	}
	size := claim.Spec.Resources.Requests[v1.ResourceStorage]
	if size.Sign() <= 0 {
		return ErrConflict
	}
	return nil
}

// RegisterStatic observes an administrator-selected volume in an isolated native
// repository. No Ensure/Create, format, attach or device RPC is issued. A native
// idle inspection is required on first registration; identical catalog replays
// do not reinterpret a head after consumers have started writing.
func (j *Journal) RegisterStatic(ctx context.Context, backend api.NativeStorageBackendClient, q StaticRegistration, claim *v1.PersistentVolumeClaim, class *storage.StorageClass) (*v1.PersistentVolume, error) {
	if err := staticClaim(q, claim, class); err != nil {
		return nil, err
	}
	handle, err := VolumeHandle(q.Repository, q.NativeID)
	if err != nil {
		return nil, err
	}
	request := "static/" + q.PVName + "/" + string(claim.UID)
	volume, err := j.Volume(ctx, handle)
	if err == nil {
		if volume.RequestName != request || volume.PVCUID != string(claim.UID) || volume.Readonly != q.Readonly || volume.InitialHead != q.ExpectedHead || volume.ProfileRevision != Profile {
			return nil, ErrConflict
		}
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	} else {
		if backend == nil {
			return nil, errors.New("verified native backend required")
		}
		identity, err := backend.Describe(ctx, &api.DescribeRequest{})
		if err != nil {
			return nil, err
		}
		if err = ValidateBackend(identity, CapabilityNativeVolume, CapabilityStaticRegistration); err != nil {
			return nil, err
		}
		if !slices.Contains(identity.RepositoryIds, q.Repository) {
			return nil, ErrConflict
		}
		receipt, err := backend.ResolveVolume(ctx, &api.ResolveVolumeRequest{RepositoryId: q.Repository, VolumeId: q.NativeID, RequireIdle: true, RequireWritable: !q.Readonly, RequireReadonly: q.Readonly})
		if err != nil {
			return nil, err
		}
		if receipt == nil || !validOperation(receipt.Operation) || receipt.Operation.RepositoryId != q.Repository || receipt.Operation.VolumeId != q.NativeID || receipt.HeadRevision != q.ExpectedHead || receipt.ContentDigest != q.ExpectedHead || receipt.CapacityGeneration == 0 || receipt.CapacityBytes == 0 || receipt.CapacityBytes > math.MaxInt64 || receipt.CapacityBytes%512 != 0 || !validFilesystemUUID(receipt.FsUuid) || receipt.State != "ContentReady" {
			return nil, ErrConflict
		}
		if len(receipt.AccessibleNodes) > 1 || (len(receipt.AccessibleNodes) == 1 && receipt.AccessibleNodes[0] == "") {
			return nil, ErrConflict
		}
		requested := claim.Spec.Resources.Requests[v1.ResourceStorage]
		if requested.Cmp(*resource.NewQuantity(int64(receipt.CapacityBytes), resource.BinarySI)) > 0 {
			return nil, errors.New("native capacity smaller than PVC request")
		}
		volume, err = j.RegisterVolume(ctx, request, string(claim.UID), Profile, q.Readonly, receipt)
		if err != nil {
			return nil, err
		}
	}
	if volume.CapacityBytes > math.MaxInt64 {
		return nil, ErrConflict
	}
	requested := claim.Spec.Resources.Requests[v1.ResourceStorage]
	if requested.Cmp(*resource.NewQuantity(int64(volume.CapacityBytes), resource.BinarySI)) > 0 {
		return nil, ErrConflict
	}
	mode := v1.PersistentVolumeBlock
	pv := &v1.PersistentVolume{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"}, ObjectMeta: metav1.ObjectMeta{Name: q.PVName, Labels: map[string]string{kube.ManagedLabel: "true"}}, Spec: v1.PersistentVolumeSpec{
		Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(int64(volume.CapacityBytes), resource.BinarySI)}, VolumeMode: &mode, AccessModes: append([]v1.PersistentVolumeAccessMode(nil), claim.Spec.AccessModes...), StorageClassName: class.Name, PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimRetain,
		ClaimRef:               &v1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: claim.Namespace, Name: claim.Name, UID: claim.UID},
		PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: DriverName, VolumeHandle: handle, ReadOnly: q.Readonly, FSType: "ext4", VolumeAttributes: map[string]string{"ssf.dev/volume-profile": Profile, "overlaybd.ssf.dev/repository": q.Repository}}},
	}}
	if len(volume.AccessibleNodes) > 0 {
		pv.Spec.NodeAffinity = &v1.VolumeNodeAffinity{Required: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{{Key: TopologyNodeKey, Operator: v1.NodeSelectorOpIn, Values: volume.AccessibleNodes}}}}}}
	}
	return pv, nil
}
