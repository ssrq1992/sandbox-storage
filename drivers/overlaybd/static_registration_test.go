package overlaybd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	api "ssf.dev/ssf/api/overlaybd/v1"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
)

func registrationFixture(t *testing.T) (StaticRegistration, *v1.PersistentVolumeClaim, *storage.StorageClass) {
	t.Helper()
	id := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	q := StaticRegistration{Repository: "repo", NativeID: Identity("volume", id), ExpectedHead: core.Hash("head"), PVName: "static-pv"}
	mode := v1.PersistentVolumeBlock
	retain := v1.PersistentVolumeReclaimRetain
	sc := "overlaybd"
	profile := core.VolumeProfile{Name: Profile, Runtime: "agentenv", Version: core.PreparationProtocol, VolumeMode: "Block", Filesystem: "ext4", InitializeBlank: false}
	claim := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "test", UID: types.UID(id), Labels: map[string]string{kube.ManagedLabel: "true"}, Finalizers: []string{kube.VolumeProtectionFinalizer}, Annotations: map[string]string{kube.VolumeProfileAnnotation: Profile, kube.ProfileRevisionAnnotation: core.Digest(profile), kube.ProfileClassUIDAnnotation: "class-uid"}}, Spec: v1.PersistentVolumeClaimSpec{VolumeName: q.PVName, VolumeMode: &mode, StorageClassName: &sc, AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, Resources: v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Gi")}}}}
	class := &storage.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: sc, UID: "class-uid", Annotations: map[string]string{kube.VolumeProfileAnnotation: Profile}}, Provisioner: DriverName, Parameters: map[string]string{"repository": "repo"}, ReclaimPolicy: &retain}
	return q, claim, class
}
func TestStaticRegistrationRejectsUnprotectedOrChangedClaims(t *testing.T) {
	q, p, c := registrationFixture(t)
	if err := staticClaim(q, p, c); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*v1.PersistentVolumeClaim, *storage.StorageClass){
		"no-finalizer":     func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) { p.Finalizers = nil },
		"no-managed-label": func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) { p.Labels = nil },
		"class-recreated":  func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) { c.UID = "replacement" },
		"wrong-profile": func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) {
			p.Annotations[kube.ProfileRevisionAnnotation] = "changed"
		},
		"wrong-pv": func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) { p.Spec.VolumeName = "other" },
		"filesystem": func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) {
			m := v1.PersistentVolumeFilesystem
			p.Spec.VolumeMode = &m
		},
		"restore": func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) {
			p.Spec.DataSource = &v1.TypedLocalObjectReference{Kind: "PersistentVolumeClaim", Name: "source"}
		},
		"readonly-reclassification": func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) {
			p.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}
		},
		"delete-policy": func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) {
			r := v1.PersistentVolumeReclaimDelete
			c.ReclaimPolicy = &r
		},
		"deleting": func(p *v1.PersistentVolumeClaim, c *storage.StorageClass) {
			now := metav1.Now()
			p.DeletionTimestamp = &now
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			p, c := p.DeepCopy(), c.DeepCopy()
			change(p, c)
			if staticClaim(q, p, c) == nil {
				t.Fatal("unsafe claim accepted")
			}
		})
	}
}

type registrationBackend struct {
	api.NativeStorageBackendClient // Unexpected mutating RPCs panic: this tool must never create a volume.
	receipt                        *api.VolumeReceipt
	calls                          int
	old                            bool
}

func (b *registrationBackend) Describe(context.Context, *api.DescribeRequest, ...grpc.CallOption) (*api.BackendIdentity, error) {
	caps := []string{CapabilityNativeVolume}
	if !b.old {
		caps = append(caps, CapabilityStaticRegistration)
	}
	return &api.BackendIdentity{Protocol: BackendProtocol, InstanceId: "host", RepositoryIds: []string{"repo"}, Capabilities: caps}, nil
}
func (b *registrationBackend) ResolveVolume(_ context.Context, r *api.ResolveVolumeRequest, _ ...grpc.CallOption) (*api.VolumeReceipt, error) {
	b.calls++
	if !r.RequireIdle || !r.RequireWritable || r.RequireReadonly || r.HeadRevision != "" {
		return nil, ErrConflict
	}
	return proto.Clone(b.receipt).(*api.VolumeReceipt), nil
}
func TestPostgresStaticRegistrationPinsIdentityWithoutCreatingVolume(t *testing.T) {
	j := database(t)
	ctx := context.Background()
	q, p, c := registrationFixture(t)
	b := &registrationBackend{receipt: &api.VolumeReceipt{Operation: &api.OperationIdentity{OperationId: "resolve", InputDigest: core.Hash("intent"), RepositoryId: q.Repository, VolumeId: q.NativeID}, HeadRevision: q.ExpectedHead, ContentDigest: q.ExpectedHead, CapacityGeneration: 1, CapacityBytes: 1 << 30, FsUuid: "29aff887-2fe8-431a-a1a5-ad5766b7bf0a", State: "ContentReady", AccessibleNodes: []string{"node-a"}}}
	b.old = true
	if _, err := j.RegisterStatic(ctx, b, q, p, c); err == nil || b.calls != 0 {
		t.Fatal("old host accepted")
	}
	b.old = false
	b.receipt.HeadRevision = core.Hash("unexpected")
	if _, err := j.RegisterStatic(ctx, b, q, p, c); err == nil {
		t.Fatal("changed native head accepted")
	}
	b.receipt.HeadRevision = q.ExpectedHead
	pv, err := j.RegisterStatic(ctx, b, q, p, c)
	if err != nil {
		t.Fatal(err)
	}
	if pv.Spec.ClaimRef.UID != p.UID || pv.Spec.NodeAffinity == nil || pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
		t.Fatal("static PV lost identity/topology/retention")
	}
	calls := b.calls
	b.receipt.HeadRevision = core.Hash("later-written-head")
	replay, err := j.RegisterStatic(ctx, b, q, p, c)
	if err != nil || core.Digest(pv) != core.Digest(replay) || b.calls != calls {
		t.Fatal("replay reinterpreted volume", err)
	}
	replacement := p.DeepCopy()
	replacement.UID = "recreated"
	if _, err := j.RegisterStatic(ctx, b, q, replacement, c); err == nil {
		t.Fatal("PVC name rebound")
	}
	alias := q
	alias.PVName = "alias"
	other := p.DeepCopy()
	other.Spec.VolumeName = alias.PVName
	if _, err := j.RegisterStatic(ctx, b, alias, other, c); err == nil {
		t.Fatal("second registration for same backend accepted")
	}
	if _, err = j.Pool.Exec(ctx, `UPDATE ssf_overlaybd.volumes SET state='Deleted' WHERE handle=$1`, pv.Spec.CSI.VolumeHandle); err != nil {
		t.Fatal(err)
	}
	b.receipt.HeadRevision = q.ExpectedHead
	if _, err := j.RegisterStatic(ctx, b, q, p, c); err == nil {
		t.Fatal("deleted identity resurrected")
	}
}
