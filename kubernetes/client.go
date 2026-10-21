// Package kubernetes translates management requests into native resources.
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"ssf.dev/ssf/core"
	"strconv"
	"time"
)

const ManagedLabel = "ssf.dev/managed"

// ErrTopology distinguishes an unsuitable candidate from an API failure.
var ErrTopology = errors.New("node outside PV topology")

type Client struct {
	API       kubernetes.Interface
	Dynamic   dynamic.Interface
	Selection interface {
		ReserveCandidate(context.Context, string, string, string) error
		Candidate(context.Context, string) (string, error)
	}
}

func (c *Client) Resolve(ctx context.Context, ref core.PVCRef, ro bool) (core.Volume, error) {
	var out core.Volume
	p, e := c.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if e != nil {
		return out, e
	}
	if ref.UID != "" && string(p.UID) != ref.UID {
		return out, core.ErrConflict
	}
	if p.Labels[ManagedLabel] != "true" || p.DeletionTimestamp != nil {
		return out, errors.New("PVC not protected or deleting")
	}
	if p.Status.Phase != v1.ClaimBound || p.Spec.VolumeName == "" {
		return out, core.ErrPending
	}
	v, e := c.API.CoreV1().PersistentVolumes().Get(ctx, p.Spec.VolumeName, metav1.GetOptions{})
	if e != nil {
		return out, e
	}
	if v.Spec.CSI == nil || v.Spec.ClaimRef == nil || v.Spec.ClaimRef.UID != p.UID || v.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain || v.DeletionTimestamp != nil {
		return out, errors.New("PV CSI identity, ownership or Retain protection invalid")
	}
	if v.Labels[ManagedLabel] != "true" {
		if v.Labels == nil {
			v.Labels = map[string]string{}
		}
		v.Labels[ManagedLabel] = "true"
		v, e = c.API.CoreV1().PersistentVolumes().Update(ctx, v, metav1.UpdateOptions{})
		if e != nil {
			return out, e
		}
	}
	mode := csi.VolumeCapability_AccessMode_UNKNOWN
	for _, m := range p.Spec.AccessModes {
		switch m {
		case v1.ReadWriteOnce:
			if mode != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
				mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
			}
		case v1.ReadWriteOncePod:
			mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER
		case v1.ReadOnlyMany:
			if mode == csi.VolumeCapability_AccessMode_UNKNOWN {
				mode = csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
			}
		case v1.ReadWriteMany:
			mode = csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
		}
	}
	if mode == csi.VolumeCapability_AccessMode_UNKNOWN || (!ro && mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY) {
		return out, errors.New("unsupported access mode")
	}
	cap := &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode}}
	if p.Spec.VolumeMode != nil && *p.Spec.VolumeMode == v1.PersistentVolumeBlock {
		cap.AccessType = &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}
	} else {
		cap.AccessType = &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: v.Spec.CSI.FSType, MountFlags: v.Spec.MountOptions}}
	}
	secrets := map[string]core.SecretRef{}
	for purpose, r := range map[string]*v1.SecretReference{"controller-publish": v.Spec.CSI.ControllerPublishSecretRef, "node-stage": v.Spec.CSI.NodeStageSecretRef, "node-publish": v.Spec.CSI.NodePublishSecretRef, "node-expand": v.Spec.CSI.NodeExpandSecretRef} {
		if r != nil {
			secrets[purpose] = core.SecretRef{Namespace: r.Namespace, Name: r.Name}
		}
	}
	out = core.Volume{UID: string(p.UID), PVC: core.PVCRef{Namespace: p.Namespace, Name: p.Name, UID: string(p.UID)}, PVName: v.Name, PVUID: string(v.UID), Driver: v.Spec.CSI.Driver, Handle: v.Spec.CSI.VolumeHandle, Capability: cap, Context: v.Spec.CSI.VolumeAttributes, ReadOnly: ro || v.Spec.CSI.ReadOnly, Secrets: secrets, Generation: 1}
	return out, nil
}
func (c *Client) Secrets(ctx context.Context, v core.Volume, purpose string) (map[string]string, error) {
	ref, ok := v.Secrets[purpose]
	if !ok {
		return nil, nil
	}
	s, e := c.API.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if e != nil {
		return nil, e
	}
	out := map[string]string{}
	for k, v := range s.Data {
		out[k] = string(v)
	}
	return out, nil
}
func (c *Client) CreatePVC(ctx context.Context, p *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	p = p.DeepCopy()
	if p.Labels == nil {
		p.Labels = map[string]string{}
	}
	p.Labels[ManagedLabel] = "true"
	if p.Spec.StorageClassName == nil {
		return nil, errors.New("explicit protected StorageClass required")
	}
	sc, e := c.API.StorageV1().StorageClasses().Get(ctx, *p.Spec.StorageClassName, metav1.GetOptions{})
	if e != nil {
		return nil, e
	}
	if sc.ReclaimPolicy == nil || *sc.ReclaimPolicy != v1.PersistentVolumeReclaimRetain {
		return nil, errors.New("StorageClass must use Retain")
	}
	return c.API.CoreV1().PersistentVolumeClaims(p.Namespace).Create(ctx, p, metav1.CreateOptions{})
}
func (c *Client) Expand(ctx context.Context, ref core.PVCRef, size string) error {
	q, e := resource.ParseQuantity(size)
	if e != nil {
		return e
	}
	p, e := c.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if e != nil {
		return e
	}
	if string(p.UID) != ref.UID {
		return core.ErrConflict
	}
	old := p.Spec.Resources.Requests[v1.ResourceStorage]
	if q.Cmp(old) < 0 {
		return errors.New("volume shrinking is forbidden")
	}
	p.Spec.Resources.Requests[v1.ResourceStorage] = q
	_, e = c.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Update(ctx, p, metav1.UpdateOptions{})
	return e
}
func (c *Client) Clone(ctx context.Context, source core.PVCRef, target *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	p, e := c.API.CoreV1().PersistentVolumeClaims(source.Namespace).Get(ctx, source.Name, metav1.GetOptions{})
	if e != nil {
		return nil, e
	}
	if string(p.UID) != source.UID || source.Namespace != target.Namespace {
		return nil, core.ErrConflict
	}
	target = target.DeepCopy()
	group := ""
	target.Spec.DataSource = &v1.TypedLocalObjectReference{APIGroup: &group, Kind: "PersistentVolumeClaim", Name: source.Name}
	return c.CreatePVC(ctx, target)
}

// EnsureBound persists selected-node using resourceVersion and never reselects
// an in-flight claim. The caller retains its placement attempt until resolved.
func (c *Client) EnsureBound(ctx context.Context, ref core.PVCRef, node string, readonly bool, validate func(context.Context) error) (core.Volume, error) {
	p, e := c.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if e != nil {
		return core.Volume{}, e
	}
	if ref.UID != "" && string(p.UID) != ref.UID {
		return core.Volume{}, core.ErrConflict
	}
	ref.UID = string(p.UID)
	if p.Spec.StorageClassName == nil {
		return core.Volume{}, errors.New("explicit StorageClass required")
	}
	sc, e := c.API.StorageV1().StorageClasses().Get(ctx, *p.Spec.StorageClassName, metav1.GetOptions{})
	if e != nil {
		return core.Volume{}, e
	}
	if e = validate(ctx); e != nil {
		return core.Volume{}, e
	}
	if p.Status.Phase != v1.ClaimBound && sc.VolumeBindingMode != nil && string(*sc.VolumeBindingMode) == "WaitForFirstConsumer" {
		static, err := c.SelectStatic(ctx, ref, node)
		if err != nil {
			return core.Volume{}, err
		}
		if !static && sc.Provisioner == "kubernetes.io/no-provisioner" {
			return core.Volume{}, core.ErrPending
		}
		if !static {
			const selected = "volume.kubernetes.io/selected-node"
			if old := p.Annotations[selected]; old != "" && old != node {
				return core.Volume{}, core.ErrConflict
			}
			if p.Annotations == nil {
				p.Annotations = map[string]string{}
			}
			p.Annotations[selected] = node
			if _, e = c.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Update(ctx, p, metav1.UpdateOptions{}); e != nil {
				return core.Volume{}, e
			}
		}
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		v, e := c.Resolve(ctx, ref, readonly)
		if e == nil {
			if e = c.CheckTopology(ctx, v, node); e != nil {
				return v, e
			}
			return v, validate(ctx)
		}
		if !errors.Is(e, core.ErrPending) {
			return v, e
		}
		select {
		case <-ctx.Done():
			return v, ctx.Err()
		case <-ticker.C:
		}
	}
}
func (c *Client) ReserveStatic(ctx context.Context, ref core.PVCRef, pv, node string) error {
	p, e := c.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if e != nil {
		return e
	}
	if string(p.UID) != ref.UID {
		return core.ErrConflict
	}
	v, e := c.API.CoreV1().PersistentVolumes().Get(ctx, pv, metav1.GetOptions{})
	if e != nil {
		return e
	}
	if v.Spec.CSI == nil {
		return errors.New("static volume must be CSI")
	}
	if v.DeletionTimestamp != nil {
		return core.ErrConflict
	}
	if v.Spec.ClaimRef != nil && v.Spec.ClaimRef.UID != p.UID {
		return core.ErrConflict
	}
	if p.Spec.StorageClassName == nil || v.Spec.StorageClassName != *p.Spec.StorageClassName {
		return core.ErrConflict
	}
	if p.Spec.Selector != nil {
		selector, e := metav1.LabelSelectorAsSelector(p.Spec.Selector)
		if e != nil {
			return e
		}
		if !selector.Matches(labels.Set(v.Labels)) {
			return errors.New("static PV selector mismatch")
		}
	}
	want := p.Spec.Resources.Requests[v1.ResourceStorage]
	have := v.Spec.Capacity[v1.ResourceStorage]
	if have.Cmp(want) < 0 {
		return errors.New("insufficient volume capacity")
	}
	for _, m := range p.Spec.AccessModes {
		found := false
		for _, vm := range v.Spec.AccessModes {
			found = found || m == vm
		}
		if !found {
			return errors.New("static access mode mismatch")
		}
	}
	pm, vm := v1.PersistentVolumeFilesystem, v1.PersistentVolumeFilesystem
	if p.Spec.VolumeMode != nil {
		pm = *p.Spec.VolumeMode
	}
	if v.Spec.VolumeMode != nil {
		vm = *v.Spec.VolumeMode
	}
	if pm != vm {
		return errors.New("static volume mode mismatch")
	}
	if e = c.checkAffinity(ctx, v, node); e != nil {
		return e
	}
	if c.Selection != nil {
		if e = c.Selection.ReserveCandidate(ctx, ref.UID, string(v.UID), node); e != nil {
			return e
		}
	}
	v.Spec.ClaimRef = &v1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: p.Namespace, Name: p.Name, UID: p.UID}
	_, e = c.API.CoreV1().PersistentVolumes().Update(ctx, v, metav1.UpdateOptions{})
	return e
}
func (c *Client) CheckTopology(ctx context.Context, v core.Volume, node string) error {
	pv, e := c.API.CoreV1().PersistentVolumes().Get(ctx, v.PVName, metav1.GetOptions{})
	if e != nil {
		return e
	}
	if string(pv.UID) != v.PVUID {
		return core.ErrConflict
	}
	return c.checkAffinity(ctx, pv, node)
}
func (c *Client) checkAffinity(ctx context.Context, v *v1.PersistentVolume, node string) error {
	n, e := c.API.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if e != nil {
		return e
	}
	if v.Spec.NodeAffinity == nil || v.Spec.NodeAffinity.Required == nil {
		return nil
	}
	for _, term := range v.Spec.NodeAffinity.Required.NodeSelectorTerms {
		ok := len(term.MatchExpressions)+len(term.MatchFields) > 0
		for _, r := range term.MatchExpressions {
			ok = ok && match(n.Labels, r)
		}
		for _, r := range term.MatchFields {
			ok = ok && match(map[string]string{"metadata.name": n.Name}, r)
		}
		if ok {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrTopology, node)
}
func match(values map[string]string, r v1.NodeSelectorRequirement) bool {
	x, exists := values[r.Key]
	in := false
	for _, v := range r.Values {
		in = in || v == x
	}
	switch r.Operator {
	case v1.NodeSelectorOpIn:
		return exists && in
	case v1.NodeSelectorOpNotIn:
		return !in
	case v1.NodeSelectorOpExists:
		return exists
	case v1.NodeSelectorOpDoesNotExist:
		return !exists
	case v1.NodeSelectorOpGt, v1.NodeSelectorOpLt:
		if !exists || len(r.Values) != 1 {
			return false
		}
		a, e1 := strconv.ParseInt(x, 10, 64)
		b, e2 := strconv.ParseInt(r.Values[0], 10, 64)
		if e1 != nil || e2 != nil {
			return false
		}
		if r.Operator == v1.NodeSelectorOpGt {
			return a > b
		}
		return a < b
	default:
		return false
	}
}
func (c *Client) DeletePVC(ctx context.Context, ref core.PVCRef) error {
	if ref.UID == "" {
		return errors.New("UID precondition required")
	}
	uid := types.UID(ref.UID)
	return c.API.CoreV1().PersistentVolumeClaims(ref.Namespace).Delete(ctx, ref.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
}
