// Package guard protects the independent consumer against native resource deletion.
package guard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	admission "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"net/http"
	"reflect"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
)

type Permits interface {
	Allowed(context.Context, string, string) (bool, error)
	AllowedSelection(context.Context, string, string, string) (bool, error)
}
type Handler struct {
	Mutating                            bool
	VolumeProfiles                      []core.VolumeProfile
	ManagedNamespaces, CoordinatorUsers []string
	Ledger                              Permits
	API                                 kubernetes.Interface
	Dynamic                             dynamic.Interface
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var review admission.AdmissionReview
	b, e := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if e != nil || json.Unmarshal(b, &review) != nil || review.Request == nil {
		http.Error(w, "invalid AdmissionReview", 400)
		return
	}
	var allowed bool
	var reason string
	var patch []byte
	if h.Mutating {
		var err error
		patch, err = h.mutate(r.Context(), review.Request)
		allowed = err == nil
		reason = ""
		if err != nil {
			reason = err.Error()
		}
	} else {
		allowed, reason = h.check(r.Context(), review.Request)
	}
	review.Response = &admission.AdmissionResponse{UID: review.Request.UID, Allowed: allowed, Result: &metav1.Status{Message: reason}}
	if len(patch) > 0 {
		kind := admission.PatchTypeJSONPatch
		review.Response.PatchType = &kind
		review.Response.Patch = patch
	}
	review.Request = nil
	_ = json.NewEncoder(w).Encode(review)
}
func (h *Handler) permit(ctx context.Context, uid, action string) (bool, string) {
	if uid == "" {
		return false, "UID required"
	}
	ok, e := h.Ledger.Allowed(ctx, uid, action)
	if e != nil {
		return false, "consumption authority unavailable"
	}
	if !ok {
		return false, "drain/reclaim permit required"
	}
	return true, ""
}
func (h *Handler) check(ctx context.Context, r *admission.AdmissionRequest) (bool, string) {
	switch r.Kind.Kind {
	case "VolumeSnapshot", "VolumeSnapshotContent":
		return h.checkSnapshot(ctx, r)
	case "Pod":
		var p v1.Pod
		if json.Unmarshal(r.Object.Raw, &p) != nil {
			return false, "invalid Pod"
		}
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}
			claim, e := h.API.CoreV1().PersistentVolumeClaims(r.Namespace).Get(ctx, v.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
			if apierrors.IsNotFound(e) {
				continue
			}
			if e != nil {
				return false, "PVC lookup failed"
			}
			if claim.Labels[kube.ManagedLabel] == "true" {
				return false, "SSF-managed volume cannot be consumed by native Pod"
			}
			_, profiled, err := h.profile(ctx, claim)
			if err != nil {
				return false, "PVC profile lookup failed"
			}
			if profiled {
				return false, "profiled sandbox volume cannot be consumed by native Pod"
			}
		}
		return true, ""
	case "PersistentVolumeClaim":
		var old, next v1.PersistentVolumeClaim
		_ = json.Unmarshal(r.OldObject.Raw, &old)
		_ = json.Unmarshal(r.Object.Raw, &next)
		if r.Operation != admission.Delete {
			if ok, reason := h.checkPreparation(ctx, r, &old, &next); !ok {
				return false, reason
			}
		}
		managed := old.Labels[kube.ManagedLabel] == "true" || next.Labels[kube.ManagedLabel] == "true"
		if !managed {
			return true, ""
		}
		if r.Operation == admission.Delete {
			if ok, _ := h.checkPreparation(ctx, r, &old, &next); ok {
				return true, ""
			}
			if ok, _ := h.permit(ctx, string(old.UID), "Delete"); ok {
				return true, ""
			}
			return h.permit(ctx, string(old.UID), "Retain")
		}
		if r.Operation == admission.Update {
			if next.Spec.Resources.Requests.Storage().Cmp(*old.Spec.Resources.Requests.Storage()) != 0 {
				authority, ok := h.Ledger.(interface {
					AllowedExpansion(context.Context, string, int64) (bool, error)
				})
				if !ok {
					return false, "offline expansion reservation required"
				}
				allowed, err := authority.AllowedExpansion(ctx, string(old.UID), next.Spec.Resources.Requests.Storage().Value())
				if err != nil || !allowed {
					return false, "offline expansion reservation required"
				}
			}
			const selected = "volume.kubernetes.io/selected-node"
			if old.Annotations[selected] != next.Annotations[selected] {
				if next.Annotations[selected] == "" {
					return false, "selected node cannot be cleared while attempt outcome is unresolved"
				}
				ok, e := h.Ledger.AllowedSelection(ctx, string(old.UID), next.Annotations[selected], "")
				if e != nil || !ok {
					return false, "authorized placement attempt required"
				}
			}
			if next.Labels[kube.ManagedLabel] != "true" {
				return false, "cannot remove consumption protection"
			}
			if old.Spec.VolumeName != "" && next.Spec.VolumeName != old.Spec.VolumeName {
				return false, "bound volume identity immutable"
			}
			if old.Spec.VolumeMode != nil && !reflect.DeepEqual(old.Spec.VolumeMode, next.Spec.VolumeMode) {
				return false, "volume mode immutable"
			}
		}
		return true, ""
	case "PersistentVolume":
		var old, next v1.PersistentVolume
		_ = json.Unmarshal(r.OldObject.Raw, &old)
		_ = json.Unmarshal(r.Object.Raw, &next)
		managed := old.Labels[kube.ManagedLabel] == "true" || next.Labels[kube.ManagedLabel] == "true"
		claim := next.Spec.ClaimRef
		if claim == nil {
			claim = old.Spec.ClaimRef
		}
		if !managed && claim != nil {
			p, e := h.API.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(e) {
				return true, ""
			}
			if e != nil {
				return false, "cannot verify PV claim protection"
			}
			managed = p.Labels[kube.ManagedLabel] == "true"
		}
		if !managed {
			return true, ""
		}
		if r.Operation == admission.Create && next.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
			return false, "managed PV must start with Retain"
		}
		uid := ""
		if old.Spec.ClaimRef != nil {
			uid = string(old.Spec.ClaimRef.UID)
		}
		if r.Operation == admission.Delete {
			return h.permit(ctx, uid, "Delete")
		}
		if r.Operation == admission.Update {
			if (old.Spec.ClaimRef == nil || old.Spec.ClaimRef.UID == "") && next.Spec.ClaimRef != nil && next.Spec.ClaimRef.UID != "" {
				if ok, reason := h.initialBinding(ctx, &old, &next); !ok {
					return ok, reason
				}
			}
			if old.Labels[kube.ManagedLabel] == "true" && next.Labels[kube.ManagedLabel] != "true" {
				return false, "cannot remove consumption protection"
			}
			if !reflect.DeepEqual(old.Spec.CSI, next.Spec.CSI) {
				return false, "backend identity/parameters immutable while managed"
			}
			if old.Spec.PersistentVolumeReclaimPolicy != next.Spec.PersistentVolumeReclaimPolicy && next.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
				return h.permit(ctx, uid, "Delete")
			}
			if old.Spec.ClaimRef != nil && old.Spec.ClaimRef.UID != "" && (next.Spec.ClaimRef == nil || old.Spec.ClaimRef.UID != next.Spec.ClaimRef.UID || old.Spec.ClaimRef.Name != next.Spec.ClaimRef.Name || old.Spec.ClaimRef.Namespace != next.Spec.ClaimRef.Namespace) {
				return h.permit(ctx, uid, "Retain")
			}
		}
		return true, ""
	default:
		return false, fmt.Sprintf("unsupported admission kind %s", r.Kind.Kind)
	}
}
