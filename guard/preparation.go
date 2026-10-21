package guard

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	admission "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
)

func (h *Handler) coordinator(user string) bool { return slices.Contains(h.CoordinatorUsers, user) }
func (h *Handler) profile(ctx context.Context, p *v1.PersistentVolumeClaim) (core.VolumeProfile, bool, error) {
	if !slices.Contains(h.ManagedNamespaces, p.Namespace) {
		return core.VolumeProfile{}, false, nil
	}
	return (&kube.Client{API: h.API}).VolumeProfile(ctx, p, h.VolumeProfiles)
}
func (h *Handler) mutate(ctx context.Context, r *admission.AdmissionRequest) ([]byte, error) {
	if r.Kind.Kind != "PersistentVolumeClaim" || r.Operation != admission.Create {
		return nil, nil
	}
	var p v1.PersistentVolumeClaim
	if err := json.Unmarshal(r.Object.Raw, &p); err != nil {
		return nil, err
	}
	p.Namespace = r.Namespace
	profile, managed, err := h.profile(ctx, &p)
	if err != nil || !managed {
		return nil, err
	}
	// A creator may not choose another profile by pre-populating the pin.
	copy := p.DeepCopy()
	delete(copy.Annotations, kube.VolumeProfileAnnotation)
	delete(copy.Annotations, kube.ProfileRevisionAnnotation)
	scProfile, _, err := h.profile(ctx, copy)
	if err != nil {
		return nil, err
	}
	if scProfile != profile {
		return nil, errors.New("PVC profile differs from its StorageClass")
	}
	for _, key := range preparationStatusKeys {
		if p.Annotations[key] != "" {
			return nil, errors.New("preparation status is controller-owned")
		}
	}
	if p.Spec.VolumeMode != nil && *p.Spec.VolumeMode != v1.PersistentVolumeBlock {
		return nil, errors.New("profile requires Block PVC")
	}
	if p.Labels == nil {
		p.Labels = map[string]string{}
	}
	if p.Annotations == nil {
		p.Annotations = map[string]string{}
	}
	p.Labels[kube.ManagedLabel] = "true"
	p.Annotations[kube.VolumeProfileAnnotation] = profile.Name
	p.Annotations[kube.ProfileRevisionAnnotation] = core.Digest(profile)
	class, err := h.API.StorageV1().StorageClasses().Get(ctx, *p.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if class.UID == "" {
		return nil, errors.New("StorageClass UID required")
	}
	p.Annotations[kube.ProfileClassUIDAnnotation] = string(class.UID)
	if p.Annotations[kube.SourceRevisionAnnotation] != "" {
		return nil, errors.New("source identity is admission-owned")
	}
	revision, err := (&kube.Client{API: h.API, Dynamic: h.Dynamic}).PreparationSourceRevision(ctx, &p)
	if err != nil {
		return nil, err
	}
	p.Annotations[kube.SourceRevisionAnnotation] = revision
	if !slices.Contains(p.Finalizers, kube.VolumeProtectionFinalizer) {
		p.Finalizers = append(p.Finalizers, kube.VolumeProtectionFinalizer)
	}
	type patch struct {
		Op    string `json:"op"`
		Path  string `json:"path"`
		Value any    `json:"value"`
	}
	return json.Marshal([]patch{
		{"add", "/metadata/labels", p.Labels}, {"add", "/metadata/annotations", p.Annotations},
		{"add", "/metadata/finalizers", p.Finalizers}, {"add", "/spec/volumeMode", "Block"},
	})
}

var preparationStatusKeys = []string{kube.PreparationStateAnnotation, kube.PreparationReasonAnnotation, kube.PreparationRevisionAnnotation}

func (h *Handler) checkPreparation(ctx context.Context, r *admission.AdmissionRequest, old, next *v1.PersistentVolumeClaim) (bool, string) {
	if r.Operation == admission.Delete {
		if old.Labels[kube.ManagedLabel] == "true" && old.Annotations[kube.ProfileRevisionAnnotation] != "" && slices.Contains(old.Finalizers, kube.VolumeProtectionFinalizer) {
			return true, ""
		}
		return false, ""
	}
	next.Namespace = r.Namespace
	profile, managed, err := h.profile(ctx, next)
	if err != nil {
		return false, err.Error()
	}
	if !managed {
		return true, ""
	}
	if r.Operation == admission.Create {
		if err = kube.ValidatePreparedClaim(next, profile); err != nil {
			return false, err.Error()
		}
		revision, err := (&kube.Client{API: h.API, Dynamic: h.Dynamic}).PreparationSourceRevision(ctx, next)
		if err != nil || revision != next.Annotations[kube.SourceRevisionAnnotation] {
			return false, "source identity must be pinned at admission"
		}
		for _, k := range preparationStatusKeys {
			if next.Annotations[k] != "" {
				return false, "preparation status is controller-owned"
			}
		}
		return true, ""
	}
	for _, k := range []string{kube.VolumeProfileAnnotation, kube.ProfileRevisionAnnotation, kube.ProfileClassUIDAnnotation, kube.SourceRevisionAnnotation} {
		if old.Annotations[k] != next.Annotations[k] {
			return false, "preparation profile is immutable"
		}
	}
	for _, k := range preparationStatusKeys {
		if old.Annotations[k] != next.Annotations[k] && !h.coordinator(r.UserInfo.Username) {
			return false, "preparation status is controller-owned"
		}
	}
	if slices.Contains(old.Finalizers, kube.VolumeProtectionFinalizer) && !slices.Contains(next.Finalizers, kube.VolumeProtectionFinalizer) {
		if !h.coordinator(r.UserInfo.Username) || old.DeletionTimestamp == nil {
			return false, "volume finalizer removal requires draining coordinator"
		}
		return h.permit(ctx, string(old.UID), "Retain")
	}
	return true, ""
}
