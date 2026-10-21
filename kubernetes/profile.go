package kubernetes

import (
	"context"
	"errors"
	"slices"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
)

const (
	VolumeProfileAnnotation       = "ssf.dev/volume-profile"
	ProfileRevisionAnnotation     = "ssf.dev/profile-revision"
	ProfileClassUIDAnnotation     = "ssf.dev/profile-class-uid"
	SourceRevisionAnnotation      = "ssf.dev/source-revision"
	PreparationStateAnnotation    = "ssf.dev/preparation-state"
	PreparationReasonAnnotation   = "ssf.dev/preparation-reason"
	PreparationRevisionAnnotation = "ssf.dev/preparation-revision"
	VolumeProtectionFinalizer     = "ssf.dev/volume-protection"
)

func ValidateProfiles(profiles []core.VolumeProfile) error {
	names := map[string]bool{}
	for _, p := range profiles {
		if err := p.Validate(); err != nil {
			return err
		}
		if names[p.Name] {
			return errors.New("duplicate volume profile")
		}
		names[p.Name] = true
	}
	return nil
}

func (c *Client) VolumeProfile(ctx context.Context, p *v1.PersistentVolumeClaim, profiles []core.VolumeProfile) (core.VolumeProfile, bool, error) {
	var empty core.VolumeProfile
	if p.Spec.StorageClassName == nil || *p.Spec.StorageClassName == "" {
		return empty, false, nil
	}
	sc, err := c.API.StorageV1().StorageClasses().Get(ctx, *p.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return empty, false, err
	}
	if pinned := p.Annotations[ProfileClassUIDAnnotation]; pinned != "" && pinned != string(sc.UID) {
		return empty, true, errors.New("profile StorageClass identity changed")
	}
	name := sc.Annotations[VolumeProfileAnnotation]
	// Once pinned, changes to the class cannot silently migrate existing data.
	if prior := p.Annotations[VolumeProfileAnnotation]; prior != "" {
		name = prior
	}
	if name == "" {
		return empty, false, nil
	}
	for _, profile := range profiles {
		if profile.Name == name {
			if err = profile.Validate(); err != nil {
				return empty, true, err
			}
			if sc.ReclaimPolicy == nil || *sc.ReclaimPolicy != v1.PersistentVolumeReclaimRetain {
				return empty, true, errors.New("profiled class must use Retain")
			}
			if revision := p.Annotations[ProfileRevisionAnnotation]; revision != "" && revision != core.Digest(profile) {
				return empty, true, errors.New("pinned volume profile changed")
			}
			return profile, true, nil
		}
	}
	return empty, true, errors.New("volume profile is unavailable")
}

func ValidatePreparedClaim(p *v1.PersistentVolumeClaim, profile core.VolumeProfile) error {
	if p.DeletionTimestamp != nil || p.Spec.VolumeMode == nil || *p.Spec.VolumeMode != v1.PersistentVolumeBlock ||
		p.Labels[ManagedLabel] != "true" || p.Annotations[VolumeProfileAnnotation] != profile.Name ||
		p.Annotations[ProfileClassUIDAnnotation] == "" ||
		p.Annotations[SourceRevisionAnnotation] == "" ||
		p.Annotations[ProfileRevisionAnnotation] != core.Digest(profile) || !slices.Contains(p.Finalizers, VolumeProtectionFinalizer) {
		return errors.New("PVC preparation protection or profile missing")
	}
	return nil
}
