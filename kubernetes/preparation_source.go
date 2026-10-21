package kubernetes

import (
	"context"
	"errors"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"ssf.dev/ssf/core"
)

// PreparationSourceRevision pins source object identities, not their reusable
// names. Cross-namespace references require a separate authorization contract.
func (c *Client) PreparationSourceRevision(ctx context.Context, p *v1.PersistentVolumeClaim) (string, error) {
	var name, kind, group string
	if s := p.Spec.DataSource; s != nil {
		name, kind = s.Name, s.Kind
		if s.APIGroup != nil {
			group = *s.APIGroup
		}
	}
	if s := p.Spec.DataSourceRef; s != nil {
		g := ""
		if s.APIGroup != nil {
			g = *s.APIGroup
		}
		if s.Namespace != nil && *s.Namespace != p.Namespace {
			return "", errors.New("cross-namespace preparation source unsupported")
		}
		if name != "" && (name != s.Name || kind != s.Kind || group != g) {
			return "", errors.New("PVC source references disagree")
		}
		name, kind, group = s.Name, s.Kind, g
	}
	if name == "" {
		if kind != "" || p.Spec.DataSource != nil || p.Spec.DataSourceRef != nil {
			return "", errors.New("empty preparation source")
		}
		return core.Digest("blank"), nil
	}
	switch {
	case kind == "PersistentVolumeClaim" && group == "":
		source, err := c.API.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		if source.UID == "" || source.DeletionTimestamp != nil || source.Status.Phase != v1.ClaimBound {
			return "", core.ErrPending
		}
		return core.Digest([]string{kind, p.Namespace, name, string(source.UID)}), nil
	case kind == "VolumeSnapshot" && group == snapshots.Group:
		if c.Dynamic == nil {
			return "", errors.New("snapshot client unavailable")
		}
		source, err := c.Dynamic.Resource(snapshots).Namespace(p.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		if source.GetUID() == "" || source.GetDeletionTimestamp() != nil {
			return "", core.ErrPending
		}
		content, _, _ := unstructured.NestedString(source.Object, "status", "boundVolumeSnapshotContentName")
		ready, _, _ := unstructured.NestedBool(source.Object, "status", "readyToUse")
		if content == "" || !ready {
			return "", core.ErrPending
		}
		contentUID := ""
		if content != "" {
			bound, err := c.Dynamic.Resource(snapshotContents).Get(ctx, content, metav1.GetOptions{})
			if err != nil {
				return "", err
			}
			ref, _, _ := unstructured.NestedString(bound.Object, "spec", "volumeSnapshotRef", "uid")
			if ref != string(source.GetUID()) {
				return "", core.ErrConflict
			}
			contentUID = string(bound.GetUID())
			if contentUID == "" || bound.GetDeletionTimestamp() != nil {
				return "", core.ErrPending
			}
		}
		return core.Digest([]string{group, kind, p.Namespace, name, string(source.GetUID()), contentUID}), nil
	default:
		return "", errors.New("unsupported preparation data source")
	}
}
