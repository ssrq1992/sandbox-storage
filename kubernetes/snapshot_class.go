package kubernetes

import (
	"context"
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// DefaultSnapshotClass uses Kubernetes' driver-scoped default convention.
// Ambiguity is an error; an arbitrary class must never change retention policy.
func (c *Client) DefaultSnapshotClass(ctx context.Context, driver string) (string, error) {
	if c.Dynamic == nil {
		return "", errors.New("snapshot client unavailable")
	}
	classes, err := c.Dynamic.Resource(snapshotClasses).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	name := ""
	for _, class := range classes.Items {
		actual, _, _ := unstructured.NestedString(class.Object, "driver")
		if actual != driver || class.GetAnnotations()["snapshot.storage.kubernetes.io/is-default-class"] != "true" {
			continue
		}
		policy, _, _ := unstructured.NestedString(class.Object, "deletionPolicy")
		if policy != "Retain" || class.GetDeletionTimestamp() != nil {
			return "", errors.New("default snapshot class must use Retain")
		}
		if name != "" {
			return "", errors.New("multiple default snapshot classes for CSI driver")
		}
		name = class.GetName()
	}
	if name == "" {
		return "", errors.New("no default snapshot class for CSI driver")
	}
	return name, nil
}
