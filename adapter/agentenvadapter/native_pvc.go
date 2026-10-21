package agentenvadapter

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"strconv"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"ssf.dev/ssf/core"
)

// Only immutable internal consumption metadata is stored. No alias is claimed,
// no PVC is created and no Kubernetes-owned volume is adopted for deletion.
func (c *NativeControl) resolvePVCMount(ctx context.Context, request string, index int, m NativeMount) (NativeMount, error) {
	if c.Preparation == nil || m.PVC == nil {
		return m, errors.New("direct PVC preparation is unavailable")
	}
	namespace := m.PVC.Namespace
	if namespace == "" {
		namespace = c.DefaultNamespace
	}
	if namespace == "" || !c.Principal.Allows(namespace) || len(validation.IsDNS1123Label(namespace)) != 0 || len(validation.IsDNS1123Subdomain(m.PVC.Name)) != 0 {
		return m, errors.New("PVC namespace or name is not authorized")
	}
	input := core.PVCRef{Namespace: namespace, Name: m.PVC.Name}
	var v NativeVolume
	err := c.Ledger.Step(ctx, "agentenv/pvc-source/"+request+"/"+strconv.Itoa(index), core.Digest(input), func(ctx context.Context) (any, error) {
		record, managed, err := c.Preparation.Observe(ctx, input)
		if err != nil {
			return nil, err
		}
		if !managed {
			return nil, errors.New("PVC StorageClass must select an enabled volume profile")
		}
		claim, err := c.Kube.API.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, input.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if string(claim.UID) != record.PVC.UID || claim.DeletionTimestamp != nil {
			return nil, core.ErrConflict
		}
		readonly := false
		for _, mode := range claim.Spec.AccessModes {
			if mode == v1.ReadOnlyMany {
				readonly = true
			}
		}
		return NativeVolume{DirectPVC: true, PVCSource: true, Private: true, Provider: "ssf",
			Name: "pvc/" + record.PVC.UID, Tenant: c.Principal.Tenant, PVC: record.PVC, StorageClass: *claim.Spec.StorageClassName,
			Bytes: claim.Spec.Resources.Requests.Storage().Value(), ReadOnly: readonly, State: string(claim.Status.Phase), IntentDigest: record.IntentDigest()}, nil
	}, &v)
	if err != nil {
		return m, err
	}
	if !v.DirectPVC || v.Tenant != c.Principal.Tenant || v.PVC.Namespace != namespace || v.PVC.Name != input.Name {
		return m, core.ErrConflict
	}
	// Revalidate UID on every retry, never resolve a replacement by name.
	claim, err := c.Kube.API.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, input.Name, metav1.GetOptions{})
	if err != nil {
		return m, err
	}
	if string(claim.UID) != v.PVC.UID || claim.DeletionTimestamp != nil {
		return m, core.ErrConflict
	}
	err = c.Ledger.Exclusive(ctx, "agentenv/volume/"+v.Tenant+"/"+v.Name, func(ctx context.Context) error {
		prior, err := c.Volumes.Get(ctx, v.Tenant, v.Name)
		if err == nil {
			if !prior.DirectPVC || prior.PVC != v.PVC || prior.IntentDigest != v.IntentDigest {
				return core.ErrConflict
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		return c.Volumes.save(ctx, v)
	})
	if err != nil {
		return m, err
	}
	m.Name = v.Name
	return m, nil
}
