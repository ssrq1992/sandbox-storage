package agentenvadapter

import (
	"context"
	"errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"ssf.dev/ssf/core"
	"strconv"
)

// Expand records an offline intent; native Controller sidecars do provisioning.
// A pause hold or an uncertain attachment keeps the intent pending indefinitely.
func (s *NativeVolumes) Expand(ctx context.Context, tenant, name string, bytes int64) (NativeVolume, error) {
	var out NativeVolume
	if bytes <= 0 {
		return out, errors.New("positive expansion capacity required")
	}
	err := s.Ledger.Exclusive(ctx, "agentenv/volume/"+tenant+"/"+name, func(ctx context.Context) error {
		v, err := s.Get(ctx, tenant, name)
		if err != nil {
			return err
		}
		if !v.ReadOnly && v.State == "Bound" && v.ExpansionRequested == 0 && v.ExpansionTarget == bytes {
			out = v
			return nil
		}
		if v.ReadOnly || v.State == "Deleted" || v.State == "Reclaiming" || v.State == "Deleting" || v.PVC.UID == "" || bytes < v.Bytes {
			return core.ErrConflict
		}
		if v.ExpansionRequested > 0 && v.ExpansionRequested != bytes {
			return core.ErrConflict
		}
		if v.ExpansionRequested == 0 && bytes == v.Bytes {
			out = v
			return nil
		}
		sc, err := s.Kube.API.StorageV1().StorageClasses().Get(ctx, v.StorageClass, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion {
			return errors.New("StorageClass does not allow expansion")
		}
		volume, err := s.Kube.Resolve(ctx, v.PVC, false)
		if err != nil {
			return err
		}
		if err = s.Ledger.Register(ctx, volume); err != nil {
			return err
		}
		v.ExpansionRequested = bytes
		if err = s.save(ctx, v); err != nil {
			return err
		}
		out = v
		token := "agentenv/expand/" + core.Hash(tenant+"/"+name+"/"+v.PVC.UID+"/"+strconv.FormatInt(bytes, 10))
		if err = s.Ledger.BeginOfflineMaintenance(ctx, v.PVC.UID, token, bytes); err != nil {
			return err
		}
		v.State = "Expanding"
		v.ExpansionTarget = bytes
		if err = s.save(ctx, v); err != nil {
			return err
		}
		out = v
		if err = s.Kube.Expand(ctx, v.PVC, strconv.FormatInt(bytes, 10)); err != nil {
			return err
		}
		// PV capacity is Controller completion. PVC status may wait for NodeExpand.
		claim, err := s.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		pv, err := s.Kube.API.CoreV1().PersistentVolumes().Get(ctx, volume.PVName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(claim.UID) != v.PVC.UID || claim.Spec.VolumeName != pv.Name || string(pv.UID) != volume.PVUID || claim.DeletionTimestamp != nil || pv.DeletionTimestamp != nil {
			return core.ErrConflict
		}
		capacity := pv.Spec.Capacity[v1.ResourceStorage]
		if capacity.Value() < bytes {
			return core.ErrPending
		}
		// Persist the Node/filesystem requirement before allowing a new consumer.
		if err = s.Ledger.CompleteOfflineMaintenance(ctx, v.PVC.UID, token, bytes); err != nil {
			return err
		}
		v.Bytes = max(v.Bytes, bytes)
		v.State = "Bound"
		v.ExpansionRequested = 0
		if err = s.save(ctx, v); err != nil {
			return err
		}
		out = v
		return nil
	})
	return out, err
}
