package agentenvadapter

import (
	"context"
	"errors"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"log/slog"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"time"
)

func (c *NativeControl) recoverNativeCloneGC(ctx context.Context) error {
	rows, err := c.Ledger.Session(ctx).Query(ctx, `SELECT v.name,r.id FROM agentenv_native_volumes v JOIN agentenv_native_runs r ON r.id=v.descriptor->>'internalOwner' WHERE v.tenant=$1 AND r.intent->>'tenant'=$1 AND v.descriptor->>'state'<>'Deleted' AND r.state IN ('Released','Revoked') AND (NOT r.ever_ready OR r.intent->>'path'='/internal/compute' OR v.descriptor->>'private'='true') ORDER BY v.gc_attempt_at,v.name LIMIT 100`, c.Principal.Tenant)
	if err != nil {
		return err
	}
	var work [][2]string
	for rows.Next() {
		var item [2]string
		if err = rows.Scan(&item[0], &item[1]); err != nil {
			rows.Close()
			return err
		}
		work = append(work, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, item := range work {
		if _, err = c.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_volumes SET gc_attempt_at=now() WHERE tenant=$1 AND name=$2`, c.Principal.Tenant, item[0]); err != nil {
			return err
		}
		if err = c.reclaimNativeClone(ctx, item[0], item[1]); err != nil && !errors.Is(err, core.ErrPending) {
			slog.Warn("native internal clone retained", "volume", item[0], "error", err)
		}
	}
	return nil
}

// Only never-ready failed clones and private compute clones are owned cleanup.
// Ordinary successfully exposed sandbox volumes survive sandbox deletion.
func (c *NativeControl) reclaimNativeClone(ctx context.Context, name, request string) error {
	return c.Ledger.Exclusive(ctx, "agentenv/native-run/"+request, func(ctx context.Context) error {
		var eligible bool
		if err := c.Ledger.Session(ctx).QueryRow(ctx, `SELECT state IN ('Released','Revoked') AND (NOT ever_ready OR intent->>'path'='/internal/compute' OR EXISTS(SELECT 1 FROM agentenv_native_volumes v WHERE v.tenant=$2 AND v.name=$3 AND v.descriptor->>'internalOwner'=$1 AND v.descriptor->>'private'='true')) AND intent->>'tenant'=$2 FROM agentenv_native_runs WHERE id=$1`, request, c.Principal.Tenant, name).Scan(&eligible); err != nil {
			return err
		}
		if !eligible {
			return core.ErrConflict
		}
		s := c.Volumes
		return s.Ledger.Exclusive(ctx, "agentenv/volume/"+c.Principal.Tenant+"/"+name, func(ctx context.Context) error {
			v, err := s.Get(ctx, c.Principal.Tenant, name)
			if err != nil {
				return err
			}
			if v.InternalOwner != request {
				return core.ErrConflict
			}
			if v.State == "Deleted" {
				return nil
			}
			if v.ExpansionRequested > 0 {
				return core.ErrPending
			}
			v.State = "Reclaiming"
			if err = s.save(ctx, v); err != nil {
				return err
			}
			claim, err := s.Kube.API.CoreV1().PersistentVolumeClaims(v.PVC.Namespace).Get(ctx, v.PVC.Name, metav1.GetOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			if apierrors.IsNotFound(err) {
				claim = nil
			}
			if err == nil {
				if claim.Labels[kube.ManagedLabel] != "true" || claim.Annotations["agentenv.ssf.dev/intent"] != v.IntentDigest || (v.PVC.UID != "" && v.PVC.UID != string(claim.UID)) {
					return core.ErrConflict
				}
				v.PVC.UID = string(claim.UID)
				if v.PVC.UID == "" {
					return core.ErrPending
				}
				if claim.Spec.VolumeName != "" {
					pv, e := s.Kube.API.CoreV1().PersistentVolumes().Get(ctx, claim.Spec.VolumeName, metav1.GetOptions{})
					if e != nil {
						return e
					}
					if pv.Spec.ClaimRef == nil || string(pv.Spec.ClaimRef.UID) != v.PVC.UID || pv.Spec.ClaimRef.Namespace != v.PVC.Namespace || pv.Spec.ClaimRef.Name != v.PVC.Name || pv.Spec.CSI == nil {
						return core.ErrConflict
					}
					if v.ReclaimPVUID != "" && (v.ReclaimPVUID != string(pv.UID) || v.ReclaimPVName != pv.Name) {
						return core.ErrConflict
					}
					v.ReclaimPVName = pv.Name
					v.ReclaimPVUID = string(pv.UID)
				}
				if err = s.save(ctx, v); err != nil {
					return err
				}
			}
			if v.PVC.UID != "" {
				if err = s.Ledger.DrainIdentity(ctx, v.PVC); err != nil {
					return err
				}
				if err = s.Ledger.Permit(ctx, v.PVC.UID, "Delete", 5*time.Minute); err != nil {
					return err
				}
			}
			if claim == nil && v.ReclaimPVName == "" && v.PVC.UID != "" {
				volumes, e := s.Kube.API.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
				if e != nil {
					return e
				}
				for _, pv := range volumes.Items {
					ref := pv.Spec.ClaimRef
					if ref == nil || string(ref.UID) != v.PVC.UID {
						continue
					}
					if ref.Name != v.PVC.Name || ref.Namespace != v.PVC.Namespace || pv.Spec.CSI == nil || v.ReclaimPVName != "" {
						return core.ErrConflict
					}
					v.ReclaimPVName, v.ReclaimPVUID = pv.Name, string(pv.UID)
				}
				if v.ReclaimPVName != "" {
					if err = s.save(ctx, v); err != nil {
						return err
					}
				}
			}
			if v.ReclaimPVName != "" {
				pv, e := s.Kube.API.CoreV1().PersistentVolumes().Get(ctx, v.ReclaimPVName, metav1.GetOptions{})
				if e != nil && !apierrors.IsNotFound(e) {
					return e
				}
				if e == nil {
					if string(pv.UID) != v.ReclaimPVUID || pv.Spec.ClaimRef == nil || string(pv.Spec.ClaimRef.UID) != v.PVC.UID {
						return core.ErrConflict
					}
					if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimDelete {
						if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain || pv.DeletionTimestamp != nil {
							return core.ErrConflict
						}
						if pv.Labels == nil {
							pv.Labels = map[string]string{}
						}
						pv.Labels[kube.ManagedLabel] = "true"
						pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimDelete
						if _, e = s.Kube.API.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{}); e != nil {
							return e
						}
					}
				}
			}
			if claim != nil {
				if err = s.Kube.DeletePVC(ctx, v.PVC); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
				return core.ErrPending
			}
			if v.ReclaimPVName != "" {
				pv, e := s.Kube.API.CoreV1().PersistentVolumes().Get(ctx, v.ReclaimPVName, metav1.GetOptions{})
				if e != nil && !apierrors.IsNotFound(e) {
					return e
				}
				if e == nil {
					if string(pv.UID) != v.ReclaimPVUID {
						return core.ErrConflict
					}
					uid := types.UID(v.ReclaimPVUID)
					if e = s.Kube.API.CoreV1().PersistentVolumes().Delete(ctx, v.ReclaimPVName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); e != nil && !apierrors.IsNotFound(e) {
						return e
					}
					return core.ErrPending
				}
			}
			if v.SourcePin != "" {
				if err = s.Ledger.ReleaseCheckpoint(ctx, v.SourcePin); err != nil {
					return err
				}
				v.SourcePin = ""
			}
			v.State = "Deleted"
			return s.save(ctx, v)
		})
	})
}
