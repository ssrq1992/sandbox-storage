package overlaybd

import (
	"context"
	"errors"
	"ssf.dev/ssf/ledger"
)

type LedgerDeletion struct{ Store *ledger.Store }

type LedgerExpansion struct{ Store *ledger.Store }

func (a LedgerExpansion) ReserveNativeExpansion(ctx context.Context, volume CatalogVolume, operation string, capacity int64, requested ...int64) error {
	if a.Store == nil || volume.Readonly {
		return ErrConflict
	}
	handle, err := VolumeHandle(volume.RepositoryID, volume.NativeID)
	if err != nil || handle != volume.Handle {
		return ErrConflict
	}
	uid, err := a.Store.BackendPVC(ctx, DriverName, handle)
	if err != nil {
		return err
	}
	if volume.PVCUID != "" && volume.PVCUID != uid {
		return ErrConflict
	}
	return a.Store.ReserveBackendExpansion(ctx, uid, operation, capacity, requested...)
}
func (a LedgerExpansion) CompleteNativeExpansion(ctx context.Context, volume CatalogVolume, operation string, capacity int64) error {
	if a.Store == nil {
		return ErrConflict
	}
	uid, err := a.Store.BackendPVC(ctx, DriverName, volume.Handle)
	if err != nil {
		return err
	}
	return a.Store.CompleteBackendExpansion(ctx, uid, operation, capacity)
}

func (a LedgerExpansion) AuthorizeNativeExpansion(ctx context.Context, volume CatalogVolume, operation, head string, capacity int64) (string, error) {
	if a.Store == nil || volume.Readonly {
		return "", errors.New("writable volume and SSF maintenance ledger required")
	}
	handle, err := VolumeHandle(volume.RepositoryID, volume.NativeID)
	if err != nil || handle != volume.Handle {
		return "", errors.New("driver catalog backend identity mismatch")
	}
	uid, err := a.Store.BackendPVC(ctx, DriverName, handle)
	if err != nil {
		return "", err
	}
	if volume.PVCUID != "" && volume.PVCUID != uid {
		return "", ErrConflict
	}
	return a.Store.AuthorizeBackendExpansion(ctx, DriverName, handle, uid, operation, head, capacity)
}

func (a LedgerDeletion) AuthorizeNativeDeletion(ctx context.Context, volume CatalogVolume, operation string) (string, error) {
	if a.Store == nil {
		return "", errors.New("SSF ledger unavailable")
	}
	handle, err := VolumeHandle(volume.RepositoryID, volume.NativeID)
	if err != nil || handle != volume.Handle {
		return "", errors.New("driver catalog backend identity mismatch")
	}
	return a.Store.AuthorizeBackendReclaim(ctx, DriverName, handle, operation)
}
