package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"ssf.dev/ssf/core"
)

// DrainIdentity also protects a Pending PVC that has never had a physical handle.
// The unique PVC UID prevents a racing Register from admitting it after drain.
func (s *Store) DrainIdentity(ctx context.Context, ref core.PVCRef) error {
	if ref.UID == "" {
		return errors.New("PVC UID required")
	}
	v := core.Volume{UID: ref.UID, PVC: ref, Driver: "ssf.pending", Handle: ref.UID}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = s.executor(ctx).Exec(ctx, `INSERT INTO ssf_volumes(key,pvc_uid,descriptor,draining) VALUES($1,$2,$3,true) ON CONFLICT(pvc_uid) DO UPDATE SET draining=true`, v.Key(), ref.UID, b)
	return e
}
