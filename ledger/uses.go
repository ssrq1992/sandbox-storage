package ledger

import (
	"context"
	"encoding/json"
	"ssf.dev/ssf/core"
)

func (s *Store) Uses(ctx context.Context, pvcUID string) ([]core.Binding, error) {
	rows, e := s.executor(ctx).Query(ctx, `SELECT b.descriptor,b.state FROM ssf_bindings b JOIN ssf_volumes v ON b.volume_key=v.key WHERE v.pvc_uid=$1 AND b.state<>'Released' ORDER BY b.id`, pvcUID)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []core.Binding
	for rows.Next() {
		var data []byte
		var state string
		if e = rows.Scan(&data, &state); e != nil {
			return nil, e
		}
		var b core.Binding
		if e = json.Unmarshal(data, &b); e != nil {
			return nil, e
		}
		b.State = state
		out = append(out, b)
	}
	return out, rows.Err()
}
