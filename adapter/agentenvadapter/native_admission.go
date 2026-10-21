package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"ssf.dev/ssf/core"
	kube "ssf.dev/ssf/kubernetes"
	"ssf.dev/ssf/ledger"
)

// NativeCandidate is projected from AgentENV's live discovery and heartbeat.
// It is not an adapter-owned static worker inventory.
type NativeCandidate struct {
	Defaults   Resources `json:"defaults"`
	ID         string    `json:"id"`
	Endpoint   string    `json:"endpoint"`
	KubeNode   string    `json:"kube_node"`
	NodeUID    string    `json:"node_uid"`
	BootID     string    `json:"boot_id"`
	PluginURL  string    `json:"plugin_url"`
	Capacity   Resources `json:"capacity"`
	Allocated  Resources `json:"allocated"`
	SandboxIDs []string  `json:"sandbox_ids"`
}

type NativeReservation struct {
	RequestedResources Resources       `json:"requested_resources"`
	Placement          core.Placement  `json:"placement"`
	Candidate          NativeCandidate `json:"candidate"`
	Resources          Resources       `json:"resources"`
	NativeID           string          `json:"native_id"`
	State              string          `json:"state"`
}

const NativeSchema = `
CREATE TABLE IF NOT EXISTS agentenv_native_reservations (
 id text PRIMARY KEY, digest text NOT NULL, node text NOT NULL,
 native_id text NOT NULL DEFAULT '', cpu bigint NOT NULL CHECK(cpu>0),
 memory_mib bigint NOT NULL CHECK(memory_mib>0),
 state text NOT NULL DEFAULT 'Reserved', descriptor jsonb NOT NULL,
 updated timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS agentenv_native_reservations_node ON agentenv_native_reservations(node,state);
CREATE TABLE IF NOT EXISTS agentenv_native_runs (
 id text PRIMARY KEY, digest text NOT NULL, owner_uid text NOT NULL,run_uid text NOT NULL UNIQUE,
 native_id text NOT NULL DEFAULT '', state text NOT NULL DEFAULT 'Intent',
 intent jsonb NOT NULL, dispatch jsonb NOT NULL DEFAULT '{}',
 updated timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE agentenv_native_runs ADD COLUMN IF NOT EXISTS ever_ready boolean NOT NULL DEFAULT true;
ALTER TABLE agentenv_native_runs ALTER COLUMN ever_ready SET DEFAULT false;
CREATE TABLE IF NOT EXISTS agentenv_native_retired_captures(request_id text NOT NULL,barrier_id text NOT NULL,PRIMARY KEY(request_id,barrier_id));
CREATE TABLE IF NOT EXISTS agentenv_native_publications (
 tenant text NOT NULL, repository_id text NOT NULL, native_id text NOT NULL, reference text NOT NULL,
 state text NOT NULL DEFAULT 'Publishing', descriptor jsonb NOT NULL,
 PRIMARY KEY(tenant,repository_id,native_id)
);
CREATE TABLE IF NOT EXISTS agentenv_native_template_consumers (
 reference text PRIMARY KEY, tenant text NOT NULL, source_reference text NOT NULL,
 request_id text NOT NULL, state text NOT NULL DEFAULT 'Active'
);
CREATE INDEX IF NOT EXISTS agentenv_native_template_consumer_source ON agentenv_native_template_consumers(tenant,source_reference,state);
CREATE TABLE IF NOT EXISTS agentenv_native_volumes (
 tenant text NOT NULL,name text NOT NULL,descriptor jsonb NOT NULL,
 PRIMARY KEY(tenant,name)
);
ALTER TABLE agentenv_native_volumes ADD COLUMN IF NOT EXISTS gc_attempt_at timestamptz NOT NULL DEFAULT 'epoch';
CREATE TABLE IF NOT EXISTS agentenv_native_volume_names (
 tenant text NOT NULL,name text NOT NULL,provider text NOT NULL,
 PRIMARY KEY(tenant,name)
);
INSERT INTO agentenv_native_volume_names(tenant,name,provider)
 SELECT tenant,name,'ssf' FROM agentenv_native_volumes ON CONFLICT DO NOTHING;
`

func MigrateNative(ctx context.Context, s *ledger.Store) error {
	return s.Exclusive(ctx, "agentenv/native/schema", func(ctx context.Context) error {
		// Native schema only creates idempotent objects. Shared SSF schema migrations
		// remain the responsibility of ledger.Store.Migrate.
		_, err := s.Session(ctx).Exec(ctx, NativeSchema)
		return err
	})
}

// NativeAdmission uses the native strategy once storage feasibility has been
// established. Unknown outcomes retain capacity, irrespective of lease expiry.
type NativeAdmission struct {
	Ledger     *ledger.Store
	Kube       *kube.Client
	Candidates []NativeCandidate
	Resources  Resources
	Choose     func([]NativeCandidate) (NativeCandidate, error)
	// Refresh verifies the actual Pod/K8S Node/boot identity before side effects.
	Refresh func(context.Context, NativeCandidate) (NativeCandidate, error)
}

func (p *NativeAdmission) load(ctx context.Context, id string) (NativeReservation, error) {
	var r NativeReservation
	var raw []byte
	err := p.Ledger.Session(ctx).QueryRow(ctx, `SELECT descriptor,native_id,state FROM agentenv_native_reservations WHERE id=$1`, id).Scan(&raw, &r.NativeID, &r.State)
	if err != nil {
		return r, err
	}
	var persisted NativeReservation
	if err = json.Unmarshal(raw, &persisted); err != nil {
		return r, err
	}
	persisted.NativeID = r.NativeID
	persisted.State = r.State
	return persisted, nil
}
func (p *NativeAdmission) Reserve(ctx context.Context, id string, volumes []core.Volume) (core.Placement, error) {
	var result core.Placement
	if p.Resources.CPU < 0 || p.Resources.MemoryMiB < 0 || p.Choose == nil || p.Refresh == nil {
		return result, errors.New("native admission dependencies and resources required")
	}
	// The operation lock prevents two retries from selecting different nodes.
	err := p.Ledger.Exclusive(ctx, "agentenv/reservation/"+id, func(ctx context.Context) error {
		if prior, err := p.load(ctx, id); err == nil {
			if prior.RequestedResources != p.Resources || prior.State == "Released" {
				return core.ErrConflict
			}
			result = prior.Placement
			return p.Validate(ctx, result)
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		candidates := make([]NativeCandidate, 0, len(p.Candidates))
		for _, candidate := range p.Candidates {
			n, err := p.Refresh(ctx, candidate)
			if err != nil {
				return err
			}
			suitable := true
			for _, v := range volumes {
				if v.Handle == "" {
					err = p.Kube.CheckPendingTopology(ctx, v.PVC, n.KubeNode)
				} else {
					err = p.Kube.CheckTopology(ctx, v, n.KubeNode)
				}
				if errors.Is(err, kube.ErrTopology) || errors.Is(err, core.ErrPending) {
					suitable = false
					break
				}
				if err != nil {
					return err
				}
			}
			if suitable {
				candidates = append(candidates, n)
			}
		}
		for len(candidates) > 0 {
			candidate, err := p.Choose(candidates)
			if err != nil {
				return err
			}
			index := -1
			for i, n := range candidates {
				if n.ID == candidate.ID {
					index = i
					candidate = n
					break
				}
			}
			if index < 0 {
				return errors.New("native strategy returned a node outside candidates")
			}
			admitted := false
			err = p.Ledger.Exclusive(ctx, "agentenv/capacity/"+candidate.ID, func(ctx context.Context) error {
				// Refresh inside the capacity lock: a stale pre-lock heartbeat must not
				// erase reservations committed concurrently on this node.
				n, err := p.Refresh(ctx, candidate)
				if err != nil {
					return err
				}
				if n.NodeUID != candidate.NodeUID || n.BootID != candidate.BootID {
					return core.ErrConflict
				}
				resources, err := p.effectiveResources(n)
				if err != nil {
					return err
				}
				ids := n.SandboxIDs
				if ids == nil {
					ids = []string{}
				}
				var cpu, memory int64
				err = p.Ledger.Session(ctx).QueryRow(ctx, `SELECT COALESCE(sum(cpu),0),COALESCE(sum(memory_mib),0) FROM agentenv_native_reservations WHERE node=$1 AND state<>'Released' AND (native_id='' OR NOT(native_id=ANY($2::text[])))`, n.ID, ids).Scan(&cpu, &memory)
				if err != nil {
					return err
				}
				if !fitsNativeCapacity(n.Capacity, n.Allocated, Resources{CPU: cpu, MemoryMiB: memory}, resources) {
					return nil
				}
				result = core.Placement{ID: id, Node: n.KubeNode, NodeUID: n.NodeUID, BootID: n.BootID, Expires: time.Now().Add(5 * time.Minute)}
				r := NativeReservation{Placement: result, Candidate: n, RequestedResources: p.Resources, Resources: resources, State: "Reserved"}
				raw, err := json.Marshal(r)
				if err != nil {
					return err
				}
				_, err = p.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_reservations(id,digest,node,cpu,memory_mib,descriptor) VALUES($1,$2,$3,$4,$5,$6)`, id, core.Digest(p.Resources), n.ID, resources.CPU, resources.MemoryMiB, raw)
				admitted = err == nil
				return err
			})
			if err != nil {
				return err
			}
			if admitted {
				return nil
			}
			candidates = append(candidates[:index], candidates[index+1:]...)
		}
		return errors.New("no native node has compatible storage and compute capacity")
	})
	return result, err
}
func (p *NativeAdmission) Validate(ctx context.Context, placement core.Placement) error {
	r, err := p.load(ctx, placement.ID)
	if err != nil {
		return err
	}
	if r.State == "Released" || r.Placement.Node != placement.Node || r.Placement.NodeUID != placement.NodeUID || r.Placement.BootID != placement.BootID {
		return core.ErrConflict
	}
	if p.Refresh == nil {
		return errors.New("native identity verifier required")
	}
	n, err := p.Refresh(ctx, r.Candidate)
	if err != nil {
		return err
	}
	if n.NodeUID != placement.NodeUID || n.BootID != placement.BootID {
		return core.ErrConflict
	}
	return nil
}
func (p *NativeAdmission) Renew(ctx context.Context, r core.Placement) (core.Placement, error) {
	if err := p.Validate(ctx, r); err != nil {
		return r, err
	}
	r.Expires = time.Now().Add(5 * time.Minute)
	return r, nil
}
func (p *NativeAdmission) Commit(ctx context.Context, r core.Placement, _ string) error {
	if err := p.Validate(ctx, r); err != nil {
		return err
	}
	_, err := p.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_reservations SET state='Committed',updated=now() WHERE id=$1 AND state<>'Released'`, r.ID)
	return err
}
func (p *NativeAdmission) BindNativeID(ctx context.Context, id, nativeID string) error {
	if nativeID == "" {
		return errors.New("native sandbox identity required")
	}
	tag, err := p.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_reservations SET native_id=$2,updated=now() WHERE id=$1 AND state<>'Released' AND (native_id='' OR native_id=$2)`, id, nativeID)
	if err == nil && tag.RowsAffected() != 1 {
		return fmt.Errorf("native ID association: %w", core.ErrConflict)
	}
	return err
}
func (p *NativeAdmission) Release(ctx context.Context, r core.Placement) error {
	_, err := p.Ledger.Session(ctx).Exec(ctx, `UPDATE agentenv_native_reservations SET state='Released',updated=now() WHERE id=$1`, r.ID)
	return err
}

func (p *NativeAdmission) effectiveResources(n NativeCandidate) (Resources, error) {
	resources := p.Resources
	if resources.CPU == 0 {
		resources.CPU = n.Defaults.CPU
	}
	if resources.MemoryMiB == 0 {
		resources.MemoryMiB = n.Defaults.MemoryMiB
	}
	if resources.CPU <= 0 || resources.MemoryMiB < 128 {
		return resources, errors.New("native node returned invalid default resources")
	}
	return resources, nil
}

func fitsNativeCapacity(capacity, allocated, pending, request Resources) bool {
	fits := func(cap, used, reserved, want int64) bool {
		if cap < 0 || used < 0 || reserved < 0 || want <= 0 || used > cap {
			return false
		}
		remaining := cap - used
		if reserved > remaining {
			return false
		}
		return want <= remaining-reserved
	}
	return fits(capacity.CPU, allocated.CPU, pending.CPU, request.CPU) && fits(capacity.MemoryMiB, allocated.MemoryMiB, pending.MemoryMiB, request.MemoryMiB)
}
