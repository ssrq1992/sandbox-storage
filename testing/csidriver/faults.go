package csidriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"
	"ssf.dev/ssf/core"
	"strings"
	"sync"
	"time"
)

type FaultRule struct {
	Method  string `json:"method"`
	Phase   string `json:"phase"`
	Count   int    `json:"count"`
	DelayMS int64  `json:"delay_ms,omitempty"`
	Code    string `json:"code,omitempty"`
}
type FaultPlan struct {
	Version int         `json:"version"`
	ID      string      `json:"id"`
	Rules   []FaultRule `json:"rules"`
}
type faultState struct {
	ID     string         `json:"id"`
	Digest string         `json:"digest"`
	Counts map[string]int `json:"counts"`
}

// Faults exists only in the disposable CSI fixture. It has no network control
// endpoint and never logs RPC request bodies, handles, credentials or secrets.
type Faults struct {
	File string
	mu   sync.Mutex
}

func (f *Faults) consume(method, phase string) (*FaultRule, error) {
	if f.File == "" {
		return nil, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, err := os.ReadFile(f.File)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) > 65536 {
		return nil, errors.New("fault plan too large")
	}
	var plan FaultPlan
	if err = json.Unmarshal(raw, &plan); err != nil {
		return nil, err
	}
	if plan.Version != 1 || len(plan.ID) == 0 || len(plan.ID) > 128 || len(plan.Rules) > 64 {
		return nil, errors.New("invalid fault plan")
	}
	digest := core.Digest(plan)
	state := faultState{ID: plan.ID, Digest: digest, Counts: map[string]int{}}
	if prior, e := os.ReadFile(f.File + ".state"); e == nil {
		var loaded faultState
		if e = json.Unmarshal(prior, &loaded); e != nil {
			return nil, e
		}
		if loaded.ID == plan.ID {
			if loaded.Digest != digest {
				return nil, errors.New("fault plan ID reused with different rules")
			}
			if loaded.Counts == nil {
				return nil, errors.New("invalid fault counters")
			}
			state = loaded
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	for index, rule := range plan.Rules {
		if !strings.HasPrefix(rule.Method, "/csi.v1.") || (rule.Phase != "before" && rule.Phase != "after") || rule.Count < 1 || rule.Count > 1000 || rule.DelayMS < 0 || rule.DelayMS > 300000 {
			return nil, errors.New("invalid CSI fault rule")
		}
		if _, err = faultCode(rule.Code); err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%d/%s", index, phase)
		if rule.Method != method || rule.Phase != phase || state.Counts[key] >= rule.Count {
			continue
		}
		state.Counts[key]++
		data, _ := json.Marshal(state)
		tmp, err := os.CreateTemp(filepath.Dir(f.File), ".ssf-fault-state-")
		if err != nil {
			return nil, err
		}
		defer os.Remove(tmp.Name())
		if _, err = tmp.Write(data); err == nil {
			err = tmp.Sync()
		}
		closeErr := tmp.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if err = os.Rename(tmp.Name(), f.File+".state"); err != nil {
			return nil, err
		}
		directory, err := os.Open(filepath.Dir(f.File))
		if err != nil {
			return nil, err
		}
		err = directory.Sync()
		_ = directory.Close()
		if err != nil {
			return nil, err
		}
		evidence, err := os.OpenFile(f.File+".events", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		err = json.NewEncoder(evidence).Encode(map[string]any{"time": time.Now().UTC(), "id": plan.ID, "method": method, "phase": phase, "count": state.Counts[key]})
		if err == nil {
			err = evidence.Sync()
		}
		_ = evidence.Close()
		if err != nil {
			return nil, err
		}
		return &rule, nil
	}
	return nil, nil
}
func faultCode(name string) (codes.Code, error) {
	switch name {
	case "", "Unavailable":
		return codes.Unavailable, nil
	case "Aborted":
		return codes.Aborted, nil
	case "DeadlineExceeded":
		return codes.DeadlineExceeded, nil
	case "Internal":
		return codes.Internal, nil
	}
	return codes.Unknown, errors.New("unsupported fixture fault status")
}
func (f *Faults) inject(ctx context.Context, method, phase string) error {
	rule, err := f.consume(method, phase)
	if err != nil {
		return status.Error(codes.FailedPrecondition, "fixture fault plan/state invalid")
	}
	if rule == nil {
		return nil
	}
	if rule.DelayMS > 0 {
		timer := time.NewTimer(time.Duration(rule.DelayMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
	}
	code, _ := faultCode(rule.Code)
	return status.Error(code, "injected disposable-fixture CSI fault")
}
func (f *Faults) Unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := f.inject(ctx, info.FullMethod, "before"); err != nil {
		return nil, err
	}
	response, err := handler(ctx, req)
	if err != nil {
		return response, err
	}
	if err = f.inject(ctx, info.FullMethod, "after"); err != nil {
		return nil, err
	}
	return response, nil
}
