// Package verification is a test-only runtime; it never claims to start a VM.
package verification

import (
	"context"
	"errors"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/runtime"
	"sync"
)

type Adapter struct {
	mu       sync.Mutex
	bindings map[string][]core.Endpoint
}

func (a *Adapter) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{FileTree: true, BlockDevice: true}, nil
}
func (a *Adapter) AttachPrepared(_ context.Context, id string, eps []core.Endpoint) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if id == "" || len(eps) == 0 {
		return errors.New("instance and endpoints required")
	}
	if a.bindings == nil {
		a.bindings = map[string][]core.Endpoint{}
	}
	for _, e := range eps {
		if e.BindingID == "" || e.Identity == "" {
			return errors.New("verified endpoint required")
		}
	}
	a.bindings[id] = append([]core.Endpoint(nil), eps...)
	return nil
}
func (a *Adapter) WaitGuestReady(context.Context, string) error { return runtime.ErrUnsupported }
func (a *Adapter) DetachAndConfirm(_ context.Context, id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.bindings, id)
	return nil
}
func (a *Adapter) InspectBindings(_ context.Context, id string) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, ep := range a.bindings[id] {
		out = append(out, ep.BindingID)
	}
	return out, nil
}
func (a *Adapter) Freeze(context.Context, string) error { return runtime.ErrUnsupported }
func (a *Adapter) Thaw(context.Context, string) error   { return runtime.ErrUnsupported }
