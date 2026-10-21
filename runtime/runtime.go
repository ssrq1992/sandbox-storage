// Package runtime contains project integration contracts, not VMM implementations.
package runtime

import (
	"context"
	"errors"
	"ssf.dev/ssf/core"
)

var ErrUnsupported = errors.New("runtime capability not implemented")

type Capabilities struct{ FileTree, BlockDevice, Freeze, OnlineExpand bool }
type Adapter interface {
	Capabilities(context.Context) (Capabilities, error)
	AttachPrepared(context.Context, string, []core.Endpoint) error
	WaitGuestReady(context.Context, string) error
	DetachAndConfirm(context.Context, string) error
	InspectBindings(context.Context, string) ([]string, error)
	Freeze(context.Context, string) error
	Thaw(context.Context, string) error
}
type PlacementProvider interface {
	Reserve(context.Context, string, []core.Volume) (core.Placement, error)
	Validate(context.Context, core.Placement) error
	Renew(context.Context, core.Placement) (core.Placement, error)
	Commit(context.Context, core.Placement, string) error
	Release(context.Context, core.Placement) error
}

// EvidenceVerifier is implemented by a trusted runtime integration; opaque caller
// strings alone must never authorize unmount or be interpreted as fencing.
type EvidenceVerifier interface {
	Verify(context.Context, string, string, string) error
}
