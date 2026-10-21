package runtime

import (
	"context"
	"ssf.dev/ssf/core"
)

// VolumePreparation is the first half of a runtime adapter. CSI publishing,
// locking and release remain in the public Node Agent.
type VolumePreparation interface {
	PrepareVolume(context.Context, PreparationRequest) (core.PreparationReceipt, error)
	InspectVolumePreparation(context.Context, PreparationRequest) (core.PreparationReceipt, error)
	ReleasePreparation(context.Context, PreparationRequest) (core.PreparationReceipt, error)
}

// VolumePreparationPlanner describes immutable runtime requirements and checks
// content intent before the public coordinator grants device ownership. It
// performs no Kubernetes, CSI or device operation.
type VolumePreparationPlanner interface {
	Describe(context.Context) ([]core.VolumeProfile, error)
	Plan(context.Context, core.PreparationSpec) (core.PreparationSpec, error)
}

type PreparationRequest struct {
	Operation  string               `json:"operation"`
	PlanDigest string               `json:"planDigest"`
	Spec       core.PreparationSpec `json:"spec"`
	Endpoint   core.Endpoint        `json:"endpoint"`
}
