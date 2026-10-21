package agentenvadapter

import (
	"context"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
	driver "ssf.dev/ssf/drivers/csi"
	"ssf.dev/ssf/runtime"
	"time"
)

// Per-operation durations are sums of actual attempts, including retries.
// They do not claim to measure VM boot independently of guest validation.
func elapsed(m map[string]float64, key string, start time.Time) {
	m[key] += float64(time.Since(start)) / float64(time.Millisecond)
}

type timedPlacement struct {
	runtime.PlacementProvider
	M map[string]float64
}

func (p timedPlacement) Reserve(ctx context.Context, id string, v []core.Volume) (core.Placement, error) {
	t := time.Now()
	defer elapsed(p.M, "reservation_ms", t)
	return p.PlacementProvider.Reserve(ctx, id, v)
}

type timedAuth struct {
	consume.Authorizer
	M map[string]float64
}

func (a timedAuth) Authorize(ctx context.Context, t string, r core.PVCRef, w bool) error {
	s := time.Now()
	defer elapsed(a.M, "authorization_ms", s)
	return a.Authorizer.Authorize(ctx, t, r, w)
}

type timedDrivers struct {
	consume.Controllers
	M map[string]float64
}

func (d timedDrivers) Controller(ctx context.Context, n string) (*driver.Session, error) {
	s, e := d.Controllers.Controller(ctx, n)
	if e != nil {
		return nil, e
	}
	copy := *s
	copy.Controller = timedController{s.Controller, d.M}
	return &copy, nil
}

type timedController struct {
	csi.ControllerClient
	M map[string]float64
}

func (c timedController) ControllerPublishVolume(ctx context.Context, q *csi.ControllerPublishVolumeRequest, opts ...grpc.CallOption) (*csi.ControllerPublishVolumeResponse, error) {
	t := time.Now()
	defer elapsed(c.M, "attach_ms", t)
	return c.ControllerClient.ControllerPublishVolume(ctx, q, opts...)
}
