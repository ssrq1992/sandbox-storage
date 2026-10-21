// Package csi implements standard CSI consumption calls. The separation of
// controller/node clients is inspired by Substrate; this implementation is independent.
package csi

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"strings"
	"sync"
	"time"
)

type Session struct {
	Conn                   *grpc.ClientConn
	Identity               csi.IdentityClient
	Controller             csi.ControllerClient
	Node                   csi.NodeClient
	Name, NodeID, Revision string
	Attach, Stage, Expand  bool
}

func Dial(ctx context.Context, endpoint, name, role, revision string, t *tls.Config) (*Session, error) {
	var opt grpc.DialOption
	if strings.HasPrefix(endpoint, "unix:") {
		opt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		if t == nil {
			return nil, errors.New("remote CSI requires TLS")
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(t))
	}
	c, e := grpc.NewClient(endpoint, opt)
	if e != nil {
		return nil, e
	}
	s := &Session{Conn: c, Identity: csi.NewIdentityClient(c), Controller: csi.NewControllerClient(c), Node: csi.NewNodeClient(c), Revision: revision}
	fail := func(e error) (*Session, error) { c.Close(); return nil, e }
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	i, e := s.Identity.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if e != nil {
		return fail(e)
	}
	if i.Name != name {
		return fail(errors.New("CSI driver identity mismatch"))
	}
	s.Name = i.Name
	p, e := s.Identity.Probe(ctx, &csi.ProbeRequest{})
	if e != nil {
		return fail(e)
	}
	if p.Ready != nil && !p.Ready.Value {
		return fail(errors.New("CSI driver not ready"))
	}
	if role == "node" {
		i, e := s.Node.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
		if e != nil {
			return fail(e)
		}
		if i.NodeId == "" {
			return fail(errors.New("empty CSI node ID"))
		}
		s.NodeID = i.NodeId
		caps, e := s.Node.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
		if e != nil {
			return fail(e)
		}
		for _, c := range caps.Capabilities {
			switch c.GetRpc().GetType() {
			case csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME:
				s.Stage = true
			case csi.NodeServiceCapability_RPC_EXPAND_VOLUME:
				s.Expand = true
			}
		}
	} else if role == "controller" {
		caps, e := s.Controller.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
		if e != nil {
			return fail(e)
		}
		for _, c := range caps.Capabilities {
			if c.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME {
				s.Attach = true
			}
		}
	} else {
		return fail(errors.New("invalid CSI role"))
	}
	return s, nil
}

type Registry struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

func (r *Registry) Get(ctx context.Context, endpoint, name, role, revision string, t *tls.Config) (*Session, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions == nil {
		r.sessions = map[string]*Session{}
	}
	key := name + "/" + role + "/" + revision + "/" + endpoint
	if s := r.sessions[key]; s != nil {
		return s, nil
	}
	s, e := Dial(ctx, endpoint, name, role, revision, t)
	if e == nil {
		r.sessions[key] = s
	}
	return s, e
}
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.sessions {
		s.Conn.Close()
	}
	r.sessions = nil
}
