package auth

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// PeerAllowlist checks a URI SAN against an explicit role-specific allowlist.
func PeerAllowlist(allowed []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		p, ok := peer.FromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing peer")
		}
		t, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok || len(t.State.VerifiedChains) == 0 {
			return nil, status.Error(codes.Unauthenticated, "verified mTLS required")
		}
		for _, u := range t.State.VerifiedChains[0][0].URIs {
			for _, a := range allowed {
				if u.String() == a {
					return next(ctx, req)
				}
			}
		}
		return nil, status.Error(codes.PermissionDenied, "peer is not authorized for this service")
	}
}

var ErrEvidence = errors.New("runtime evidence verification failed")
