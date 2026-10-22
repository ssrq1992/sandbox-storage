package auth

import (
	"context"
	"google.golang.org/grpc/metadata"
)

const NodePlanMetadata = "ssf-node-plan"
const NodeBindingMetadata = "ssf-node-binding"

// WithNodePlan replaces these private keys rather than appending caller input.
// Drivers authenticate the token independently before touching a device.
func WithNodePlan(ctx context.Context, token, binding string) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(NodePlanMetadata, token)
	md.Set(NodeBindingMetadata, binding)
	return metadata.NewOutgoingContext(ctx, md)
}
