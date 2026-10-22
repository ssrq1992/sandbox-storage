package auth

import (
	"context"
	"google.golang.org/grpc/metadata"
	"testing"
)

func TestNodeContextReplacesPrivateMetadata(t *testing.T) {
	old := metadata.Pairs(NodePlanMetadata, "forged", NodePlanMetadata, "duplicate", NodeBindingMetadata, "wrong", "trace", "keep")
	ctx := WithNodePlan(metadata.NewOutgoingContext(context.Background(), old), "signed", "binding")
	got, _ := metadata.FromOutgoingContext(ctx)
	if len(got.Get(NodePlanMetadata)) != 1 || got.Get(NodePlanMetadata)[0] != "signed" || len(got.Get(NodeBindingMetadata)) != 1 || got.Get(NodeBindingMetadata)[0] != "binding" || got.Get("trace")[0] != "keep" {
		t.Fatal(got)
	}
	if len(old.Get(NodePlanMetadata)) != 2 {
		t.Fatal("mutated caller metadata")
	}
}
