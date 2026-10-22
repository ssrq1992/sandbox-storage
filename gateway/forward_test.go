package gateway

import (
	"context"
	"google.golang.org/grpc/metadata"
	"testing"
)

func TestForwardOnlyAuthorizedBindingMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("ssf-binding-id", "binding", "authorization", "secret", "unrelated", "value"))
	outgoing, _ := metadata.FromOutgoingContext(forwardBinding(ctx))
	if len(outgoing.Get("ssf-binding-id")) != 1 || outgoing.Get("ssf-binding-id")[0] != "binding" || len(outgoing.Get("authorization")) != 0 || len(outgoing.Get("unrelated")) != 0 {
		t.Fatal("incorrect driver metadata forwarding")
	}
}
