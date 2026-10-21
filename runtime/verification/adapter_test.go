package verification

import (
	"context"
	"errors"
	"ssf.dev/ssf/runtime"
	"testing"
)

func TestDoesNotClaimGuestReady(t *testing.T) {
	a := &Adapter{}
	if !errors.Is(a.WaitGuestReady(context.Background(), "vm"), runtime.ErrUnsupported) {
		t.Fatal("verification adapter must not claim actual guest readiness")
	}
}
