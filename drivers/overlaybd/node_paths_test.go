package overlaybd

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestNodePathClaimsPersistAndRejectReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.db")
	s, err := OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	claim := NodePath{Path: "/private/node/stage/session", SessionID: "session", Handle: "handle", Kind: "stage"}
	if err = s.ClaimPath(claim); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenSessions(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.ClaimPath(claim); err != nil {
		t.Fatal(err)
	}
	got, err := s.Path(claim.Path)
	if err != nil || got != claim {
		t.Fatalf("lost claim: %+v %v", got, err)
	}
	claim.SessionID = "other"
	if err = s.ClaimPath(claim); !errors.Is(err, ErrConflict) {
		t.Fatalf("path reassigned: %v", err)
	}
	claim.Path = "/private/../untrusted"
	if err = s.ClaimPath(claim); err == nil {
		t.Fatal("unclean path accepted")
	}
}
