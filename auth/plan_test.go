package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"ssf.dev/ssf/core"
	"testing"
	"time"
)

func TestPlanScope(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	p := core.Plan{ID: "p", Operation: "o", Node: "n", BootID: "b", Sandbox: "s", Expires: time.Now().Add(time.Minute), Bindings: []core.Binding{{ID: "binding"}}}
	token, e := Sign(p, priv)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = Verify(token, pub, "n", "b", time.Now()); e != nil {
		t.Fatal(e)
	}
	for _, scope := range [][2]string{{"other", "b"}, {"n", "newboot"}} {
		if _, e = Verify(token, pub, scope[0], scope[1], time.Now()); e == nil {
			t.Fatal("scope bypass")
		}
	}
	if _, e = Verify(token, pub, "n", "b", time.Now().Add(time.Hour)); e == nil {
		t.Fatal("expiry bypass")
	}
	if _, e = Verify("x"+token, pub, "n", "b", time.Now()); e == nil {
		t.Fatal("signature bypass")
	}
}
