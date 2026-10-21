package auth

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"ssf.dev/ssf/core"
	"strings"
	"time"
)

func Sign(p core.Plan, key ed25519.PrivateKey) (string, error) {
	if len(key) != ed25519.PrivateKeySize {
		return "", errors.New("invalid signing key")
	}
	b, e := json.Marshal(p)
	if e != nil {
		return "", e
	}
	return base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, b)), nil
}
func Verify(token string, key ed25519.PublicKey, node, boot string, now time.Time) (core.Plan, error) {
	p, e := verifySignature(token, key)
	if e != nil {
		return p, e
	}
	if p.Node != node || p.BootID != boot || !now.Before(p.Expires) {
		return p, errors.New("plan scope or expiry mismatch")
	}
	return p, nil
}

// VerifyRevocation authenticates a stop-only plan. Expiry and boot changes must
// not prevent a durable tombstone, but physical node identity must still match.
// Callers must additionally verify actual runtime detachment before persisting it.
func VerifyRevocation(token string, key ed25519.PublicKey, node, nodeUID, boot string) (core.Plan, error) {
	p, err := verifySignature(token, key)
	if err != nil {
		return p, err
	}
	if p.Node != node || (nodeUID != "" && p.NodeUID != nodeUID) || (p.BootID != boot && (nodeUID == "" || p.NodeUID != nodeUID)) {
		return p, errors.New("revocation node identity mismatch")
	}
	return p, nil
}

func verifySignature(token string, key ed25519.PublicKey) (core.Plan, error) {
	var p core.Plan
	s := strings.Split(token, ".")
	if len(s) != 2 || len(key) != ed25519.PublicKeySize {
		return p, errors.New("invalid signed plan")
	}
	b, e := base64.RawURLEncoding.DecodeString(s[0])
	if e != nil {
		return p, e
	}
	sig, e := base64.RawURLEncoding.DecodeString(s[1])
	if e != nil || !ed25519.Verify(key, b, sig) {
		return p, errors.New("invalid signature")
	}
	if e = json.Unmarshal(b, &p); e != nil {
		return p, e
	}
	if p.ID == "" || len(p.Bindings) == 0 {
		return p, errors.New("plan scope or expiry mismatch")
	}
	if p.Purpose == core.PurposePreparation {
		if p.Sandbox != "" || p.Preparation == nil || len(p.Bindings) != 1 || p.Operation == "" {
			return p, errors.New("invalid volume preparation scope")
		}
		b := p.Bindings[0]
		if p.Preparation.SourceRevision == "" || p.Preparation.Profile.Validate() != nil || p.Preparation.PVC != b.Volume.PVC || p.Preparation.PVUID != b.Volume.PVUID ||
			b.Sandbox != "" || b.OwnerUID == "" || b.RunUID == "" || b.Epoch == 0 ||
			b.Volume.Capability == nil || b.Volume.Capability.GetBlock() == nil ||
			(p.Preparation.Initialize && (b.Volume.ReadOnly || !p.Preparation.Profile.InitializeBlank)) {
			return p, errors.New("invalid volume preparation identity or capability")
		}
	} else if p.Purpose != "" || p.Sandbox == "" || p.Preparation != nil {
		return p, errors.New("invalid runtime plan purpose")
	}
	if len(p.PreparedVolumes) > 0 {
		if p.Purpose != "" {
			return p, errors.New("preparation cannot consume runtime readiness")
		}
		for id, r := range p.PreparedVolumes {
			found := false
			for _, b := range p.Bindings {
				if b.ID == id && b.Volume.PVC.UID == r.VolumeUID {
					found = true
					break
				}
			}
			if !found || r.Rejected || r.Protocol != core.PreparationProtocol || r.UUID == "" || !r.Released || r.PlanDigest == "" || r.Operation == "" {
				return p, errors.New("invalid prepared content proof")
			}
		}
	}
	return p, nil
}
func TLS(cert, key, ca string, server bool) (*tls.Config, error) {
	pair, e := tls.LoadX509KeyPair(cert, key)
	if e != nil {
		return nil, e
	}
	b, e := os.ReadFile(ca)
	if e != nil {
		return nil, e
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, errors.New("invalid CA")
	}
	c := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, RootCAs: pool}
	if server {
		c.ClientCAs = pool
		c.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return c, nil
}
