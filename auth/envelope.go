package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

// JSONEnvelope authenticates a small runtime-specific internal protocol without
// importing that runtime's SDK or its dependency graph into SSF.
type JSONEnvelope struct {
	Version   string          `json:"version"`
	Role      string          `json:"role"`
	RequestID string          `json:"request_id"`
	IssuedAt  int64           `json:"issued_at"`
	Payload   json.RawMessage `json:"payload"`
	Signature string          `json:"signature"`
}

func VerifyJSONEnvelope(key, raw []byte, version, role string, now time.Time) (JSONEnvelope, error) {
	var e JSONEnvelope
	if len(key) < 32 || len(raw) == 0 || len(raw) > 1<<20 {
		return e, errors.New("invalid authenticated envelope")
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, err
	}
	if e.Version != version || e.Role != role || e.RequestID == "" {
		return e, errors.New("envelope identity mismatch")
	}
	age := now.Sub(time.Unix(e.IssuedAt, 0))
	if age < -30*time.Second || age > 5*time.Minute {
		return e, errors.New("envelope expired")
	}
	sig, err := base64.RawURLEncoding.DecodeString(e.Signature)
	if err != nil {
		return e, err
	}
	unsigned := e
	unsigned.Signature = ""
	b, err := json.Marshal(unsigned)
	if err != nil {
		return e, err
	}
	m := hmac.New(sha256.New, key)
	_, _ = m.Write(b)
	if !hmac.Equal(sig, m.Sum(nil)) {
		return e, errors.New("invalid envelope signature")
	}
	return e, nil
}

func SignJSONEnvelope(key []byte, version, role, id string, payload any, now time.Time) ([]byte, error) {
	if len(key) < 32 || version == "" || role == "" || id == "" {
		return nil, errors.New("envelope signing identity required")
	}
	p, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	e := JSONEnvelope{Version: version, Role: role, RequestID: id, IssuedAt: now.Unix(), Payload: p}
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	m := hmac.New(sha256.New, key)
	_, _ = m.Write(raw)
	e.Signature = base64.RawURLEncoding.EncodeToString(m.Sum(nil))
	raw, err = json.Marshal(e)
	if len(raw) > 1<<20 {
		return nil, errors.New("envelope exceeds limit")
	}
	return raw, err
}
