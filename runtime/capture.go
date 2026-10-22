package runtime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"ssf.dev/ssf/core"
)

type CaptureVerifier interface {
	VerifyCapture(context.Context, core.CaptureIntent, []core.Binding, string) error
}

// RemoteCaptureEvidence uses the runtime's existing authenticated capture
// endpoint; only that runtime can verify the VMM barrier and device identities.
type RemoteCaptureEvidence struct{ remote *RemoteEvidence }

func NewRemoteCaptureEvidence(endpoint string, t *tls.Config) (*RemoteCaptureEvidence, error) {
	remote, err := NewRemoteEvidence(endpoint, t)
	if err != nil {
		return nil, err
	}
	return &RemoteCaptureEvidence{remote: remote}, nil
}
func (v *RemoteCaptureEvidence) VerifyCapture(ctx context.Context, intent core.CaptureIntent, bindings []core.Binding, proof string) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if proof == "" || len(bindings) == 0 {
		return errors.New("capture evidence required")
	}
	body, err := json.Marshal(struct {
		Intent   core.CaptureIntent `json:"intent"`
		Bindings []core.Binding     `json:"bindings"`
		Proof    string             `json:"proof"`
	}{intent, bindings, proof})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.remote.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := v.remote.Client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return errors.New("runtime did not verify this capture barrier")
	}
	return nil
}
