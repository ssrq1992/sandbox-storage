package runtime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"
)

// RemoteEvidence asks the registered project runtime to inspect actual process
// and device state. The runtime endpoint is administrator configured, never user input.
type RemoteEvidence struct {
	URL    string
	Client *http.Client
}

func NewRemoteEvidence(endpoint string, t *tls.Config) (*RemoteEvidence, error) {
	u, e := url.Parse(endpoint)
	if e != nil || u.Scheme != "https" || t == nil {
		return nil, errors.New("trusted HTTPS runtime verifier required")
	}
	return &RemoteEvidence{endpoint, &http.Client{Transport: &http.Transport{TLSClientConfig: t}, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (r *RemoteEvidence) Verify(ctx context.Context, sandbox, action, evidence string) error {
	b, _ := json.Marshal(map[string]string{"sandbox": sandbox, "action": action, "evidence": evidence})
	q, e := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(b))
	if e != nil {
		return e
	}
	q.Header.Set("Content-Type", "application/json")
	resp, e := r.Client.Do(q)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return errors.New("runtime did not confirm actual state")
	}
	return nil
}
