package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"ssf.dev/ssf/core"
	"strings"
)

// LocalPreparation uses an administrator-configured Pod loopback endpoint.
// The token is separate from public API credentials and is never in a plan.
type LocalPreparation struct{ URL, TokenFile string }

func (p *LocalPreparation) call(ctx context.Context, action string, q PreparationRequest) (core.PreparationReceipt, error) {
	var result core.PreparationReceipt
	u, err := url.Parse(p.URL)
	if err != nil || u.Scheme != "http" || net.ParseIP(u.Hostname()) == nil || !net.ParseIP(u.Hostname()).IsLoopback() {
		return result, errors.New("preparation endpoint must be Pod loopback HTTP")
	}
	key, err := os.ReadFile(p.TokenFile)
	if err != nil {
		return result, err
	}
	token := strings.TrimSpace(string(key))
	if len(token) < 32 {
		return result, errors.New("invalid preparation credential")
	}
	body, err := json.Marshal(struct {
		Action  string             `json:"action"`
		Request PreparationRequest `json:"request"`
	}{action, q})
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", p.URL, bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("preparation redirect forbidden") }}
	resp, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, errors.New("runtime volume preparation rejected")
	}
	err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result)
	return result, err
}
func (p *LocalPreparation) PrepareVolume(ctx context.Context, q PreparationRequest) (core.PreparationReceipt, error) {
	return p.call(ctx, "prepare", q)
}
func (p *LocalPreparation) InspectVolumePreparation(ctx context.Context, q PreparationRequest) (core.PreparationReceipt, error) {
	return p.call(ctx, "inspect", q)
}
func (p *LocalPreparation) ReleasePreparation(ctx context.Context, q PreparationRequest) (core.PreparationReceipt, error) {
	return p.call(ctx, "release", q)
}
