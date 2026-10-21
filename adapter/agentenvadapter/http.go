package agentenvadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type HTTPClient struct {
	Client *http.Client
	APIKey string
}
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body) }
func (h *HTTPClient) Call(ctx context.Context, method, url string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, e := json.Marshal(in)
		if e != nil {
			return e
		}
		body = bytes.NewReader(b)
	}
	q, e := http.NewRequestWithContext(ctx, method, url, body)
	if e != nil {
		return e
	}
	q.Header.Set("Content-Type", "application/json")
	if h.APIKey != "" {
		q.Header.Set("X-API-Key", h.APIKey)
	}
	r, e := h.Client.Do(q)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	b, e := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if e != nil {
		return e
	}
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return &HTTPError{r.StatusCode, string(b)}
	}
	if out != nil && len(b) > 0 {
		return json.Unmarshal(b, out)
	}
	return nil
}
func decode(w http.ResponseWriter, r *http.Request, out any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(out); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("one JSON document required")
	}
	return nil
}
func respond(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, code int, e error) {
	slog.Warn("storage control request failed", "status", code, "error", e.Error())
	respond(w, code, map[string]string{"error": e.Error()})
}
func identity(r *http.Request) string {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
		return ""
	}
	uris := r.TLS.VerifiedChains[0][0].URIs
	if len(uris) != 1 {
		return ""
	}
	return uris[0].String()
}
func allowed(p string, values []string) bool {
	if p == "" {
		return false
	}
	for _, v := range values {
		if p == v {
			return true
		}
	}
	return false
}
func httpServer(addr string, h http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
}
func base(s string) string { return strings.TrimRight(s, "/") }
