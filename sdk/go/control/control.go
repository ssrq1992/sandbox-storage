// Package control is the dependency-free ssf.control.v1 wire client. It has no
// Kubernetes, database, CSI or runtime implementation dependency.
package control

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

const Version = "ssf.control.v1"
const MaxBytes = 4 << 20

type Resources struct {
	CPU       int64 `json:"cpu"`
	MemoryMiB int64 `json:"memory_mib"`
}
type Node struct {
	ID         string    `json:"id"`
	Endpoint   string    `json:"endpoint"`
	Capacity   Resources `json:"capacity"`
	Allocated  Resources `json:"allocated"`
	SandboxIDs []string  `json:"sandbox_ids"`
}
type Request struct {
	Version    string `json:"version"`
	Runtime    string `json:"runtime"`
	LaunchID   string `json:"launch_id,omitempty"`
	Envelope   []byte `json:"envelope,omitempty"`
	Candidates []Node `json:"candidates,omitempty"`
	Selected   string `json:"selected,omitempty"`
}
type Response struct {
	Version    string    `json:"version"`
	LaunchID   string    `json:"launch_id,omitempty"`
	State      string    `json:"state,omitempty"`
	Resources  Resources `json:"resources,omitempty"`
	Candidates []Node    `json:"candidates,omitempty"`
	Selected   *Node     `json:"selected,omitempty"`
	Dispatch   []byte    `json:"dispatch,omitempty"`
	Envelope   []byte    `json:"envelope,omitempty"`
}
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

type Config struct {
	Endpoint      string `json:"endpoint"`
	Cert          string `json:"cert"`
	Key           string `json:"key"`
	CA            string `json:"ca"`
	ServerName    string `json:"server_name,omitempty"`
	Peer          string `json:"peer"`
	ObserveListen string `json:"observe_listen,omitempty"`
}

func Load(path string) (Config, error) {
	var c Config
	b, e := os.ReadFile(path)
	if e == nil {
		e = json.Unmarshal(b, &c)
	}
	return c, e
}
func TLS(c Config, server bool) (*tls.Config, error) {
	pair, e := tls.LoadX509KeyPair(c.Cert, c.Key)
	if e != nil {
		return nil, e
	}
	pem, e := os.ReadFile(c.CA)
	if e != nil {
		return nil, e
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("invalid control CA")
	}
	t := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, RootCAs: roots, ServerName: c.ServerName}
	if server {
		t.ClientCAs = roots
		t.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		if c.Peer == "" {
			return nil, errors.New("expected control peer URI required")
		}
		t.VerifyConnection = func(s tls.ConnectionState) error {
			if !HasPeer(&s, []string{c.Peer}) {
				return errors.New("unexpected control peer")
			}
			return nil
		}
	}
	return t, nil
}
func HasPeer(s *tls.ConnectionState, peers []string) bool {
	if s == nil || len(s.VerifiedChains) == 0 {
		return false
	}
	for _, u := range s.VerifiedChains[0][0].URIs {
		for _, p := range peers {
			if p != "" && u.String() == p {
				return true
			}
		}
	}
	return false
}

type Client struct {
	endpoint string
	HTTP     *http.Client
}

func New(c Config) (*Client, error) {
	u, e := url.Parse(c.Endpoint)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("control endpoint must be an HTTPS origin")
	}
	t, e := TLS(c, false)
	if e != nil {
		return nil, e
	}
	return &Client{endpoint: u.Scheme + "://" + u.Host, HTTP: &http.Client{Transport: &http.Transport{TLSClientConfig: t}, Timeout: 6 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("control redirect forbidden") }}}, nil
}
func (c *Client) Close() { c.HTTP.CloseIdleConnections() }
func (c *Client) Call(ctx context.Context, method string, q Request) (Response, error) {
	var out Response
	q.Version = Version
	b, e := json.Marshal(q)
	if e != nil {
		return out, e
	}
	if len(b) > MaxBytes {
		return out, errors.New("control request too large")
	}
	r, e := http.NewRequestWithContext(ctx, "POST", c.endpoint+"/"+Version+"/"+method, bytes.NewReader(b))
	if e != nil {
		return out, e
	}
	r.Header.Set("Content-Type", "application/json")
	res, e := c.HTTP.Do(r)
	if e != nil {
		return out, e
	}
	defer res.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(res.Body, MaxBytes+1))
	if e != nil {
		return out, e
	}
	if len(raw) > MaxBytes {
		return out, errors.New("control response too large")
	}
	if res.StatusCode != 200 {
		var remote Error
		if json.Unmarshal(raw, &remote) != nil || remote.Code == "" {
			return out, fmt.Errorf("control HTTP %d", res.StatusCode)
		}
		return out, &remote
	}
	if e = json.Unmarshal(raw, &out); e != nil {
		return out, e
	}
	if out.Version != Version {
		return out, errors.New("incompatible control protocol")
	}
	return out, nil
}
func Decode(w http.ResponseWriter, r *http.Request) (Request, error) {
	var q Request
	if r.Method != "POST" {
		return q, errors.New("POST required")
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBytes))
	d.DisallowUnknownFields()
	if e := d.Decode(&q); e != nil {
		return q, e
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return q, errors.New("trailing control input")
	}
	if q.Version != Version {
		return q, errors.New("incompatible control protocol")
	}
	return q, nil
}
func Write(w http.ResponseWriter, out Response, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		var typed *Error
		if !errors.As(err, &typed) {
			typed = &Error{Code: "pending", Message: err.Error()}
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(typed)
		return
	}
	out.Version = Version
	_ = json.NewEncoder(w).Encode(out)
}
