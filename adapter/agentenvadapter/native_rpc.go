package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	control "ssf.dev/control"
	"ssf.dev/ssf/auth"
)

type NativeCommand struct {
	Action  string          `json:"action"`
	NodeID  string          `json:"node_id"`
	OwnerID string          `json:"owner_id,omitempty"`
	RunID   string          `json:"run_id,omitempty"`
	Data    json.RawMessage `json:"data"`
}

// InvokeNativeControl calls the independent coordinator over verified mTLS.
func InvokeNativeControl(ctx context.Context, conn *control.Client, cfg Config, command NativeCommand) (json.RawMessage, error) {
	key, err := os.ReadFile(cfg.NodeControlKeyFile)
	if err != nil {
		return nil, err
	}
	requestID := newID()
	body, err := auth.SignJSONEnvelope([]byte(strings.TrimSpace(string(key))), "agentenv.storage.v2", "node", requestID, command, time.Now())
	if err != nil {
		return nil, err
	}
	reply, err := conn.Call(ctx, "InvokeLifecycle", control.Request{Runtime: "agentenv", Envelope: body})
	if err != nil {
		return nil, err
	}
	key, err = os.ReadFile(cfg.DispatchKeyFile)
	if err != nil {
		return nil, err
	}
	envelope, err := auth.VerifyJSONEnvelope([]byte(strings.TrimSpace(string(key))), reply.Envelope, "agentenv.storage.v2", "scheduler", time.Now())
	if err != nil {
		return nil, err
	}
	if envelope.RequestID != requestID {
		return nil, errors.New("storage response correlation mismatch")
	}
	return envelope.Payload, nil
}
