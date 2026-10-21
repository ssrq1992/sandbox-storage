package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"ssf.dev/ssf/auth"
)

type NativeCommand struct {
	Action  string          `json:"action"`
	NodeID  string          `json:"node_id"`
	OwnerID string          `json:"owner_id,omitempty"`
	RunID   string          `json:"run_id,omitempty"`
	Data    json.RawMessage `json:"data"`
}

// InvokeNativeControl carries the versioned opaque envelope over the native
// StorageControl RPC. Both wire messages consist of field 1 (bytes); BytesValue
// avoids linking AgentENV's entire generated descriptor/dependency graph into
// the sidecar. The native protocol compatibility test locks this wire contract.
func InvokeNativeControl(ctx context.Context, conn grpc.ClientConnInterface, cfg Config, command NativeCommand) (json.RawMessage, error) {
	key, err := os.ReadFile(cfg.NodeControlKeyFile)
	if err != nil {
		return nil, err
	}
	requestID := newID()
	body, err := auth.SignJSONEnvelope([]byte(strings.TrimSpace(string(key))), "agentenv.storage.v2", "node", requestID, command, time.Now())
	if err != nil {
		return nil, err
	}
	var reply wrapperspb.BytesValue
	if err = conn.Invoke(ctx, "/scheduler.v1.StorageControl/Invoke", &wrapperspb.BytesValue{Value: body}, &reply); err != nil {
		return nil, err
	}
	key, err = os.ReadFile(cfg.DispatchKeyFile)
	if err != nil {
		return nil, err
	}
	envelope, err := auth.VerifyJSONEnvelope([]byte(strings.TrimSpace(string(key))), reply.Value, "agentenv.storage.v2", "scheduler", time.Now())
	if err != nil {
		return nil, err
	}
	if envelope.RequestID != requestID {
		return nil, errors.New("storage response correlation mismatch")
	}
	return envelope.Payload, nil
}
