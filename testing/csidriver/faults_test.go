package csidriver

import (
	"context"
	"encoding/json"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFixtureFaultAfterSideEffectIsDurableAndDoesNotRepeatCreate(t *testing.T) {
	file := filepath.Join(t.TempDir(), "fault.json")
	plan := FaultPlan{Version: 1, ID: "after-create", Rules: []FaultRule{{Method: "/csi.v1.Controller/CreateVolume", Phase: "after", Count: 1}}}
	raw, _ := json.Marshal(plan)
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	driver := &Driver{Root: t.TempDir(), Mode: "file"}
	request := &csi.CreateVolumeRequest{Name: "exact-id", VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}}}, Secrets: map[string]string{"password": "never-log-this"}}
	handler := func(ctx context.Context, req any) (any, error) {
		return driver.CreateVolume(ctx, req.(*csi.CreateVolumeRequest))
	}
	info := &grpc.UnaryServerInfo{FullMethod: plan.Rules[0].Method}
	first := &Faults{File: file}
	if _, err := first.Unary(context.Background(), request, info, handler); status.Code(err) != codes.Unavailable {
		t.Fatalf("side effect did not become uncertain: %v", err)
	}
	restarted := &Faults{File: file}
	result, err := restarted.Unary(context.Background(), request, info, handler)
	if err != nil {
		t.Fatal(err)
	}
	if result.(*csi.CreateVolumeResponse).Volume.VolumeId == "" {
		t.Fatal("retry omitted existing volume")
	}
	files, _ := filepath.Glob(filepath.Join(driver.Root, "*.json"))
	if len(files) != 1 {
		t.Fatalf("duplicate volume: %v", files)
	}
	evidence, _ := os.ReadFile(file + ".events")
	if strings.Contains(string(evidence), "never-log-this") {
		t.Fatal("fault evidence exposed a secret")
	}
}
func TestFixtureFaultBeforeCallDoesNotApplyBackendMutation(t *testing.T) {
	file := filepath.Join(t.TempDir(), "fault.json")
	raw, _ := json.Marshal(FaultPlan{Version: 1, ID: "before", Rules: []FaultRule{{Method: "/csi.v1.Node/NodePublishVolume", Phase: "before", Count: 1}}})
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	effects := 0
	handler := func(context.Context, any) (any, error) { effects++; return nil, nil }
	f := &Faults{File: file}
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Node/NodePublishVolume"}
	if _, err := f.Unary(context.Background(), nil, info, handler); status.Code(err) != codes.Unavailable || effects != 0 {
		t.Fatal("before fault applied mutation", err)
	}
	if _, err := f.Unary(context.Background(), nil, info, handler); err != nil || effects != 1 {
		t.Fatal("retry could not recover", err)
	}
}
