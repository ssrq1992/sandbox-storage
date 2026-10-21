package agentenvadapter

import (
	"context"
	"testing"
)

func TestNativeStopReplacementRequiresPhysicalBootFence(t *testing.T) {
	previous := NativeCandidate{ID: "old-pod", KubeNode: "worker", NodeUID: "physical", BootID: "old-boot", PluginURL: "https://old"}
	for _, tc := range []struct {
		name, id, uid, boot string
		ok                  bool
	}{
		{"same pod", "old-pod", "physical", "old-boot", true},
		{"reboot replacement", "new-pod", "physical", "new-boot", true},
		{"pod restart is not host fence", "new-pod", "physical", "old-boot", false},
		{"reused node name", "new-pod", "replacement", "new-boot", false},
		{"missing boot", "new-pod", "physical", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NativeControl{Refresh: func(_ context.Context, candidate NativeCandidate) (NativeCandidate, error) {
				candidate.NodeUID = tc.uid
				candidate.KubeNode = "worker"
				candidate.BootID = tc.boot
				candidate.PluginURL = "https://current"
				return candidate, nil
			}}
			current, err := c.stopCandidate(context.Background(), previous, tc.id)
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%v error=%v", tc.ok, err)
			}
			if tc.ok && current.PluginURL != "https://current" {
				t.Fatal("used stale Pod endpoint")
			}
			if previous.BootID != "old-boot" {
				t.Fatal("rewrote signed identity")
			}
		})
	}
}

func TestNativeStopDoesNotRequireSchedulingHealth(t *testing.T) {
	previous := NativeCandidate{ID: "draining-pod", KubeNode: "worker", NodeUID: "physical", BootID: "boot"}
	c := NativeControl{
		Refresh: func(context.Context, NativeCandidate) (NativeCandidate, error) {
			t.Fatal("cleanup used scheduling eligibility")
			return NativeCandidate{}, nil
		},
		RefreshStopped: func(context.Context, NativeCandidate) (NativeCandidate, error) {
			current := previous
			current.PluginURL = "https://draining"
			return current, nil
		},
	}
	current, err := c.stopCandidate(context.Background(), previous, previous.ID)
	if err != nil || current.PluginURL != "https://draining" {
		t.Fatalf("draining cleanup rejected: %+v %v", current, err)
	}
}
