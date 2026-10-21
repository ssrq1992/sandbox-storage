package agentenvadapter

import (
	"math"
	"testing"
)

func TestNativeDefaultsComeFromChosenNode(t *testing.T) {
	node := NativeCandidate{Defaults: Resources{CPU: 4, MemoryMiB: 4096}}
	admission := NativeAdmission{}
	actual, err := admission.effectiveResources(node)
	if err != nil || actual != node.Defaults {
		t.Fatalf("native defaults lost: %+v %v", actual, err)
	}
	admission.Resources.CPU = 2
	actual, err = admission.effectiveResources(node)
	if err != nil || actual.CPU != 2 || actual.MemoryMiB != 4096 {
		t.Fatalf("partial override lost: %+v %v", actual, err)
	}
	node.Defaults.MemoryMiB = 0
	if _, err = admission.effectiveResources(node); err == nil {
		t.Fatal("invented a missing native memory default")
	}
}
func TestNativeCapacityIncludesReservationsWithoutOverflow(t *testing.T) {
	capacity := Resources{CPU: 8, MemoryMiB: 8192}
	if !fitsNativeCapacity(capacity, Resources{2, 1024}, Resources{4, 4096}, Resources{2, 3072}) {
		t.Fatal("exact capacity rejected")
	}
	if fitsNativeCapacity(capacity, Resources{2, 1024}, Resources{4, 4096}, Resources{3, 3072}) {
		t.Fatal("pending reservations ignored")
	}
	if fitsNativeCapacity(capacity, Resources{2, 1024}, Resources{math.MaxInt64, 4096}, Resources{math.MaxInt64, 3072}) {
		t.Fatal("overflow admitted capacity")
	}
	if fitsNativeCapacity(capacity, Resources{-1, 1024}, Resources{}, Resources{1, 128}) {
		t.Fatal("invalid heartbeat admitted")
	}
}
