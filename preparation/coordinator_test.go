package preparation

import (
	"encoding/json"
	"testing"

	api "ssf.dev/ssf/api/ssf/v1"
	"ssf.dev/ssf/core"
	"ssf.dev/ssf/ledger"
)

func TestReceiptRequiresExactPreparationAndRelease(t *testing.T) {
	pvc := core.PVCRef{Namespace: "n", Name: "p", UID: "uid"}
	plan := core.Plan{Purpose: core.PurposePreparation, Operation: "op", NodeUID: "node", BootID: "boot",
		Bindings: []core.Binding{{ID: "binding", Epoch: 2, Volume: core.Volume{PVC: pvc}}}}
	record := ledger.PreparationRecord{PVC: pvc, Operation: "op", Plan: plan}
	receipt := core.PreparationReceipt{Protocol: core.PreparationProtocol, Operation: "op", PlanDigest: core.PreparationDigest(plan), BindingID: "binding", VolumeUID: "uid", NodeUID: "node", BootID: "boot", Epoch: 2, UUID: "fs"}
	response := func(state string, r core.PreparationReceipt) *api.VolumePreparation {
		raw, _ := json.Marshal(r)
		return &api.VolumePreparation{OperationId: "op", State: state, Protocol: core.PreparationProtocol, ReceiptJson: string(raw)}
	}
	if _, err := readReceipt(response("Prepared", receipt), record, false); err != nil {
		t.Fatal(err)
	}
	if _, err := readReceipt(response("Failed", receipt), record, false); err == nil {
		t.Fatal("failed state accepted")
	}
	if _, err := readReceipt(response("Released", receipt), record, true); err == nil {
		t.Fatal("release without process/FD evidence accepted")
	}
	receipt.Released = true
	if _, err := readReceipt(response("Released", receipt), record, true); err != nil {
		t.Fatal(err)
	}
	receipt.Epoch--
	if _, err := readReceipt(response("Released", receipt), record, true); err == nil {
		t.Fatal("stale epoch accepted")
	}
	record.Plan.Bindings = nil
	if _, err := readReceipt(response("Released", receipt), record, true); err == nil {
		t.Fatal("missing binding accepted")
	}
}
