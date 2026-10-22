package core

import (
	"errors"
	"time"
)

const (
	PurposePreparation  = "VolumePreparation"
	PreparationProtocol = "volume-preparation-v1"
)

// VolumeProfile describes runtime content, never backend provisioning parameters.
type VolumeProfile struct {
	Name            string `json:"name"`
	Runtime         string `json:"runtime"`
	Version         string `json:"version"`
	VolumeMode      string `json:"volumeMode"`
	Filesystem      string `json:"filesystem"`
	InitializeBlank bool   `json:"initializeBlank"`
	// The administrator asserts that this profile's CSI Controller acquires
	// the public offline fence before changing content, and completes the FS.
	ControllerOfflineExpansion bool `json:"controllerOfflineExpansion,omitempty"`
}

func (p VolumeProfile) Validate() error {
	if p.Name == "" || p.Runtime == "" || p.Version != PreparationProtocol || p.VolumeMode != "Block" || p.Filesystem == "" {
		return errors.New("invalid volume preparation profile")
	}
	return nil
}

// PreparationSpec is pinned before any device mutation and included in the
// signed plan. SourceRevision includes the actual PVC dataSource identities.
type PreparationSpec struct {
	Profile        VolumeProfile `json:"profile"`
	PVC            PVCRef        `json:"pvc"`
	PVUID          string        `json:"pvUID"`
	SourceRevision string        `json:"sourceRevision"`
	Initialize     bool          `json:"initialize"`
	ExpectedUUID   string        `json:"expectedUUID,omitempty"`
}

type PreparationReceipt struct {
	Protocol       string `json:"protocol"`
	Operation      string `json:"operation"`
	PlanDigest     string `json:"planDigest"`
	BindingID      string `json:"bindingID"`
	VolumeUID      string `json:"volumeUID"`
	NodeUID        string `json:"nodeUID"`
	BootID         string `json:"bootID"`
	Epoch          uint64 `json:"epoch"`
	UUID           string `json:"uuid"`
	DeviceIdentity string `json:"deviceIdentity"`
	Released       bool   `json:"released"`
	Rejected       bool   `json:"rejected,omitempty"`
}

// PreparationDigest remains stable when a signed plan is renewed.
func PreparationDigest(p Plan) string {
	p.ID = ""
	p.Expires = time.Time{}
	return Digest(p)
}

// PreparationReboot permits the same exclusive operation to resume on the
// same physical node after a boot change. Nothing except boot and token
// lifetime may change; in particular, it grants no new epoch or format intent.
func PreparationReboot(previous, next Plan) bool {
	if previous.Purpose != PurposePreparation || next.Purpose != PurposePreparation ||
		previous.NodeUID == "" || previous.BootID == "" || next.BootID == "" ||
		previous.BootID == next.BootID {
		return false
	}
	previous.BootID, next.BootID = "", ""
	return PreparationDigest(previous) == PreparationDigest(next)
}
