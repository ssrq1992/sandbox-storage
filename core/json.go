package core

import (
	"encoding/json"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/encoding/protojson"
)

// CSI messages contain protobuf oneofs; encoding/json alone cannot restore them.
func (v Volume) MarshalJSON() ([]byte, error) {
	type plain Volume
	var cap json.RawMessage
	if v.Capability != nil {
		b, e := protojson.Marshal(v.Capability)
		if e != nil {
			return nil, e
		}
		cap = b
	}
	return json.Marshal(struct {
		*plain
		Capability json.RawMessage
	}{(*plain)(&v), cap})
}
func (v *Volume) UnmarshalJSON(b []byte) error {
	type plain Volume
	aux := struct {
		*plain
		Capability json.RawMessage
	}{plain: (*plain)(v)}
	if e := json.Unmarshal(b, &aux); e != nil {
		return e
	}
	if len(aux.Capability) > 0 && string(aux.Capability) != "null" {
		v.Capability = &csi.VolumeCapability{}
		return protojson.Unmarshal(aux.Capability, v.Capability)
	}
	return nil
}
