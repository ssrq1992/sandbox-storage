package main

import (
	"encoding/json"
	"testing"
)

func mountDocuments() (map[string]any, map[string]any) {
	var old map[string]any
	_ = json.Unmarshal([]byte(`{"components":{"schemas":{"Volume":{"type":"object","properties":{"name":{"type":"string"}}},"NewVolume":{"type":"object","properties":{"name":{"type":"string"}}},"SandboxVolumeMount":{"type":"object","required":["name","path"],"properties":{"name":{"type":"string"},"path":{"type":"string"}}}}},"paths":{"/sandboxes":{"post":{"security":[{"ApiKey":[]}]}}}}`), &old)
	raw, _ := json.Marshal(old)
	var next map[string]any
	_ = json.Unmarshal(raw, &next)
	s := next["components"].(map[string]any)["schemas"].(map[string]any)
	s["SandboxPVCSource"] = map[string]any{"type": "object", "required": []any{"name"}, "properties": map[string]any{"name": map[string]any{"type": "string"}, "namespace": map[string]any{"type": "string"}}}
	mount := s["SandboxVolumeMount"].(map[string]any)
	mount["required"] = []any{"path"}
	mount["description"] = "name xor pvc"
	mount["properties"].(map[string]any)["pvc"] = map[string]any{"$ref": "#/components/schemas/SandboxPVCSource"}
	return old, next
}
func TestOnlyApprovedPVCAlternativeIsCompatible(t *testing.T) {
	old, next := mountDocuments()
	if err := compatible(old, next); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"name-type", "path-required", "security"} {
		t.Run(kind, func(t *testing.T) {
			old, next := mountDocuments()
			s := next["components"].(map[string]any)["schemas"].(map[string]any)
			m := s["SandboxVolumeMount"].(map[string]any)
			switch kind {
			case "name-type":
				m["properties"].(map[string]any)["name"] = map[string]any{"type": "integer"}
			case "path-required":
				m["required"] = []any{}
			case "security":
				next["paths"].(map[string]any)["/sandboxes"].(map[string]any)["post"].(map[string]any)["security"] = []any{}
			}
			if compatible(old, next) == nil {
				t.Fatal("unapproved change accepted")
			}
		})
	}
}
