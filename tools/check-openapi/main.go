// Command check-openapi protects the original public sandbox API while allowing
// additive, optional Volume provider fields.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"

	"sigs.k8s.io/yaml"
)

func document(raw []byte) (map[string]any, error) {
	data, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(data, &result)
	return result, err
}
func compatible(old, next map[string]any) error {
	schemas := func(doc map[string]any) (map[string]any, error) {
		components, ok := doc["components"].(map[string]any)
		if !ok {
			return nil, errors.New("OpenAPI components missing")
		}
		schemas, ok := components["schemas"].(map[string]any)
		if !ok {
			return nil, errors.New("OpenAPI schemas missing")
		}
		return schemas, nil
	}
	oldSchemas, err := schemas(old)
	if err != nil {
		return err
	}
	newSchemas, err := schemas(next)
	if err != nil {
		return err
	}
	// Direct PVC is an explicitly approved additive alternative to a named
	// mount. Normalize only that exact shape, preserving every original
	// name/path property, public path and authentication definition.
	if source, exists := newSchemas["SandboxPVCSource"]; exists {
		pvc, ok := source.(map[string]any)
		if !ok || pvc["type"] != "object" || !reflect.DeepEqual(pvc["required"], []any{"name"}) {
			return errors.New("invalid PVC source schema")
		}
		props, ok := pvc["properties"].(map[string]any)
		if !ok || len(props) != 2 {
			return errors.New("invalid PVC source fields")
		}
		for _, name := range []string{"name", "namespace"} {
			property, ok := props[name].(map[string]any)
			if !ok || property["type"] != "string" || len(property) > 2 {
				return errors.New("PVC name and namespace must remain strings")
			}
		}
		before, ok := oldSchemas["SandboxVolumeMount"].(map[string]any)
		if !ok {
			return errors.New("original mount schema missing")
		}
		after, ok := newSchemas["SandboxVolumeMount"].(map[string]any)
		if !ok || !reflect.DeepEqual(after["required"], []any{"path"}) {
			return errors.New("direct PVC mount must require only path")
		}
		mountProps, ok := after["properties"].(map[string]any)
		if !ok || !reflect.DeepEqual(mountProps["pvc"], map[string]any{"$ref": "#/components/schemas/SandboxPVCSource"}) {
			return errors.New("invalid direct PVC reference")
		}
		delete(mountProps, "pvc")
		after["required"] = before["required"]
		if description, exists := before["description"]; exists {
			after["description"] = description
		} else {
			delete(after, "description")
		}
		if !reflect.DeepEqual(before, after) {
			return errors.New("original named mount semantics changed")
		}
		delete(newSchemas, "SandboxPVCSource")
	}
	allowed := map[string]map[string]bool{"Volume": {"provider": true, "capabilities": true}, "NewVolume": {"provider": true, "storage": true}}
	for name, additions := range allowed {
		before, ok := oldSchemas[name].(map[string]any)
		if !ok {
			return fmt.Errorf("missing original %s", name)
		}
		after, ok := newSchemas[name].(map[string]any)
		if !ok {
			return fmt.Errorf("missing current %s", name)
		}
		if !reflect.DeepEqual(before["required"], after["required"]) {
			return fmt.Errorf("%s added or changed required fields", name)
		}
		beforeProps, ok := before["properties"].(map[string]any)
		if !ok {
			return errors.New("original properties missing")
		}
		afterProps, ok := after["properties"].(map[string]any)
		if !ok {
			return errors.New("current properties missing")
		}
		for field, definition := range beforeProps {
			if !reflect.DeepEqual(definition, afterProps[field]) {
				return fmt.Errorf("changed existing %s.%s", name, field)
			}
		}
		for field := range afterProps {
			if _, existed := beforeProps[field]; !existed && !additions[field] {
				return fmt.Errorf("unexpected new %s.%s", name, field)
			}
		}
		copySchema := map[string]any{}
		for key, value := range after {
			copySchema[key] = value
		}
		copySchema["properties"] = beforeProps
		if !reflect.DeepEqual(before, copySchema) {
			return fmt.Errorf("changed %s schema semantics", name)
		}
		newSchemas[name] = before
	}
	// Only this additive Volume operation is allowed; every original path,
	// security requirement and sandbox schema must still compare exactly.
	if definition, exists := newSchemas["ExpandVolume"]; exists {
		schema, ok := definition.(map[string]any)
		if !ok || !reflect.DeepEqual(schema["required"], []any{"sizeMB"}) {
			return errors.New("invalid ExpandVolume required fields")
		}
		props, ok := schema["properties"].(map[string]any)
		if !ok || len(props) != 1 || props["sizeMB"] == nil {
			return errors.New("invalid ExpandVolume fields")
		}
		paths, ok := next["paths"].(map[string]any)
		if !ok {
			return errors.New("OpenAPI paths missing")
		}
		path, ok := paths["/volumes/{volumeID}/expand"].(map[string]any)
		if !ok || len(path) != 1 || path["post"] == nil {
			return errors.New("offline expansion POST missing")
		}
		delete(paths, "/volumes/{volumeID}/expand")
		delete(newSchemas, "ExpandVolume")
	}
	if !reflect.DeepEqual(old, next) {
		return errors.New("public API changed outside approved Volume fields, direct PVC mounts and offline expansion")
	}
	return nil
}
func main() {
	root := flag.String("agentenv", "../AgentENV", "native repository")
	baseline := flag.String("baseline", "HEAD", "original native git revision")
	flag.Parse()
	cmd := exec.Command("git", "show", *baseline+":src/api/openapi.yml")
	cmd.Dir = *root
	oldRaw, err := cmd.Output()
	if err == nil {
		var nextRaw []byte
		nextRaw, err = os.ReadFile(filepath.Join(*root, "src/api/openapi.yml"))
		if err == nil {
			var old, next map[string]any
			old, err = document(oldRaw)
			if err == nil {
				next, err = document(nextRaw)
			}
			if err == nil {
				err = compatible(old, next)
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Original public API preserved; approved additive Volume fields, direct PVC mounts and offline expansion")
}
