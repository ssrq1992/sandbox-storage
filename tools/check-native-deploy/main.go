// Command check-native-deploy checks rendered architecture, not cluster behavior.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func main() {
	path := flag.String("file", "", "rendered YAML")
	namespace := flag.String("namespace", "agentenv-system", "expected namespace")
	flag.Parse()
	if err := check(*path, *namespace); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("native incremental deployment structure passed; no cluster resources applied")
}
func check(path, namespace string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := yaml.NewYAMLOrJSONDecoder(file, 4096)
	objects := map[string]*unstructured.Unstructured{}
	for {
		var raw map[string]any
		err = decoder.Decode(&raw)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			continue
		}
		object := &unstructured.Unstructured{Object: raw}
		objects[object.GetKind()+"/"+object.GetName()] = object
	}
	for _, key := range []string{"Deployment/agentenv-gateway", "Deployment/agentenv-scheduler", "DaemonSet/agentenv-node", "Service/agentenv-nodes", "Service/agentenv-scheduler", "ConfigMap/agentenv-ssf-config", "Secret/agentenv-auth"} {
		object := objects[key]
		if object == nil || object.GetNamespace() != namespace {
			return fmt.Errorf("missing native resource or namespace: %s", key)
		}
	}
	if objects["Deployment/agentenv-control"] != nil {
		return errors.New("standalone sandbox control plane present")
	}
	headless, _, _ := unstructured.NestedString(objects["Service/agentenv-nodes"].Object, "spec", "clusterIP")
	if headless != "None" {
		return errors.New("original headless discovery missing")
	}
	daemon := objects["DaemonSet/agentenv-node"].Object
	shared, _, _ := unstructured.NestedBool(daemon, "spec", "template", "spec", "shareProcessNamespace")
	if !shared {
		return errors.New("runtime process evidence requires shared PID namespace")
	}
	containers, _, _ := unstructured.NestedSlice(daemon, "spec", "template", "spec", "containers")
	var native map[string]any
	for _, item := range containers {
		c := item.(map[string]any)
		if c["name"] == "agentenv" {
			native = c
		}
	}
	if native == nil {
		return errors.New("original native container missing")
	}
	for _, field := range []string{"startupProbe", "readinessProbe", "livenessProbe", "lifecycle"} {
		if native[field] == nil {
			return fmt.Errorf("native %s removed", field)
		}
	}
	env := map[string]string{}
	for _, item := range native["env"].([]any) {
		entry := item.(map[string]any)
		if value, ok := entry["value"].(string); ok {
			env[entry["name"].(string)] = value
		}
	}
	if env["AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED"] != "true" || env["API_ADDR"] != "0.0.0.0:8000" || env["AENV_STORAGE_PROVIDER_URL"] != "http://127.0.0.1:9445" {
		return errors.New("native heartbeat/listener or provider endpoint changed")
	}
	sidecars, _, _ := unstructured.NestedSlice(daemon, "spec", "template", "spec", "initContainers")
	if len(sidecars) != 2 {
		return errors.New("expected two ordered native sidecars")
	}
	for i, name := range []string{"ssf-node", "agentenv-storage"} {
		c := sidecars[i].(map[string]any)
		if c["name"] != name || c["restartPolicy"] != "Always" {
			return errors.New("native sidecar lifetime/order missing")
		}
	}
	data, _, _ := unstructured.NestedStringMap(objects["ConfigMap/agentenv-ssf-config"].Object, "data")
	var node map[string]any
	if err = json.Unmarshal([]byte(data["node.json"]), &node); err != nil {
		return err
	}
	if node["native_mode"] != true || node["hook_listen"] != "127.0.0.1:9445" || node["scheduler_endpoint"] != "agentenv-scheduler:9090" {
		return errors.New("plugin config not wired into native scheduler")
	}
	if _, ok := node["workers"]; ok {
		return errors.New("static worker inventory forbidden")
	}
	return nil
}
