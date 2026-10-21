package agentenvadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"ssf.dev/ssf/consume"
	"ssf.dev/ssf/core"
)

func TestNativePublicationDeletionCannotCollectUnknownCommit(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	repository := core.Hash("repository")
	ref := id + "ref"
	if err := c.Ledger.BeginCheckpoint(ctx, ref, "template", map[string]string{"template": id}); err != nil {
		t.Fatal(err)
	}
	if err := c.Ledger.PinCheckpoint(ctx, ref, id+"snapshot"); err != nil {
		t.Fatal(err)
	}
	if err := c.Ledger.CommitCheckpoint(ctx, ref, map[string]string{"template": id}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_publications(tenant,native_id,reference,descriptor,repository_id) VALUES($1,$2,$3,'{}',$4)`, c.Principal.Tenant, id, ref, repository); err != nil {
		t.Fatal(err)
	}
	if managed, err := c.BeginNativePublicationDelete(ctx, id, core.Hash("other")); managed || err != nil {
		t.Fatalf("different repository inherited publication: %v %v", managed, err)
	}
	if managed, err := c.BeginNativePublicationDelete(ctx, id, repository); !managed || !errors.Is(err, core.ErrPending) {
		t.Fatalf("unknown publication collected: %v %v", managed, err)
	}
	if err := c.CompleteNativePublicationDelete(ctx, id, repository); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("release without delete intent: %v", err)
	}
	if err := c.NativePublished(ctx, id, repository); err != nil {
		t.Fatal(err)
	}
	if managed, err := c.BeginNativePublicationDelete(ctx, id, repository); !managed || err != nil {
		t.Fatalf("delete admission: %v %v", managed, err)
	}
	if err := c.NativePublished(ctx, id, repository); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("late publication reopened deletion: %v", err)
	}
	if _, err := c.BeginNativePublication(ctx, json.RawMessage(`{}`), id, repository); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("new publication reopened deletion: %v", err)
	}
	if err := c.Ledger.PermitSnapshotDelete(ctx, id+"snapshot"); !errors.Is(err, core.ErrPending) {
		t.Fatalf("intent released references: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := c.CompleteNativePublicationDelete(ctx, id, repository); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Ledger.PermitSnapshotDelete(ctx, id+"snapshot"); err != nil {
		t.Fatal(err)
	}
	if managed, err := c.BeginNativePublicationDelete(ctx, "legacy", repository); managed || err != nil {
		t.Fatalf("legacy path changed: %v %v", managed, err)
	}
}

func TestNativeRepositoryOutboxRequiresExplicitRepositoryEvidence(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "publications"), 0700); err != nil {
		t.Fatal(err)
	}
	id := "template"
	repository := core.Hash("repository")
	path := filepath.Join(root, "publications", repository+"_"+id+"-deleted.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"action":"deleted","native_id":"template","repository_id":"`+repository+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	status, body := 404, `{}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/storage/checkpoints/template" || r.Header.Get("X-API-Key") != "key" {
			t.Error("wrong native proof query")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	calls := 0
	failControl := true
	n := Node{Config: Config{RuntimeRoot: root, AgentURL: server.URL, NativeNodeID: "node"}, Agent: &HTTPClient{Client: server.Client(), APIKey: "key"}, Control: func(_ context.Context, c NativeCommand) (json.RawMessage, error) {
		calls++
		var receipt map[string]string
		if err := json.Unmarshal(c.Data, &receipt); err != nil || receipt["repository_id"] != repository {
			t.Error("repository identity lost in completion")
		}
		if c.Action != "checkpoint.deleted" || c.NodeID != "node" {
			t.Error("wrong completion command")
		}
		if failControl {
			return nil, errors.New("control unavailable")
		}
		return json.RawMessage(`{}`), nil
	}}
	for _, observation := range []struct {
		status int
		body   string
	}{{404, `{}`}, {503, `{}`}, {200, `{}`}, {200, `{"id":"template","exists":false,"committed":false,"repository_id":"` + core.Hash("other") + `"}`}, {200, `{"id":"other","exists":false,"committed":false}`}, {200, `{"id":"template","exists":true,"committed":true}`}} {
		status, body = observation.status, observation.body
		if err := n.importNativePublications(context.Background()); err == nil {
			t.Fatal("accepted missing/invalid absence evidence")
		}
		if calls != 0 {
			t.Fatal("control called before repository absence")
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatal("lost durable task")
		}
	}
	status, body = 200, `{"id":"template","exists":false,"committed":false,"repository_id":"`+repository+`"}`
	if err := n.importNativePublications(context.Background()); err == nil {
		t.Fatal("control failure ignored")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("task removed before control persisted completion")
	}
	failControl = false
	if err := n.importNativePublications(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("acknowledged task retained: %v", err)
	}
}

func TestNativeTemplateDeleteWaitsForArtifactConsumer(t *testing.T) {
	c, ctx, id := nativeLifecycleStore(t)
	c.Principal.Namespaces = []string{"test"}
	d, err := c.Dispatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	member := consume.CaptureMember{BindingID: id + "binding", MountID: "data", GuestPath: "/data", PVC: core.PVCRef{Namespace: "test", Name: "data", UID: id + "pvc"}, Snapshot: "snapshot", SnapshotUID: id + "snapshot", ContentUID: id + "content"}
	manifest := consume.CaptureManifest{NativeResourceUID: id + "artifact", Intent: consume.CaptureIntent{Version: consume.CaptureVersion, ID: id + "capture", Operation: id + "operation", OwnerUID: d.OwnerUID, RunUID: d.RunUID, BarrierID: "barrier", NativeArtifact: "artifact", NativeDigest: core.Hash("artifact"), Classes: map[string]string{member.BindingID: "class"}}, Members: []consume.CaptureMember{member}}
	if err = c.Ledger.BeginCheckpoint(ctx, manifest.Intent.ID, d.OwnerUID, manifest.Intent); err != nil {
		t.Fatal(err)
	}
	if err = c.Ledger.PinCheckpoint(ctx, manifest.Intent.ID, manifest.NativeResourceUID, member.SnapshotUID, member.ContentUID); err != nil {
		t.Fatal(err)
	}
	if err = c.Ledger.CommitCheckpoint(ctx, manifest.Intent.ID, manifest); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(manifest)
	repository := core.Hash("catalog")
	published, err := c.BeginNativePublication(ctx, raw, id+"template", repository)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.NativePublished(ctx, id+"template", repository); err != nil {
		t.Fatal(err)
	}
	capture, err := c.Ledger.Checkpoint(ctx, manifest.Intent.ID)
	if err != nil || capture.State != "Released" || len(capture.Resources) != 0 {
		t.Fatalf("transient capture pin retained after publication: %+v %v", capture, err)
	}
	if err = c.Ledger.PermitSnapshotDelete(ctx, member.SnapshotUID); !errors.Is(err, core.ErrPending) {
		t.Fatalf("publication transfer lost protection: %v", err)
	}
	if _, err = c.BeginNativePublication(ctx, raw, id+"template", repository); err != nil {
		t.Fatalf("publication replay after transfer: %v", err)
	}
	source := published["reference"].(string)
	child := id + "restore"
	intent := NativeIntent{RequestID: child, Tenant: "test", Path: "/v2/sandboxes", Body: []byte(`{}`), BodyDigest: core.Hash("{}")}
	raw, _ = json.Marshal(intent)
	if _, err = c.Ledger.Session(ctx).Exec(ctx, `INSERT INTO agentenv_native_runs(id,digest,owner_uid,run_uid,state,intent,updated) VALUES($1,$2,$3,$4,'Intent',$5,now()-interval '1 minute')`, child, core.Digest(intent), child+"owner", child+"run", raw); err != nil {
		t.Fatal(err)
	}
	if _, err = c.RetainNativeCheckpoint(ctx, source, "restore", child); err != nil {
		t.Fatal(err)
	}
	if managed, err := c.BeginNativePublicationDelete(ctx, id+"template", repository); !managed || !errors.Is(err, core.ErrPending) {
		t.Fatalf("deleted native artifact during consumption: %v %v", managed, err)
	}
	if err = c.recoverNativeTemplateConsumers(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = c.BeginNativePublicationDelete(ctx, id+"template", repository); !errors.Is(err, core.ErrPending) {
		t.Fatalf("pending intent treated as completed read: %v", err)
	}
	// The actual no-reservation/no-bindings compensation is required, not time alone.
	if err = c.recoverNativeIntent(ctx, intent, child+"owner", child+"run", 0); err != nil {
		t.Fatal(err)
	}
	if err = c.recoverNativeTemplateConsumers(ctx); err != nil {
		t.Fatal(err)
	}
	if managed, err := c.BeginNativePublicationDelete(ctx, id+"template", repository); !managed || err != nil {
		t.Fatalf("settled consumer retained source directory: %v %v", managed, err)
	}
	if _, err = c.RetainNativeCheckpoint(ctx, source, "restore", id+"late"); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("consumer entered deleting repository: %v", err)
	}
}
