package node

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	v1 "ssf.dev/ssf/api/ssf/v1"
	driver "ssf.dev/ssf/drivers/csi"
)

type managedFixture struct {
	v1.ManagedStorageDriverClient
	requests []*v1.DriverFinalizeRequest
	fail     bool
}

func (m *managedFixture) FinalizeRuntime(_ context.Context, q *v1.DriverFinalizeRequest, _ ...grpc.CallOption) (*v1.Operation, error) {
	m.requests = append(m.requests, q)
	if m.fail {
		return nil, context.DeadlineExceeded
	}
	return &v1.Operation{OperationId: q.BindingUid, State: "Finalized"}, nil
}

type managedSessions struct {
	node    *fakeNode
	managed *managedFixture
}

func (s managedSessions) Node(context.Context, string) (*driver.Session, error) {
	return &driver.Session{Node: s.node, Managed: s.managed, NodeID: "csi-node", Stage: true}, nil
}

func TestManagedFinalizationPrecedesUnpublishAndSurvivesRetry(t *testing.T) {
	n, f, key := setup(t)
	m := &managedFixture{fail: true}
	n.Sessions = managedSessions{f, m}
	token := plan(key, "managed")
	if _, err := n.Prepare(context.Background(), "managed", token); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Release(context.Background(), "managed", "managed", "verified-test-state"); err == nil {
		t.Fatal("finalization failure lost")
	}
	if f.unpublish != 0 || f.unstage != 0 {
		t.Fatal("cleaned up before final publication")
	}
	r, err := n.WAL.Get("managed")
	if err != nil {
		t.Fatal(err)
	}
	if r.ReleaseEvidence != "verified-test-state" || r.SignedPlan != token {
		t.Fatal("cleanup authority not persisted")
	}
	m.fail = false
	if err = n.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.unpublish != 1 || f.unstage != 1 || len(m.requests) != 2 {
		t.Fatal("cleanup retry did not resume")
	}
	for _, q := range m.requests {
		if q.SignedNodePlan != token || q.DetachedEvidence != "verified-test-state" || q.BindingUid != "managed" {
			t.Fatal("lost cleanup scope")
		}
	}
	if err = n.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(m.requests) != 2 {
		t.Fatal("repeated completed finalization")
	}
}
