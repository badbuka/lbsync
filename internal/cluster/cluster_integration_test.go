//go:build integration

// Run with: go test -tags integration ./internal/cluster/...
package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/badbuka/lbsync/internal/engine"
)

func startNode(t *testing.T, olricPort, mlistPort int, peers []string) *Cluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := NewCluster(ctx, Config{
		Env:            "local",
		BindAddr:       "127.0.0.1",
		BindPort:       olricPort,
		MemberlistPort: mlistPort,
		Peers:          peers,
		Password:       "test-secret",
		ReplicaCount:   2,
	})
	if err != nil {
		t.Fatalf("start node %d: %v", olricPort, err)
	}
	t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer scancel()
		_ = c.Close(sctx)
	})
	return c
}

func TestTwoNodeConvergence(t *testing.T) {
	a := startNode(t, 4320, 4322, []string{"127.0.0.1:4332"})
	b := startNode(t, 4330, 4332, []string{"127.0.0.1:4322"})

	ctx := context.Background()
	rec := &engine.Record{
		Kind:    "certs",
		Key:     "example.com",
		Version: engine.Version{Primary: 100, Secondary: 200, Tiebreak: "abc"},
		Blobs:   map[string][]byte{"cert": []byte("CERT"), "key": []byte("KEY")},
		Meta:    map[string]string{"domain": "example.com"},
		Source:  "node-a",
	}
	if err := a.Put(ctx, rec); err != nil {
		t.Fatalf("put: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		got, ok, err := b.Get(ctx, "certs", "example.com")
		if err == nil && ok {
			if got.Version.Primary != 100 || string(got.Blobs["cert"]) != "CERT" {
				t.Fatalf("converged record mismatch: %+v", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("record did not converge to node b (ok=%v err=%v)", ok, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	keys, err := b.Keys(ctx, "certs")
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "example.com" {
		t.Fatalf("unexpected keys on node b: %v", keys)
	}

	release, err := a.Lock(ctx, "certs", "example.com", 5*time.Second)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	release()
}
