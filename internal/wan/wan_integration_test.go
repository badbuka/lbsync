//go:build integration

package wan

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/badbuka/lbsync/internal/engine"
)

func startIntegrationNode(t *testing.T, port int, peers []string) *Backend {
	t.Helper()
	srv, err := New(context.Background(), Config{
		BindAddr:  "127.0.0.1",
		Port:      port,
		Peers:     peers,
		TLSConfig: testTLSConfig(t),
	})
	if err != nil {
		t.Fatalf("startIntegrationNode port=%d: %v", port, err)
	}
	waitForPort(t, port)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Close(ctx)
	})
	return srv
}

func TestTwoNodeWANConvergence(t *testing.T) {
	aAddr := "127.0.0.1:4420"
	bAddr := "127.0.0.1:4430"

	a := startIntegrationNode(t, 4420, []string{bAddr})
	b := startIntegrationNode(t, 4430, []string{aAddr})

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

	// Poll node B for up to 15 seconds for convergence via fan-out.
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
			t.Fatalf("record did not converge to node B (ok=%v err=%v)", ok, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	keys, err := b.Keys(ctx, "certs")
	if err != nil {
		t.Fatalf("keys on B: %v", err)
	}
	sort.Strings(keys)
	if len(keys) != 1 || keys[0] != "example.com" {
		t.Fatalf("unexpected keys on node B: %v", keys)
	}
}
