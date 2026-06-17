package certs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/badbuka/lbsync/internal/bundle"
	"github.com/badbuka/lbsync/internal/cert"
	"github.com/badbuka/lbsync/internal/hook"
)

func writeLive(t *testing.T, root, lineage string, serial int64, notAfter time.Time) {
	t.Helper()
	dir := filepath.Join(root, "live", lineage)
	cert.WriteTestCertKey(t,
		filepath.Join(dir, bundle.CertFile),
		filepath.Join(dir, bundle.KeyFile),
		serial, notAfter, []string{lineage})
}

func TestLocalAndApply(t *testing.T) {
	root := t.TempDir()
	serving := t.TempDir()
	writeLive(t, root, "example.com", 1, time.Now().Add(60*24*time.Hour))

	p := &provider{root: root, servingDir: serving, host: "node-1", runner: &hook.Runner{Kind: Kind}}

	recs, err := p.Local(context.Background())
	if err != nil {
		t.Fatalf("Local: %v", err)
	}
	if len(recs) != 1 || recs[0].Key != "example.com" {
		t.Fatalf("unexpected local records: %+v", recs)
	}

	changed, err := p.Apply(context.Background(), recs[0])
	if err != nil || !changed {
		t.Fatalf("Apply changed=%v err=%v", changed, err)
	}
	if _, err := os.Stat(filepath.Join(serving, "example.com", bundle.KeyFile)); err != nil {
		t.Fatalf("expected key written to serving dir: %v", err)
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("Flush with empty hook should succeed: %v", err)
	}
}

func TestRollbackAfterVerifyFailure(t *testing.T) {
	root := t.TempDir()
	serving := t.TempDir()

	// First publish an older cert and apply it cleanly.
	writeLive(t, root, "example.com", 1, time.Now().Add(30*24*time.Hour))
	p := &provider{root: root, servingDir: serving, host: "node-1", runner: &hook.Runner{Kind: Kind}}
	recs, _ := p.Local(context.Background())
	if _, err := p.Apply(context.Background(), recs[0]); err != nil {
		t.Fatal(err)
	}
	oldServed, err := bundle.ReadLineage("example.com", filepath.Join(serving, "example.com"))
	if err != nil {
		t.Fatal(err)
	}

	// Now a newer cert arrives but the verify command fails.
	writeLive(t, root, "example.com", 2, time.Now().Add(90*24*time.Hour))
	recs, _ = p.Local(context.Background())
	if _, err := p.Apply(context.Background(), recs[0]); err != nil {
		t.Fatal(err)
	}
	p.runner = &hook.Runner{Kind: Kind, Verify: []string{"false"}}
	if err := p.Flush(context.Background()); err == nil {
		t.Fatal("expected Flush to fail when verify command fails")
	}
	if err := p.Rollback(context.Background(), []string{"example.com"}); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	restored, err := bundle.ReadLineage("example.com", filepath.Join(serving, "example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if restored.Version.Serial != oldServed.Version.Serial {
		t.Fatalf("rollback did not restore old cert: got serial %s want %s",
			restored.Version.Serial, oldServed.Version.Serial)
	}
}
