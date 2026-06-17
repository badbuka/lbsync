package blob

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/badbuka/lbsync/internal/hook"
)

func newProvider(res *resource) *provider {
	return &provider{
		resources: []*resource{res},
		byName:    map[string]*resource{res.name: res},
		host:      "node-1",
		pending:   map[string]struct{}{},
	}
}

func TestLocalAndApply(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	src := filepath.Join(srcDir, "blocklist.txt")
	dst := filepath.Join(dstDir, "blocklist.txt")
	if err := os.WriteFile(src, []byte("1.2.3.4\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	res := &resource{name: "blocklist", source: src, dest: dst, strategy: "mtime", runner: &hook.Runner{Kind: Kind}}
	p := newProvider(res)

	recs, err := p.Local(context.Background())
	if err != nil {
		t.Fatalf("Local: %v", err)
	}
	if len(recs) != 1 || recs[0].Key != "blocklist" {
		t.Fatalf("unexpected records: %+v", recs)
	}

	changed, err := p.Apply(context.Background(), recs[0])
	if err != nil || !changed {
		t.Fatalf("Apply changed=%v err=%v", changed, err)
	}
	got, err := os.ReadFile(dst)
	if err != nil || string(got) != "1.2.3.4\n" {
		t.Fatalf("dest content = %q err=%v", string(got), err)
	}

	// Re-applying identical content is a no-op.
	changed, err = p.Apply(context.Background(), recs[0])
	if err != nil || changed {
		t.Fatalf("identical apply should be no-op: changed=%v err=%v", changed, err)
	}
}

func TestRollback(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	src := filepath.Join(srcDir, "f.txt")
	dst := filepath.Join(dstDir, "f.txt")
	res := &resource{name: "f", source: src, dest: dst, strategy: "sha", runner: &hook.Runner{Kind: Kind}}
	p := newProvider(res)

	if err := os.WriteFile(src, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	recs, _ := p.Local(context.Background())
	if _, err := p.Apply(context.Background(), recs[0]); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(src, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	recs, _ = p.Local(context.Background())
	if _, err := p.Apply(context.Background(), recs[0]); err != nil {
		t.Fatal(err)
	}
	if err := p.Rollback(context.Background(), []string{"f"}); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	got, _ := os.ReadFile(dst)
	if string(got) != "old" {
		t.Fatalf("rollback content = %q, want old", string(got))
	}
}
