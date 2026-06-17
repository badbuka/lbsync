package bundle

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/badbuka/lbsync/internal/cert"
)

func writeLineage(t *testing.T, dir string, serial int64, notAfter time.Time) {
	t.Helper()
	cert.WriteTestCertKey(t,
		filepath.Join(dir, CertFile),
		filepath.Join(dir, KeyFile),
		serial, notAfter, []string{"example.com"})
}

func TestReadLineageAndCompare(t *testing.T) {
	older := t.TempDir()
	newer := t.TempDir()
	writeLineage(t, older, 1, time.Now().Add(30*24*time.Hour))
	writeLineage(t, newer, 2, time.Now().Add(60*24*time.Hour))

	ob, err := ReadLineage("example.com", older)
	if err != nil {
		t.Fatalf("ReadLineage older: %v", err)
	}
	nb, err := ReadLineage("example.com", newer)
	if err != nil {
		t.Fatalf("ReadLineage newer: %v", err)
	}
	if ob.Domain != "example.com" {
		t.Fatalf("domain = %q", ob.Domain)
	}
	if !Newer(nb.Version, ob.Version) {
		t.Fatalf("expected newer bundle to compare greater")
	}
	if Newer(ob.Version, nb.Version) {
		t.Fatalf("older bundle should not be newer")
	}
}

func TestWriteAtomicPermsAndNoop(t *testing.T) {
	src := t.TempDir()
	writeLineage(t, src, 1, time.Now().Add(30*24*time.Hour))
	b, err := ReadLineage("example.com", src)
	if err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "example.com")
	changed, err := b.WriteAtomic(dst)
	if err != nil {
		t.Fatalf("WriteAtomic: %v", err)
	}
	if !changed {
		t.Fatal("first write should report changed")
	}

	info, err := os.Stat(filepath.Join(dst, KeyFile))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("key perm = %o, want 0600", info.Mode().Perm())
	}

	changed, err = b.WriteAtomic(dst)
	if err != nil {
		t.Fatalf("WriteAtomic second: %v", err)
	}
	if changed {
		t.Fatal("identical write should report no change")
	}
}

func TestRollbackRestoresPrevious(t *testing.T) {
	srcOld := t.TempDir()
	srcNew := t.TempDir()
	writeLineage(t, srcOld, 1, time.Now().Add(30*24*time.Hour))
	writeLineage(t, srcNew, 2, time.Now().Add(60*24*time.Hour))

	oldB, _ := ReadLineage("example.com", srcOld)
	newB, _ := ReadLineage("example.com", srcNew)

	dst := filepath.Join(t.TempDir(), "example.com")
	if _, err := oldB.WriteAtomic(dst); err != nil {
		t.Fatal(err)
	}
	if _, err := newB.WriteAtomic(dst); err != nil {
		t.Fatal(err)
	}

	// After writing the new bundle, rolling back should restore the old cert.
	if err := Rollback(dst); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	restored, err := ReadLineage("example.com", dst)
	if err != nil {
		t.Fatalf("ReadLineage after rollback: %v", err)
	}
	if restored.Version.Serial != oldB.Version.Serial {
		t.Fatalf("rollback did not restore old cert: serial=%s want=%s", restored.Version.Serial, oldB.Version.Serial)
	}
}
