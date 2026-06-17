// Package bundle reads and writes a full certbot certificate bundle (the leaf
// cert, private key, and optional chain/fullchain) and provides a total
// ordering over bundle versions so the newest one can be selected.
package bundle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/badbuka/lbsync/internal/atomicfile"
	"github.com/badbuka/lbsync/internal/cert"
)

// Filenames within a lineage directory (certbot layout).
const (
	CertFile      = "cert.pem"
	KeyFile       = "privkey.pem"
	ChainFile     = "chain.pem"
	FullchainFile = "fullchain.pem"
)

// Version is a total order describing which bundle is newest.
type Version struct {
	NotBefore time.Time
	NotAfter  time.Time
	Serial    string
	SHA256    string
}

// Bundle is a full, serveable certificate bundle for one lineage.
type Bundle struct {
	Lineage  string
	Domain   string
	Version  Version
	CertPEM  []byte
	KeyPEM   []byte
	ChainPEM []byte
	FullPEM  []byte
}

// ReadLineage reads <dir>/{cert,privkey}.pem (required) plus optional
// chain/fullchain and parses the leaf to compute the bundle Version.
func ReadLineage(lineage, dir string) (*Bundle, error) {
	certPath := filepath.Join(dir, CertFile)
	keyPath := filepath.Join(dir, KeyFile)

	leaf, err := cert.Load(certPath)
	if err != nil {
		return nil, err
	}
	certPEM, err := os.ReadFile(certPath) //nolint:gosec // G304: operator-configured lineage dir
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", certPath, err)
	}
	keyPEM, err := cert.LoadKey(keyPath)
	if err != nil {
		return nil, err
	}

	b := &Bundle{
		Lineage: lineage,
		Domain:  cert.PrimaryDomain(leaf, lineage),
		Version: Version{
			NotBefore: leaf.NotBefore.UTC(),
			NotAfter:  leaf.NotAfter.UTC(),
			Serial:    leaf.SerialNumber.Text(16),
			SHA256:    sha256Hex(certPEM),
		},
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
	}
	if chain, err := os.ReadFile(filepath.Join(dir, ChainFile)); err == nil { //nolint:gosec // G304
		b.ChainPEM = chain
	}
	if full, err := os.ReadFile(filepath.Join(dir, FullchainFile)); err == nil { //nolint:gosec // G304
		b.FullPEM = full
	}
	return b, nil
}

// Compare returns >0 if a is newer than b, <0 if older, 0 if equal. Ordering:
// NotBefore, then NotAfter, then Serial, then SHA256 (all descending = newer).
func Compare(a, b Version) int {
	if !a.NotBefore.Equal(b.NotBefore) {
		if a.NotBefore.After(b.NotBefore) {
			return 1
		}
		return -1
	}
	if !a.NotAfter.Equal(b.NotAfter) {
		if a.NotAfter.After(b.NotAfter) {
			return 1
		}
		return -1
	}
	if c := strings.Compare(a.Serial, b.Serial); c != 0 {
		return c
	}
	return strings.Compare(a.SHA256, b.SHA256)
}

// Newer reports whether a is strictly newer than b.
func Newer(a, b Version) bool { return Compare(a, b) > 0 }

// WriteAtomic writes the bundle's files into dir using temp file + rename. The
// private key is written 0600, certs 0644. When a file's content is unchanged
// it is left untouched. Any file that is replaced is first copied to
// "<name>.bak" so a later Rollback can restore it. It reports whether anything
// changed on disk.
func (b *Bundle) WriteAtomic(dir string) (bool, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return false, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	files := []struct {
		name string
		data []byte
		perm os.FileMode
	}{
		{CertFile, b.CertPEM, 0o644},
		{KeyFile, b.KeyPEM, 0o600},
		{ChainFile, b.ChainPEM, 0o644},
		{FullchainFile, b.FullPEM, 0o644},
	}
	changed := false
	for _, f := range files {
		if len(f.data) == 0 {
			continue
		}
		c, err := atomicfile.WriteAtomic(filepath.Join(dir, f.name), f.data, f.perm)
		if err != nil {
			return changed, err
		}
		changed = changed || c
	}
	return changed, nil
}

// Rollback restores any "<name>.bak" files in dir over their originals. It is a
// no-op for files that have no backup.
func Rollback(dir string) error {
	return atomicfile.RestoreDir(dir)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
