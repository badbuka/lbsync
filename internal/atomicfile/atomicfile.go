// Package atomicfile writes files atomically (temp file + rename) while keeping
// a single "<name>.bak" backup of any replaced file, so a failed downstream
// step (e.g. a config verify) can be rolled back.
package atomicfile

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// BackupSuffix is appended to a file's name to store its previous contents.
const BackupSuffix = ".bak"

// WriteAtomic writes data to path via a temp file + rename. When the existing
// content is identical it is left untouched and false is returned. When the
// file is replaced, its previous contents are first copied to path+BackupSuffix.
func WriteAtomic(path string, data []byte, perm os.FileMode) (bool, error) {
	if existing, err := os.ReadFile(path); err == nil { //nolint:gosec // G304: managed path
		if bytes.Equal(existing, data) {
			return false, nil
		}
		if err := copyFile(path, path+BackupSuffix, perm); err != nil {
			return false, err
		}
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return false, fmt.Errorf("create temp for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("write temp for %s: %w", path, err)
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("chmod temp for %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return false, fmt.Errorf("close temp for %s: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return false, fmt.Errorf("rename temp to %s: %w", path, err)
	}
	return true, nil
}

// Restore restores path+BackupSuffix over path if the backup exists.
func Restore(path string) error {
	bak := path + BackupSuffix
	if _, err := os.Stat(bak); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := os.Rename(bak, path); err != nil {
		return fmt.Errorf("restore %s: %w", path, err)
	}
	return nil
}

// RestoreDir restores every "<name>.bak" file in dir over its original.
func RestoreDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read %s: %w", dir, err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, BackupSuffix) {
			continue
		}
		orig := filepath.Join(dir, strings.TrimSuffix(name, BackupSuffix))
		if err := os.Rename(filepath.Join(dir, name), orig); err != nil {
			return fmt.Errorf("restore %s: %w", orig, err)
		}
	}
	return nil
}

func copyFile(src, dst string, perm os.FileMode) error {
	data, err := os.ReadFile(src) //nolint:gosec // G304: managed path
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	if err := os.WriteFile(dst, data, perm); err != nil { //nolint:gosec // G703: managed backup path
		return fmt.Errorf("write %s: %w", dst, err)
	}
	return nil
}
