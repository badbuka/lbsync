package cert

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAndLoadKey(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "privkey.pem")
	WriteTestCertKey(t, certPath, keyPath, 7, time.Now().Add(24*time.Hour), []string{"example.com", "www.example.com"})

	leaf, err := Load(certPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := PrimaryDomain(leaf, "fallback"); got != "example.com" {
		t.Fatalf("PrimaryDomain = %q, want example.com", got)
	}

	key, err := LoadKey(keyPath)
	if err != nil {
		t.Fatalf("LoadKey: %v", err)
	}
	if !LooksLikePrivateKey(key) {
		t.Fatalf("LoadKey returned bytes that do not look like a private key")
	}
}

func TestLoadKeyRejectsCert(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	WriteTestCert(t, certPath, time.Now().Add(24*time.Hour), []string{"example.com"})
	if _, err := LoadKey(certPath); err == nil {
		t.Fatal("LoadKey accepted a certificate file as a private key")
	}
}
