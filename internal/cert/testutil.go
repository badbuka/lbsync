package cert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// WriteTestCert writes a self-signed PEM certificate to path.
func WriteTestCert(t *testing.T, path string, notAfter time.Time, dnsNames []string) {
	t.Helper()
	writeCertAndKey(t, path, "", big.NewInt(42), notAfter, dnsNames)
}

// WriteTestCertKey writes a self-signed PEM certificate to certPath and its
// matching PEM private key to keyPath.
func WriteTestCertKey(t *testing.T, certPath, keyPath string, serial int64, notAfter time.Time, dnsNames []string) {
	t.Helper()
	writeCertAndKey(t, certPath, keyPath, big.NewInt(serial), notAfter, dnsNames)
}

func writeCertAndKey(t *testing.T, certPath, keyPath string, serial *big.Int, notAfter time.Time, dnsNames []string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cn := ""
	if len(dnsNames) > 0 {
		cn = dnsNames[0]
	}
	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		Issuer:       pkix.Name{CommonName: "Test CA"},
		NotBefore:    notAfter.Add(-90 * 24 * time.Hour),
		NotAfter:     notAfter,
		DNSNames:     dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(certPath), 0o750); err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
	if keyPath == "" {
		return
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
}
