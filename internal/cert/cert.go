// Package cert loads X.509 certificates (PEM or DER) and extracts primary
// domain names from parsed certificate fields. It also loads the matching
// private key so that a full, serveable bundle can be assembled.
//
// The public-certificate parsing logic is adapted from
// github.com/badbuka/letsencrypt-exporter (MIT licensed); the private-key
// loading is added here because that upstream package deliberately reads only
// the public certificate.
package cert

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// Load reads a certificate file and returns the first X.509 certificate.
// PEM-encoded files may contain a chain; only the first CERTIFICATE block is
// used. DER-encoded files (common for .cer) are also supported.
func Load(path string) (*x509.Certificate, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: operator-configured cert path
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	parsed, err := parseCertificate(raw)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return parsed, nil
}

// LoadKey reads a PEM-encoded private key file and validates that it parses as
// a supported private key type (PKCS#1, PKCS#8, or SEC1/EC). It returns the raw
// PEM bytes on success so the caller can store/serve them verbatim.
func LoadKey(path string) ([]byte, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: operator-configured key path
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if !LooksLikePrivateKey(raw) {
		return nil, fmt.Errorf("%s: not a parseable private key", path)
	}
	return raw, nil
}

// LooksLikeCertificate reports whether raw contains a parseable X.509
// certificate in PEM or DER form.
func LooksLikeCertificate(raw []byte) bool {
	_, err := parseCertificate(raw)
	return err == nil
}

// LooksLikeCertificateFile reads path and reports whether it contains a
// certificate.
func LooksLikeCertificateFile(path string) bool {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: discovery reads operator-configured cert paths
	if err != nil {
		return false
	}
	return LooksLikeCertificate(raw)
}

// LooksLikePrivateKey reports whether raw contains a parseable PEM private key.
func LooksLikePrivateKey(raw []byte) bool {
	block, _ := pem.Decode(raw)
	if block == nil {
		return false
	}
	if _, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		return true
	}
	if _, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return true
	}
	if _, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return true
	}
	return false
}

func parseCertificate(raw []byte) (*x509.Certificate, error) {
	if block, _ := pem.Decode(raw); block != nil {
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("expected CERTIFICATE PEM block, got %q", block.Type)
		}
		return x509.ParseCertificate(block.Bytes)
	}
	return x509.ParseCertificate(raw)
}

// PrimaryDomain returns the best hostname label for a certificate: the first
// DNS SAN when present, otherwise the subject common name, otherwise fallback.
func PrimaryDomain(c *x509.Certificate, fallback string) string {
	if c == nil {
		return fallback
	}
	if len(c.DNSNames) > 0 {
		return c.DNSNames[0]
	}
	if cn := c.Subject.CommonName; cn != "" {
		return cn
	}
	return fallback
}
