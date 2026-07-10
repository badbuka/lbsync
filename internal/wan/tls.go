package wan

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// buildTLSConfig returns (serverTLS, clientTLS, error).
//   - certFile == "": generate ephemeral self-signed cert, log warning
//   - caFile != "": mTLS — server requires client cert, client presents cert + verifies CA
//   - certFile set but caFile == "": client uses InsecureSkipVerify (TLS encryption only)
//   - certFile == "" AND caFile == "": dev mode, client uses InsecureSkipVerify
func buildTLSConfig(certFile, keyFile, caFile, hostname string) (*tls.Config, *tls.Config, error) {
	// Guard: mTLS requires explicit cert files.
	if caFile != "" && certFile == "" {
		return nil, nil, fmt.Errorf("wan: --tls-ca requires --tls-cert and --tls-key (mTLS needs a client certificate)")
	}

	var cert tls.Certificate
	var err error

	if certFile == "" {
		log.Println("WARNING: --tls-cert not provided; using ephemeral self-signed certificate. For production, set --tls-cert/--tls-key and --tls-ca.")
		cert, err = generateSelfSigned(hostname)
		if err != nil {
			return nil, nil, fmt.Errorf("generate self-signed cert: %w", err)
		}
	} else {
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("load cert/key: %w", err)
		}
	}

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	clientTLS := &tls.Config{}

	if caFile != "" {
		caPEM, err := os.ReadFile(filepath.Clean(caFile))
		if err != nil {
			return nil, nil, fmt.Errorf("read CA file: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPEM) {
			return nil, nil, fmt.Errorf("parse CA cert from %q", caFile)
		}
		// mTLS: server requires and verifies client cert.
		serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
		serverTLS.ClientCAs = caPool
		// Client presents cert and verifies server using CA pool.
		clientTLS.Certificates = []tls.Certificate{cert}
		clientTLS.RootCAs = caPool
	} else {
		// TLS encryption only — no CA to verify server cert.
		clientTLS.InsecureSkipVerify = true //nolint:gosec // intentional: no CA provided
	}

	return serverTLS, clientTLS, nil
}

// generateSelfSigned generates an in-memory ECDSA P-256 self-signed certificate
// valid for 1 year with CN=hostname and SANs: hostname, "localhost", "127.0.0.1".
func generateSelfSigned(hostname string) (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate serial: %w", err)
	}

	now := time.Now()
	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: hostname},
		NotBefore:    now,
		NotAfter:     now.Add(365 * 24 * time.Hour),
		DNSNames:     []string{hostname, "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	return tls.X509KeyPair(certPEM, keyPEM)
}
