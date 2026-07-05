// Package wan implements an HTTPS-based distributed key-value store that
// satisfies the engine.ClusterKV interface. It is intended as a WAN cluster
// backend for nodes communicating over the public internet, replacing the
// LAN-optimised Olric backend.
package wan

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/badbuka/lbsync/internal/engine"
)

// Config configures the WAN HTTPS cluster backend.
type Config struct {
	BindAddr  string      // listening address; default "0.0.0.0"
	Port      int         // WAN HTTPS listen port
	Peers     []string    // peer "host:port" HTTPS addresses
	CertFile  string      // PEM cert path; "" → auto-generate ephemeral cert
	KeyFile   string      // PEM key path (required when CertFile is set)
	CAFile    string      // PEM CA path for mTLS; "" → no mTLS
	Hostname  string      // CN for the auto-generated cert; default "localhost"
	TLSConfig *tls.Config // server TLS override (for tests; takes precedence over cert files)
}

// Backend implements engine.ClusterKV over HTTPS.
type Backend struct {
	addr   string       // "bindaddr:port" used for self-identification
	peers  []string     // peer "host:port" list
	store  sync.Map     // storeKey(kind,key) → []byte (gob-encoded *engine.Record)
	server *http.Server
	client *http.Client // shared TLS client for peer calls
}

// storeKey returns the composite key used in b.store.
func storeKey(kind, key string) string { return kind + "\x00" + key }

// New starts the HTTPS listener and returns a ready Backend.
// The server runs in a background goroutine; ctx cancellation does not stop it
// — call Close for graceful shutdown.
func New(ctx context.Context, cfg Config) (*Backend, error) {
	bindAddr := cfg.BindAddr
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	hostname := cfg.Hostname
	if hostname == "" {
		hostname = "localhost"
	}
	addr := net.JoinHostPort(bindAddr, strconv.Itoa(cfg.Port))

	b := &Backend{
		addr:  addr,
		peers: cfg.Peers,
	}

	var serverTLS *tls.Config
	var clientTLS *tls.Config

	if cfg.TLSConfig != nil {
		// Test override: use the provided TLS config for the server.
		// For the client, skip verification (tests use self-signed certs).
		serverTLS = cfg.TLSConfig
		clientTLS = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test override
	} else {
		var err error
		serverTLS, clientTLS, err = buildTLSConfig(cfg.CertFile, cfg.KeyFile, cfg.CAFile, hostname)
		if err != nil {
			return nil, fmt.Errorf("wan: build TLS config: %w", err)
		}
	}

	mux := newMux(b)
	b.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		TLSConfig:         serverTLS,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	go func() {
		if err := b.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			log.Printf("wan: server error: %v", err)
		}
	}()

	b.client = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: clientTLS,
		},
		Timeout: 10 * time.Second,
	}

	return b, nil
}

// Get returns the record for (kind, key).
// It checks the local store first; on a miss it queries each peer in order and
// returns the first successful response.
func (b *Backend) Get(ctx context.Context, kind, key string) (*engine.Record, bool, error) {
	if val, ok := b.store.Load(storeKey(kind, key)); ok {
		rec, err := decodeRecord(val.([]byte))
		if err != nil {
			return nil, false, err
		}
		return rec, true, nil
	}

	for _, peer := range b.peers {
		url := fmt.Sprintf("https://%s/v1/records/%s/%s", peer, kind, key)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}
		resp, err := b.client.Do(req)
		if err != nil {
			continue
		}
		if resp.StatusCode == http.StatusOK {
			var rec engine.Record
			decErr := gob.NewDecoder(resp.Body).Decode(&rec)
			_ = resp.Body.Close()
			if decErr != nil {
				continue
			}
			return &rec, true, nil
		}
		_ = resp.Body.Close()
	}
	return nil, false, nil
}

// Put stores r locally and fans out to all peers concurrently (best-effort).
func (b *Backend) Put(ctx context.Context, r *engine.Record) error {
	raw, err := encodeRecord(r)
	if err != nil {
		return err
	}
	b.store.Store(storeKey(r.Kind, r.Key), raw)

	for _, peer := range b.peers {
		peer := peer
		go func() {
			// Use an independent 5s timeout so caller cancellation does not
			// abort the fan-out (best-effort convergence).
			pctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			url := fmt.Sprintf("https://%s/v1/records", peer)
			req, err := http.NewRequestWithContext(pctx, http.MethodPut, url, bytes.NewReader(raw))
			if err != nil {
				log.Printf("wan: put peer %s: build request: %v", peer, err)
				return
			}
			resp, err := b.client.Do(req)
			if err != nil {
				log.Printf("wan: put peer %s: %v", peer, err)
				return
			}
			_ = resp.Body.Close()
		}()
	}
	return nil
}

// Keys returns the deduplicated union of local and peer keys for kind.
func (b *Backend) Keys(ctx context.Context, kind string) ([]string, error) {
	prefix := kind + "\x00"
	seen := map[string]struct{}{}

	b.store.Range(func(k, _ any) bool {
		sk := k.(string)
		if len(sk) > len(prefix) && sk[:len(prefix)] == prefix {
			seen[sk[len(prefix):]] = struct{}{}
		}
		return true
	})

	for _, peer := range b.peers {
		url := fmt.Sprintf("https://%s/v1/keys/%s", peer, kind)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}
		resp, err := b.client.Do(req)
		if err != nil {
			continue
		}
		if resp.StatusCode == http.StatusOK {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr != nil {
				continue
			}
			keys, decErr := decodeStrings(body)
			if decErr != nil {
				continue
			}
			for _, k := range keys {
				seen[k] = struct{}{}
			}
		} else {
			_ = resp.Body.Close()
		}
	}

	result := make([]string, 0, len(seen))
	for k := range seen {
		result = append(result, k)
	}
	return result, nil
}

// Lock is a no-op; the engine handles lock failures gracefully.
func (b *Backend) Lock(_ context.Context, _, _ string, _ time.Duration) (func(), error) {
	return func() {}, nil
}

// Members returns the count of reachable peers (HTTP 200 on /v1/ping) plus 1
// for self. Pings are issued concurrently with a 3s timeout each.
func (b *Backend) Members() int {
	ch := make(chan int, len(b.peers))
	for _, peer := range b.peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			url := fmt.Sprintf("https://%s/v1/ping", peer)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				ch <- 0
				return
			}
			resp, err := b.client.Do(req)
			if err != nil {
				ch <- 0
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ch <- 1
			} else {
				ch <- 0
			}
		}()
	}
	count := 1 // self
	for range b.peers {
		count += <-ch
	}
	return count
}

// Close shuts down the HTTPS server gracefully.
func (b *Backend) Close(ctx context.Context) error {
	return b.server.Shutdown(ctx)
}

// ---- gob helpers (adapted from internal/cluster/cluster.go) ----

func encodeRecord(r *engine.Record) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		return nil, fmt.Errorf("gob encode record: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeRecord(raw []byte) (*engine.Record, error) {
	var r engine.Record
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&r); err != nil {
		return nil, fmt.Errorf("gob decode record: %w", err)
	}
	return &r, nil
}

func encodeStrings(s []string) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return nil, fmt.Errorf("gob encode strings: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeStrings(raw []byte) ([]string, error) {
	var s []string
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&s); err != nil {
		return nil, fmt.Errorf("gob decode strings: %w", err)
	}
	return s, nil
}
