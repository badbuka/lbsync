// Package wan implements a WAN-safe cluster backend for lbsync using HTTPS (TLS/mTLS).
//
// Storage model: Each node stores only records it originated or received via fan-out.
// This is origin-authoritative: when a node publishes a record, it fans it out to all
// configured peers; peers store it locally. There is no replication of records not
// explicitly pushed — if the origin node is down, other nodes cannot retrieve the record
// via Get/Keys until the origin comes back and re-publishes.
//
// For the cert/blob use case this is acceptable: each node republishes its local files
// every reconcile tick, so a missed fan-out is corrected on the next tick.
//
// Single-origin-per-key assumption: handlePutRecord (incoming peer pushes) uses a
// version guard to prevent stale writes. Two nodes originating the same key
// concurrently may cause transient disagreement; the engine's maybePublish re-check
// provides eventual convergence.
package wan

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	neturl "net/url"
	"strconv"
	"sync"
	"time"

	"github.com/badbuka/lbsync/internal/engine"
	"github.com/badbuka/lbsync/internal/gobcodec"
)

// Compile-time assertion: Backend must satisfy engine.ClusterKV.
var _ engine.ClusterKV = (*Backend)(nil)

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
	addr  string   // "bindaddr:port" used for self-identification
	peers []string // peer "host:port" list

	mu    sync.RWMutex
	store map[string][]byte // storeKey(kind,key) → gob-encoded *engine.Record

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
		store: make(map[string][]byte),
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

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("wan: listen %s: %w", addr, err)
	}
	tlsLn := tls.NewListener(ln, serverTLS)
	go func() {
		if err := b.server.Serve(tlsLn); err != nil && err != http.ErrServerClosed {
			log.Printf("wan: server error: %v", err)
		}
	}()

	b.client = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   clientTLS,
			ForceAttemptHTTP2: true,
		},
		Timeout: 10 * time.Second,
	}

	return b, nil
}

// Get returns the record for (kind, key).
// It checks the local store first; on a miss it queries each peer in order and
// returns the first successful response.
func (b *Backend) Get(ctx context.Context, kind, key string) (*engine.Record, bool, error) {
	b.mu.RLock()
	val, ok := b.store[storeKey(kind, key)]
	b.mu.RUnlock()
	if ok {
		rec, err := decodeRecord(val)
		if err != nil {
			return nil, false, err
		}
		return rec, true, nil
	}

	for _, peer := range b.peers {
		rawURL := fmt.Sprintf("https://%s/v1/records/%s/%s", peer, neturl.PathEscape(kind), neturl.PathEscape(key))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
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
			rec, decErr := decodeRecord(body)
			if decErr != nil {
				continue
			}
			return rec, true, nil
		}
		_, _ = io.Copy(io.Discard, resp.Body)
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
	b.mu.Lock()
	b.store[storeKey(r.Kind, r.Key)] = raw
	b.mu.Unlock()

	// Detach from ctx's cancellation (but keep its values) so caller
	// cancellation does not abort the fan-out (best-effort convergence).
	detached := context.WithoutCancel(ctx)
	for _, peer := range b.peers {
		go func() {
			pctx, cancel := context.WithTimeout(detached, 5*time.Second)
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

	b.mu.RLock()
	for sk := range b.store {
		if len(sk) > len(prefix) && sk[:len(prefix)] == prefix {
			seen[sk[len(prefix):]] = struct{}{}
		}
	}
	b.mu.RUnlock()

	for _, peer := range b.peers {
		rawURL := fmt.Sprintf("https://%s/v1/keys/%s", peer, neturl.PathEscape(kind))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
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
			_, _ = io.Copy(io.Discard, resp.Body)
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

func encodeRecord(r *engine.Record) ([]byte, error) { return gobcodec.Encode(r) }

func decodeRecord(raw []byte) (*engine.Record, error) {
	r, err := gobcodec.Decode[engine.Record](raw)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func encodeStrings(s []string) ([]byte, error) { return gobcodec.Encode(s) }

func decodeStrings(raw []byte) ([]string, error) { return gobcodec.Decode[[]string](raw) }
