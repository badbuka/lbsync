package wan

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/badbuka/lbsync/internal/engine"
)

// testTLSConfig returns a *tls.Config with a self-signed cert suitable for
// the HTTPS server. When Config.TLSConfig is non-nil, wan.New uses it for the
// server and automatically sets InsecureSkipVerify on the client side.
func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	cert, err := generateSelfSigned("localhost")
	if err != nil {
		t.Fatalf("testTLSConfig: %v", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
}

// waitForPort polls until the TCP port is open or 5 s elapses.
func waitForPort(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("port %d did not open within 5s", port)
}

// startTestBackend starts a Backend on 127.0.0.1:port and waits for it to accept connections.
func startTestBackend(t *testing.T, port int, peers []string) *Backend {
	t.Helper()
	srv, err := New(context.Background(), Config{
		BindAddr:  "127.0.0.1",
		Port:      port,
		Peers:     peers,
		TLSConfig: testTLSConfig(t),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	waitForPort(t, port)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Close(ctx)
	})
	return srv
}

func TestPutGet(t *testing.T) {
	b := startTestBackend(t, 5420, nil)
	rec := &engine.Record{
		Kind:    "certs",
		Key:     "example.com",
		Version: engine.Version{Primary: 100},
		Blobs:   map[string][]byte{"cert": []byte("CERT")},
	}
	if err := b.Put(context.Background(), rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := b.Get(context.Background(), "certs", "example.com")
	if err != nil || !ok || got.Version.Primary != 100 {
		t.Fatalf("Get: ok=%v err=%v got=%+v", ok, err, got)
	}
	if string(got.Blobs["cert"]) != "CERT" {
		t.Fatalf("Get: blob mismatch, got %q", got.Blobs["cert"])
	}
}

func TestKeys(t *testing.T) {
	b := startTestBackend(t, 5421, nil)
	ctx := context.Background()

	for _, key := range []string{"alpha.com", "beta.com"} {
		rec := &engine.Record{
			Kind:    "certs",
			Key:     key,
			Version: engine.Version{Primary: 1},
			Blobs:   map[string][]byte{"cert": []byte("X")},
		}
		if err := b.Put(ctx, rec); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	keys, err := b.Keys(ctx, "certs")
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "alpha.com" || keys[1] != "beta.com" {
		t.Fatalf("Keys: got %v, want [alpha.com beta.com]", keys)
	}
}

func TestLock(t *testing.T) {
	b := startTestBackend(t, 5422, nil)
	release, err := b.Lock(context.Background(), "certs", "example.com", 5*time.Second)
	if err != nil {
		t.Fatalf("Lock returned error: %v", err)
	}
	release() // must not panic
}

func TestMembersSelf(t *testing.T) {
	b := startTestBackend(t, 5423, nil)
	if m := b.Members(); m != 1 {
		t.Fatalf("Members() = %d, want 1", m)
	}
}

func TestTwoNodeFanOut(t *testing.T) {
	// Start both nodes with each other as peers, then Put on A and verify B receives it.
	aAddr := "127.0.0.1:5424"
	bAddr := "127.0.0.1:5425"

	a := startTestBackend(t, 5424, []string{bAddr})
	b := startTestBackend(t, 5425, []string{aAddr})
	_ = b // ensure B is started and its port is ready

	rec := &engine.Record{
		Kind:    "certs",
		Key:     "fanout.com",
		Version: engine.Version{Primary: 42},
		Blobs:   map[string][]byte{"cert": []byte("DATA")},
	}
	if err := a.Put(context.Background(), rec); err != nil {
		t.Fatalf("Put on A: %v", err)
	}

	// Poll B for up to 5 seconds for the fan-out to arrive.
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, ok, err := b.Get(ctx, "certs", "fanout.com")
		if err == nil && ok {
			if got.Version.Primary != 42 {
				t.Fatalf("fan-out record mismatch: %+v", got)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fan-out did not reach node B (ok=%v err=%v)", ok, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestTwoNodeKeysUnion(t *testing.T) {
	aAddr := "127.0.0.1:5426"
	bAddr := "127.0.0.1:5427"

	a := startTestBackend(t, 5426, []string{bAddr})
	b := startTestBackend(t, 5427, []string{aAddr})

	ctx := context.Background()

	if err := a.Put(ctx, &engine.Record{
		Kind: "certs", Key: "akey.com",
		Version: engine.Version{Primary: 1},
		Blobs:   map[string][]byte{"cert": []byte("A")},
	}); err != nil {
		t.Fatalf("Put on A: %v", err)
	}
	if err := b.Put(ctx, &engine.Record{
		Kind: "certs", Key: "bkey.com",
		Version: engine.Version{Primary: 1},
		Blobs:   map[string][]byte{"cert": []byte("B")},
	}); err != nil {
		t.Fatalf("Put on B: %v", err)
	}

	// Keys aggregates local store + peer query, so the union should be visible immediately.
	// But fan-out is async; retry briefly to let it settle.
	deadline := time.Now().Add(5 * time.Second)
	for {
		keysA, err := a.Keys(ctx, "certs")
		if err != nil {
			t.Fatalf("Keys on A: %v", err)
		}
		sort.Strings(keysA)
		if len(keysA) == 2 && keysA[0] == "akey.com" && keysA[1] == "bkey.com" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("keys union not complete on A: %v", keysA)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
