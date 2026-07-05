package wan

import (
	"io"
	"net/http"
)

// newMux returns a *http.ServeMux with all routes registered.
// Uses Go 1.22+ method+path patterns with path parameters.
func newMux(b *Backend) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/records", b.handlePutRecord)
	mux.HandleFunc("GET /v1/records/{kind}/{key}", b.handleGetRecord)
	mux.HandleFunc("GET /v1/keys/{kind}", b.handleGetKeys)
	mux.HandleFunc("POST /v1/locks/{kind}/{key}", b.handlePostLock)
	mux.HandleFunc("GET /v1/ping", b.handlePing)
	return mux
}

// handlePutRecord decodes the gob body as *engine.Record and stores it locally.
// CRITICAL: no fan-out here — fan-out is done in the public Put method only.
func (b *Backend) handlePutRecord(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	rec, err := decodeRecord(body)
	if err != nil {
		http.Error(w, "decode error", http.StatusBadRequest)
		return
	}
	b.store.Store(storeKey(rec.Kind, rec.Key), body)
	w.WriteHeader(http.StatusNoContent)
}

// handleGetRecord looks up (kind, key) in the local store and returns the
// gob-encoded record body, or 404 if absent.
func (b *Backend) handleGetRecord(w http.ResponseWriter, r *http.Request) {
	kind := r.PathValue("kind")
	key := r.PathValue("key")
	val, ok := b.store.Load(storeKey(kind, key))
	if !ok {
		http.NotFound(w, r)
		return
	}
	raw := val.([]byte)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

// handleGetKeys scans the local store for all keys matching kind and returns
// them as a gob-encoded []string.
func (b *Backend) handleGetKeys(w http.ResponseWriter, r *http.Request) {
	kind := r.PathValue("kind")
	prefix := kind + "\x00"
	var keys []string
	b.store.Range(func(k, _ any) bool {
		sk := k.(string)
		if len(sk) > len(prefix) && sk[:len(prefix)] == prefix {
			keys = append(keys, sk[len(prefix):])
		}
		return true
	})
	if keys == nil {
		keys = []string{}
	}
	raw, err := encodeStrings(keys)
	if err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

// handlePostLock is a no-op placeholder for future distributed locking.
func (b *Backend) handlePostLock(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// handlePing responds 200 "pong" for membership probing.
func (b *Backend) handlePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("pong"))
}

