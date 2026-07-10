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
	mux.HandleFunc("GET /v1/ping", b.handlePing)
	return mux
}

// handlePutRecord decodes the gob body as *engine.Record and stores it locally.
// CRITICAL: no fan-out here — fan-out is done in the public Put method only.
func (b *Backend) handlePutRecord(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	rec, err := decodeRecord(body)
	if err != nil {
		http.Error(w, "decode record", http.StatusBadRequest)
		return
	}
	k := storeKey(rec.Kind, rec.Key)

	b.mu.Lock()
	defer b.mu.Unlock()
	// Only overwrite if incoming record is strictly newer.
	if existing, ok := b.store[k]; ok {
		existingRec, err := decodeRecord(existing)
		if err == nil && !rec.Version.Newer(existingRec.Version) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	b.store[k] = body
	w.WriteHeader(http.StatusNoContent)
}

// handleGetRecord looks up (kind, key) in the local store and returns the
// gob-encoded record body, or 404 if absent.
func (b *Backend) handleGetRecord(w http.ResponseWriter, r *http.Request) {
	kind := r.PathValue("kind")
	key := r.PathValue("key")
	b.mu.RLock()
	raw, ok := b.store[storeKey(kind, key)]
	b.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
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
	b.mu.RLock()
	for sk := range b.store {
		if len(sk) > len(prefix) && sk[:len(prefix)] == prefix {
			keys = append(keys, sk[len(prefix):])
		}
	}
	b.mu.RUnlock()
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

// handlePing responds 200 "pong" for membership probing.
func (b *Backend) handlePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("pong"))
}
