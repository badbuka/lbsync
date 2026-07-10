// Package gobcodec provides generic gob encode/decode helpers shared by the
// cluster backends (internal/cluster, internal/wan).
package gobcodec

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// Encode gob-encodes v.
func Encode[T any](v T) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("gob encode: %w", err)
	}
	return buf.Bytes(), nil
}

// Decode gob-decodes raw into a T.
func Decode[T any](raw []byte) (T, error) {
	var v T
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&v); err != nil {
		var zero T
		return zero, fmt.Errorf("gob decode: %w", err)
	}
	return v, nil
}
