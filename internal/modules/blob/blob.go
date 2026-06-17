// Package blob implements a generic, config-driven newest-wins module. Each
// configured resource maps a local source file to a destination path; the
// newest version (by mtime or content hash) is replicated across the cluster
// and written to every node, with an optional per-resource verify/reload hook.
//
// It is the second reference provider and proves the engine generalizes beyond
// certificates (IP block lists, WAF rules, haproxy maps, config fragments).
package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"

	"github.com/badbuka/lbsync/internal/atomicfile"
	"github.com/badbuka/lbsync/internal/engine"
	"github.com/badbuka/lbsync/internal/hook"
	"github.com/badbuka/lbsync/internal/metrics"
	"github.com/badbuka/lbsync/internal/module"
)

// Kind is the resource kind / DMap namespace for generic blobs.
const Kind = "blob"

func init() {
	module.Register("blob", func(cfg module.Config) (module.Module, error) {
		return &Module{resources: cfg.BlobResources, host: cfg.Hostname}, nil
	})
}

// Module wires the blob provider into the engine.
type Module struct {
	resources []module.BlobResource
	host      string
}

// Name implements module.Module.
func (m *Module) Name() string { return "blob" }

// Start builds the provider and registers it with the engine.
func (m *Module) Start(_ context.Context, d module.Deps) error {
	p := &provider{
		host:    m.host,
		metrics: d.Metrics,
		byName:  map[string]*resource{},
		pending: map[string]struct{}{},
	}
	for _, r := range m.resources {
		res := &resource{
			name:     r.Name,
			source:   r.Source,
			dest:     r.Dest,
			strategy: r.Strategy,
			runner: &hook.Runner{
				Kind:    Kind,
				Verify:  r.Verify,
				Reload:  r.Reload,
				Metrics: d.Metrics,
			},
		}
		p.resources = append(p.resources, res)
		p.byName[res.name] = res
	}
	d.Engine.Register(p)
	return nil
}

type resource struct {
	name     string
	source   string
	dest     string
	strategy string
	runner   *hook.Runner
}

type provider struct {
	resources []*resource
	byName    map[string]*resource
	host      string
	metrics   *metrics.Metrics
	pending   map[string]struct{}
}

func (p *provider) Kind() string { return Kind }

// Local reads each configured source file and builds a versioned record.
func (p *provider) Local(_ context.Context) ([]engine.Record, error) {
	p.pending = map[string]struct{}{}
	recs := make([]engine.Record, 0, len(p.resources))
	for _, r := range p.resources {
		data, err := os.ReadFile(r.source) //nolint:gosec // G304: operator-configured source
		if err != nil {
			continue
		}
		sha := sha256Hex(data)
		ver := engine.Version{Tiebreak: sha}
		if r.strategy != "sha" {
			if info, err := os.Stat(r.source); err == nil {
				ver.Primary = info.ModTime().Unix()
			}
		}
		recs = append(recs, engine.Record{
			Kind:    Kind,
			Key:     r.name,
			Version: ver,
			Blobs:   map[string][]byte{"data": data},
			Meta:    map[string]string{"sha256": sha},
			Source:  p.host,
		})
	}
	return recs, nil
}

// Apply writes the winning blob to the resource's destination.
func (p *provider) Apply(_ context.Context, r engine.Record) (bool, error) {
	res, ok := p.byName[r.Key]
	if !ok {
		// A resource present in the cluster but not configured locally is
		// skipped: we don't know where to write it on this node.
		return false, nil
	}
	changed, err := atomicfile.WriteAtomic(res.dest, r.Blobs["data"], 0o644)
	if err != nil {
		return false, err
	}
	if changed {
		p.pending[r.Key] = struct{}{}
	}
	return changed, nil
}

// Flush runs the verify/reload hook of every resource that changed this tick.
func (p *provider) Flush(ctx context.Context) error {
	var firstErr error
	for name := range p.pending {
		res := p.byName[name]
		if res == nil {
			continue
		}
		if err := res.runner.Run(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		p.pending = map[string]struct{}{}
	}
	return firstErr
}

// Rollback restores the previous destination files for the given resources.
func (p *provider) Rollback(_ context.Context, keys []string) error {
	var firstErr error
	for _, key := range keys {
		res, ok := p.byName[key]
		if !ok {
			continue
		}
		if err := atomicfile.Restore(res.dest); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
