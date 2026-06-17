// Package certs implements the flagship newest-wins module: it discovers
// certbot lineages, replicates the full bundle (cert + key + chain) across the
// cluster, writes the newest bundle for each lineage to the serving directory,
// and runs a verify-then-reload hook (rolling back on verify failure).
package certs

import (
	"context"
	"path/filepath"
	"strconv"

	"github.com/badbuka/lbsync/internal/bundle"
	"github.com/badbuka/lbsync/internal/discovery"
	"github.com/badbuka/lbsync/internal/engine"
	"github.com/badbuka/lbsync/internal/hook"
	"github.com/badbuka/lbsync/internal/metrics"
	"github.com/badbuka/lbsync/internal/module"
)

// Kind is the resource kind / DMap namespace for certificates.
const Kind = "certs"

func init() {
	module.Register("certs", func(cfg module.Config) (module.Module, error) {
		return &Module{cfg: cfg}, nil
	})
}

// Module wires the certs provider into the engine.
type Module struct {
	cfg module.Config
}

// Name implements module.Module.
func (m *Module) Name() string { return "certs" }

// Start builds the provider and registers it with the engine.
func (m *Module) Start(_ context.Context, d module.Deps) error {
	p := &provider{
		root:       m.cfg.LetsencryptPath,
		servingDir: m.cfg.ServingDir,
		host:       m.cfg.Hostname,
		metrics:    d.Metrics,
		runner: &hook.Runner{
			Kind:    Kind,
			Verify:  m.cfg.VerifyCmd,
			Reload:  m.cfg.ReloadCmd,
			Timeout: m.cfg.ReloadTimeout,
			Metrics: d.Metrics,
		},
	}
	d.Engine.Register(p)
	return nil
}

type provider struct {
	root       string
	servingDir string
	host       string
	metrics    *metrics.Metrics
	runner     *hook.Runner
}

func (p *provider) Kind() string { return Kind }

// Local enumerates certbot lineages and reads each full bundle from
// <root>/live/<lineage>.
func (p *provider) Local(_ context.Context) ([]engine.Record, error) {
	certs, err := discovery.ScanAll(discovery.Config{CertbotRoot: p.root})
	if err != nil {
		return nil, err
	}
	recs := make([]engine.Record, 0, len(certs))
	for _, c := range certs {
		lineage := c.FallbackID
		dir := filepath.Join(p.root, "live", lineage)
		b, err := bundle.ReadLineage(lineage, dir)
		if err != nil {
			continue
		}
		recs = append(recs, toRecord(b, p.host))
	}
	return recs, nil
}

// Apply writes the winning bundle for a lineage into the serving directory.
func (p *provider) Apply(_ context.Context, r engine.Record) (bool, error) {
	b := fromRecord(r)
	dir := filepath.Join(p.servingDir, r.Key)
	changed, err := b.WriteAtomic(dir)
	if err != nil {
		return false, err
	}
	if changed && p.metrics != nil {
		if na, err := strconv.ParseInt(r.Meta["not_after"], 10, 64); err == nil {
			p.metrics.NotAfter.WithLabelValues(Kind, r.Key).Set(float64(na))
		}
	}
	return changed, nil
}

// Flush runs the verify-then-reload hook once per tick.
func (p *provider) Flush(ctx context.Context) error {
	return p.runner.Run(ctx)
}

// Rollback restores the previous serving files for the given lineages.
func (p *provider) Rollback(_ context.Context, keys []string) error {
	var firstErr error
	for _, key := range keys {
		if err := bundle.Rollback(filepath.Join(p.servingDir, key)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func toRecord(b *bundle.Bundle, host string) engine.Record {
	return engine.Record{
		Kind: Kind,
		Key:  b.Lineage,
		Version: engine.Version{
			Primary:   b.Version.NotBefore.Unix(),
			Secondary: b.Version.NotAfter.Unix(),
			Tiebreak:  b.Version.Serial + ":" + b.Version.SHA256,
		},
		Blobs: map[string][]byte{
			"cert":  b.CertPEM,
			"key":   b.KeyPEM,
			"chain": b.ChainPEM,
			"full":  b.FullPEM,
		},
		Meta: map[string]string{
			"domain":    b.Domain,
			"serial":    b.Version.Serial,
			"sha256":    b.Version.SHA256,
			"not_after": strconv.FormatInt(b.Version.NotAfter.Unix(), 10),
		},
		Source: host,
	}
}

func fromRecord(r engine.Record) *bundle.Bundle {
	return &bundle.Bundle{
		Lineage:  r.Key,
		Domain:   r.Meta["domain"],
		CertPEM:  r.Blobs["cert"],
		KeyPEM:   r.Blobs["key"],
		ChainPEM: r.Blobs["chain"],
		FullPEM:  r.Blobs["full"],
	}
}
