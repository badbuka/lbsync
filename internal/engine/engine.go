// Package engine implements a generic "newest-wins" replication loop. Each
// Provider contributes resources of a single Kind; the engine reconciles the
// node's local copy against the cluster copy, publishes the newer one, applies
// the winner locally, and runs the provider's post-apply hook (verify+reload),
// rolling back on a verify failure.
package engine

import (
	"context"
	"time"

	"github.com/badbuka/lbsync/internal/metrics"
)

// Version is a total order describing "which copy wins". Larger is newer.
type Version struct {
	Primary   int64  // e.g. NotBefore unix, mtime, or an explicit sequence
	Secondary int64  // e.g. NotAfter unix
	Tiebreak  string // e.g. cert serial then sha256 (deterministic)
}

// Newer reports whether v is strictly newer than o.
func (v Version) Newer(o Version) bool {
	if v.Primary != o.Primary {
		return v.Primary > o.Primary
	}
	if v.Secondary != o.Secondary {
		return v.Secondary > o.Secondary
	}
	return v.Tiebreak > o.Tiebreak
}

// Record is one replicated item, namespaced by Kind.
type Record struct {
	Kind    string
	Key     string
	Version Version
	Blobs   map[string][]byte
	Meta    map[string]string
	Source  string
}

// Provider is implemented by every newest-wins module.
type Provider interface {
	// Kind returns the resource kind, used as the DMap/namespace name.
	Kind() string
	// Local enumerates the resources this node currently has on its input.
	Local(ctx context.Context) ([]Record, error)
	// Apply writes the winning record to local serving state. It reports
	// whether anything changed on disk.
	Apply(ctx context.Context, r Record) (changed bool, err error)
	// Flush runs the post-apply hook (verify then reload). It is called once
	// per tick after all applies, only if something changed.
	Flush(ctx context.Context) error
	// Rollback restores the given keys to their pre-tick on-disk copy. It is
	// called when Flush reports a verify failure.
	Rollback(ctx context.Context, keys []string) error
}

// ClusterKV is the subset of the cluster facade the engine needs.
type ClusterKV interface {
	Get(ctx context.Context, kind, key string) (*Record, bool, error)
	Put(ctx context.Context, r *Record) error
	Keys(ctx context.Context, kind string) ([]string, error)
	Lock(ctx context.Context, kind, key string, ttl time.Duration) (release func(), err error)
	Members() int
}

// Engine runs the reconcile loop over a set of providers.
type Engine struct {
	cl        ClusterKV
	providers []Provider
	interval  time.Duration
	lockTTL   time.Duration
	metrics   *metrics.Metrics
	now       func() time.Time
}

// Options configures an Engine.
type Options struct {
	Interval time.Duration
	LockTTL  time.Duration
	Now      func() time.Time
}

// New builds an Engine.
func New(cl ClusterKV, m *metrics.Metrics, opts Options) *Engine {
	if opts.Interval <= 0 {
		opts.Interval = 30 * time.Second
	}
	if opts.LockTTL <= 0 {
		opts.LockTTL = 10 * time.Second
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Engine{
		cl:       cl,
		interval: opts.Interval,
		lockTTL:  opts.LockTTL,
		metrics:  m,
		now:      now,
	}
}

// Register adds a provider. Not safe to call concurrently with Run.
func (e *Engine) Register(p Provider) {
	e.providers = append(e.providers, p)
}

// Run reconciles once immediately, then on every interval tick until ctx is
// cancelled.
func (e *Engine) Run(ctx context.Context) error {
	e.reconcile(ctx)

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			e.reconcile(ctx)
		}
	}
}

func (e *Engine) reconcile(ctx context.Context) {
	if e.metrics != nil {
		e.metrics.ClusterMembers.Set(float64(e.cl.Members()))
	}
	for _, p := range e.providers {
		e.tickProvider(ctx, p)
	}
}

func (e *Engine) tickProvider(ctx context.Context, p Provider) {
	kind := p.Kind()
	start := e.now()
	defer func() {
		if e.metrics != nil {
			e.metrics.ReconcileDur.WithLabelValues(kind).Observe(e.now().Sub(start).Seconds())
		}
	}()

	localByKey, keys := e.gatherKeys(ctx, p, kind)

	var changed []string
	for _, key := range keys {
		local := localByKey[key]
		clusterR, _, _ := e.cl.Get(ctx, kind, key)

		e.maybePublish(ctx, kind, key, local, clusterR)

		best := pickNewer(local, clusterR)
		if best == nil {
			continue
		}
		didChange, err := p.Apply(ctx, *best)
		if err != nil {
			continue
		}
		if didChange {
			changed = append(changed, key)
			if e.metrics != nil {
				e.metrics.ApplyTotal.WithLabelValues(kind).Inc()
				e.metrics.AppliedTS.WithLabelValues(kind, key).Set(float64(e.now().Unix()))
			}
		}
	}

	if len(changed) == 0 {
		return
	}
	if err := p.Flush(ctx); err != nil {
		_ = p.Rollback(ctx, changed)
		if e.metrics != nil {
			e.metrics.RollbackTotal.WithLabelValues(kind).Inc()
		}
	}
}

// gatherKeys returns this node's local records indexed by key plus the union of
// local and cluster keys, so receive-only nodes still apply cluster resources.
func (e *Engine) gatherKeys(ctx context.Context, p Provider, kind string) (map[string]*Record, []string) {
	localByKey := map[string]*Record{}
	if locals, err := p.Local(ctx); err == nil {
		for i := range locals {
			r := locals[i]
			localByKey[r.Key] = &r
		}
	}

	seen := map[string]struct{}{}
	keys := make([]string, 0, len(localByKey))
	for k := range localByKey {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	if clusterKeys, err := e.cl.Keys(ctx, kind); err == nil {
		for _, k := range clusterKeys {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
	}
	return localByKey, keys
}

func (e *Engine) maybePublish(ctx context.Context, kind, key string, local, clusterR *Record) {
	if local == nil {
		return
	}
	if clusterR != nil && !local.Version.Newer(clusterR.Version) {
		return
	}

	// Best-effort lock to narrow the publish race. If it cannot be acquired we
	// still publish, relying on per-tick re-publish for convergence.
	if release, err := e.cl.Lock(ctx, kind, key, e.lockTTL); err == nil {
		defer release()
	}

	// Re-check before writing to avoid clobbering a concurrently published
	// newer record.
	cur, ok, _ := e.cl.Get(ctx, kind, key)
	if ok && !local.Version.Newer(cur.Version) {
		return
	}
	if err := e.cl.Put(ctx, local); err != nil {
		return
	}
	if e.metrics != nil {
		e.metrics.PublishTotal.WithLabelValues(kind).Inc()
	}
}

// pickNewer returns the newer of two records (either may be nil).
func pickNewer(a, b *Record) *Record {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case a.Version.Newer(b.Version):
		return a
	default:
		return b
	}
}
