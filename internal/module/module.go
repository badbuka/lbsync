// Package module defines the pluggable module abstraction for the lbsync agent
// and a registry that concrete modules self-register into via init(). The
// registry deliberately does not import the concrete module packages, so there
// is no import cycle: main imports the module packages for their side-effect
// registration and then asks this package to build the enabled set.
package module

import (
	"context"
	"fmt"
	"time"

	"github.com/badbuka/lbsync/internal/cluster"
	"github.com/badbuka/lbsync/internal/engine"
	"github.com/badbuka/lbsync/internal/metrics"
)

// Deps are the shared dependencies handed to every module at Start.
type Deps struct {
	Cluster *cluster.Cluster
	Engine  *engine.Engine
	Metrics *metrics.Metrics
	Logf    func(format string, args ...any)
}

// Module is a long-lived capability. Newest-wins modules register an
// engine.Provider with d.Engine inside Start; coordination modules use the
// cluster primitives.
type Module interface {
	Name() string
	Start(ctx context.Context, d Deps) error
}

// BlobResource describes one generic newest-wins file managed by the blob
// module.
type BlobResource struct {
	Name     string
	Source   string
	Dest     string
	Strategy string // "mtime" (default) or "sha"
	Verify   []string
	Reload   []string
}

// Config carries all module configuration. Each factory reads the fields it
// needs.
type Config struct {
	LetsencryptPath string
	ServingDir      string
	VerifyCmd       []string
	ReloadCmd       []string
	ReloadTimeout   time.Duration
	BlobResources   []BlobResource
	Hostname        string
}

// Factory builds a module from config.
type Factory func(cfg Config) (Module, error)

var registry = map[string]Factory{}

// Register adds a factory under name. Intended to be called from init().
func Register(name string, f Factory) {
	registry[name] = f
}

// Registered reports whether a module name is known.
func Registered(name string) bool {
	_, ok := registry[name]
	return ok
}

// Build instantiates the named modules in order.
func Build(names []string, cfg Config) ([]Module, error) {
	out := make([]Module, 0, len(names))
	for _, name := range names {
		f, ok := registry[name]
		if !ok {
			return nil, fmt.Errorf("unknown module %q", name)
		}
		m, err := f(cfg)
		if err != nil {
			return nil, fmt.Errorf("build module %q: %w", name, err)
		}
		out = append(out, m)
	}
	return out, nil
}
