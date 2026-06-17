// Package stubs registers placeholder modules for capabilities that the agent
// is designed to support but does not yet implement. Each is a real
// module.Module that logs an explanatory message at Start and documents the
// cluster primitive it will use, so enabling it later is a localized change.
package stubs

import (
	"context"

	"github.com/badbuka/lbsync/internal/module"
)

func init() {
	for name, intent := range intents {
		n, i := name, intent
		module.Register(n, func(_ module.Config) (module.Module, error) {
			return &stub{name: n, intent: i}, nil
		})
	}
}

// intents documents how each future module will use the cluster primitives
// declared in internal/cluster (Counter, Topic, Leader).
var intents = map[string]string{
	"acme-leader": "elect a single node (cluster.Leader) to run certbot/ACME renewal; pairs with the certs module to avoid duplicate ACME orders",
	"ratelimit":   "enforce fleet-wide rate limits using cluster.Counter (atomic Incr/Decr) plus an HTTP check endpoint",
	"ocsp":        "fetch and share OCSP staples as a newest-wins engine.Provider (kind=ocsp)",
	"pubsub":      "fan out cache purge / feature flags / maintenance toggles using cluster.Topic",
	"health":      "share upstream health and membership via short-TTL cluster keys",
}

type stub struct {
	name   string
	intent string
}

func (s *stub) Name() string { return s.name }

func (s *stub) Start(_ context.Context, d module.Deps) error {
	if d.Logf != nil {
		d.Logf("module %q is not implemented yet (planned: %s)", s.name, s.intent)
	}
	return nil
}
