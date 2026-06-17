package cluster

import (
	"context"
	"time"
)

// The interfaces below are declared now so coordination modules (rate limiting,
// pub/sub, leader election) can be implemented later as a localized change
// against this package, without reworking the engine or module API. Olric
// provides the underlying primitives (atomic Incr/Decr, DTopic pub/sub, and
// distributed locks) used to satisfy them.

// Counter exposes fleet-wide atomic counters (e.g. for rate limiting).
type Counter interface {
	Incr(ctx context.Context, key string, delta int) (int, error)
	Decr(ctx context.Context, key string, delta int) (int, error)
}

// Topic exposes cluster-wide publish/subscribe (e.g. cache purge, feature
// flags, maintenance toggles).
type Topic interface {
	Publish(ctx context.Context, name string, msg []byte) error
	Subscribe(ctx context.Context, name string, handler func(msg []byte)) (cancel func(), err error)
}

// Leader exposes single-leader election (e.g. so only one node runs ACME
// renewal). Campaign returns whether this node became leader and a resign func.
type Leader interface {
	Campaign(ctx context.Context, name string, ttl time.Duration) (isLeader bool, resign func(), err error)
}
