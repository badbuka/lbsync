// Package cluster wraps an embedded Olric member and exposes a small facade
// used by the engine (key/value with per-key locking and membership).
package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/olric-data/olric"
	"github.com/olric-data/olric/config"

	"github.com/badbuka/lbsync/internal/engine"
	"github.com/badbuka/lbsync/internal/gobcodec"
)

// Config configures the embedded Olric member.
type Config struct {
	Env            string // "local" | "lan" | "wan"
	BindAddr       string
	BindPort       int
	MemberlistPort int
	AdvertiseAddr  string
	Peers          []string
	Password       string
	GossipKey      []byte // memberlist SecretKey (16, 24, or 32 bytes); optional
	ReplicaCount   int
}

// Cluster is the embedded Olric member plus a cached set of DMaps (one per
// resource kind).
type Cluster struct {
	db     *olric.Olric
	client olric.Client

	mu    sync.Mutex
	dmaps map[string]olric.DMap
}

// NewCluster starts an embedded Olric member, joins the configured peers, and
// blocks until the node is ready to accept connections (or ctx is cancelled).
func NewCluster(ctx context.Context, cfg Config) (*Cluster, error) {
	env := cfg.Env
	if env == "" {
		env = "lan"
	}
	c := config.New(env)
	c.BindAddr = cfg.BindAddr
	c.BindPort = cfg.BindPort
	c.MemberlistConfig.BindAddr = cfg.BindAddr
	c.MemberlistConfig.BindPort = cfg.MemberlistPort
	c.MemberlistConfig.AdvertisePort = cfg.MemberlistPort
	if cfg.AdvertiseAddr != "" {
		c.MemberlistConfig.AdvertiseAddr = cfg.AdvertiseAddr
	}
	c.Peers = cfg.Peers
	if cfg.Password != "" {
		c.Authentication = &config.Authentication{Password: cfg.Password}
	}
	if len(cfg.GossipKey) > 0 {
		c.MemberlistConfig.SecretKey = cfg.GossipKey
	}
	if cfg.ReplicaCount > 0 {
		c.ReplicaCount = cfg.ReplicaCount
		c.ReadQuorum = 1
		c.WriteQuorum = 1
	}

	started := make(chan struct{})
	c.Started = func() { close(started) }

	db, err := olric.New(c)
	if err != nil {
		return nil, fmt.Errorf("olric.New: %w", err)
	}

	startErr := make(chan error, 1)
	go func() {
		if err := db.Start(); err != nil {
			startErr <- err
		}
	}()

	select {
	case <-started:
	case err := <-startErr:
		return nil, fmt.Errorf("olric.Start: %w", err)
	case <-ctx.Done():
		_ = db.Shutdown(context.Background())
		return nil, ctx.Err()
	}

	return &Cluster{
		db:     db,
		client: db.NewEmbeddedClient(),
		dmaps:  map[string]olric.DMap{},
	}, nil
}

func (c *Cluster) dmap(kind string) (olric.DMap, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if dm, ok := c.dmaps[kind]; ok {
		return dm, nil
	}
	dm, err := c.client.NewDMap(kind)
	if err != nil {
		return nil, fmt.Errorf("new dmap %q: %w", kind, err)
	}
	c.dmaps[kind] = dm
	return dm, nil
}

// Get returns the record stored for (kind,key). The bool is false when the key
// is absent.
func (c *Cluster) Get(ctx context.Context, kind, key string) (*engine.Record, bool, error) {
	dm, err := c.dmap(kind)
	if err != nil {
		return nil, false, err
	}
	resp, err := dm.Get(ctx, key)
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get %q/%q: %w", kind, key, err)
	}
	raw, err := resp.Byte()
	if err != nil {
		return nil, false, fmt.Errorf("decode get %q/%q: %w", kind, key, err)
	}
	rec, err := decodeRecord(raw)
	if err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

// Reserved keys used to maintain a per-kind key index. Olric's embedded Scan
// requires an authenticated internal cluster client which it does not provide,
// so we cannot enumerate a DMap with auth enabled. Instead we keep an explicit
// index (a stored list of keys) updated on every Put, which only needs
// Get/Put/Lock. The index converges even if an update is lost, because each
// owner re-publishes (and re-indexes) its keys every reconcile tick.
const (
	indexKey     = "__lbsync_index__"
	indexLockKey = "__lbsync_index_lock__"
)

// Put stores r under (r.Kind, r.Key) and records the key in the kind's index.
func (c *Cluster) Put(ctx context.Context, r *engine.Record) error {
	dm, err := c.dmap(r.Kind)
	if err != nil {
		return err
	}
	raw, err := encodeRecord(r)
	if err != nil {
		return err
	}
	if err := dm.Put(ctx, r.Key, raw); err != nil {
		return fmt.Errorf("put %q/%q: %w", r.Kind, r.Key, err)
	}
	return c.addToIndex(ctx, dm, r.Key)
}

// Keys returns all resource keys currently recorded in kind's index.
func (c *Cluster) Keys(ctx context.Context, kind string) ([]string, error) {
	dm, err := c.dmap(kind)
	if err != nil {
		return nil, err
	}
	return c.readIndex(ctx, dm)
}

func (c *Cluster) readIndex(ctx context.Context, dm olric.DMap) ([]string, error) {
	resp, err := dm.Get(ctx, indexKey)
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("read index: %w", err)
	}
	raw, err := resp.Byte()
	if err != nil {
		return nil, fmt.Errorf("decode index: %w", err)
	}
	return decodeStrings(raw)
}

func (c *Cluster) addToIndex(ctx context.Context, dm olric.DMap, key string) error {
	// Best-effort lock to serialize index updates; on failure we still proceed,
	// relying on per-tick re-publish for convergence.
	if lc, err := dm.LockWithTimeout(ctx, indexLockKey, 10*time.Second, 5*time.Second); err == nil {
		defer func() { _ = lc.Unlock(context.Background()) }()
	}

	keys, err := c.readIndex(ctx, dm)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if k == key {
			return nil
		}
	}
	keys = append(keys, key)
	raw, err := encodeStrings(keys)
	if err != nil {
		return err
	}
	if err := dm.Put(ctx, indexKey, raw); err != nil {
		return fmt.Errorf("put index: %w", err)
	}
	return nil
}

// lockPrefix namespaces lock entries away from data keys. Olric implements a
// lock as an NX Put on the key itself, so locking a key that already holds a
// value always fails; we lock a derived key instead.
const lockPrefix = "__lbsync_lock__:"

// Lock acquires an approximate, auto-expiring lock for (kind,key). The returned
// release function unlocks it. ttl bounds both how long we wait to acquire and
// how long the lock is held before auto-release.
func (c *Cluster) Lock(ctx context.Context, kind, key string, ttl time.Duration) (func(), error) {
	dm, err := c.dmap(kind)
	if err != nil {
		return nil, err
	}
	lc, err := dm.LockWithTimeout(ctx, lockPrefix+key, ttl, ttl)
	if err != nil {
		return nil, fmt.Errorf("lock %q/%q: %w", kind, key, err)
	}
	return func() {
		_ = lc.Unlock(context.Background())
	}, nil
}

// Members returns the number of members currently in the cluster.
func (c *Cluster) Members() int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	members, err := c.client.Members(ctx)
	if err != nil {
		return 0
	}
	return len(members)
}

// Close shuts down the embedded member and leaves the cluster.
func (c *Cluster) Close(ctx context.Context) error {
	return c.db.Shutdown(ctx)
}

func encodeRecord(r *engine.Record) ([]byte, error) { return gobcodec.Encode(r) }

func decodeRecord(raw []byte) (*engine.Record, error) {
	r, err := gobcodec.Decode[engine.Record](raw)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func encodeStrings(s []string) ([]byte, error) { return gobcodec.Encode(s) }

func decodeStrings(raw []byte) ([]string, error) { return gobcodec.Decode[[]string](raw) }
