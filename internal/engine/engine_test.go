package engine

import (
	"context"
	"testing"
	"time"
)

type fakeKV struct {
	data map[string]map[string]*Record
}

func newFakeKV() *fakeKV { return &fakeKV{data: map[string]map[string]*Record{}} }

func (f *fakeKV) Get(_ context.Context, kind, key string) (*Record, bool, error) {
	m := f.data[kind]
	if m == nil {
		return nil, false, nil
	}
	r, ok := m[key]
	return r, ok, nil
}

func (f *fakeKV) Put(_ context.Context, r *Record) error {
	if f.data[r.Kind] == nil {
		f.data[r.Kind] = map[string]*Record{}
	}
	cp := *r
	f.data[r.Kind][r.Key] = &cp
	return nil
}

func (f *fakeKV) Keys(_ context.Context, kind string) ([]string, error) {
	keys := make([]string, 0, len(f.data[kind]))
	for k := range f.data[kind] {
		keys = append(keys, k)
	}
	return keys, nil
}

func (f *fakeKV) Lock(_ context.Context, _, _ string, _ time.Duration) (func(), error) {
	return func() {}, nil
}

func (f *fakeKV) Members() int { return 1 }

type fakeProvider struct {
	kind     string
	locals   []Record
	applied  []Record
	flushErr error
	flushes  int
	rollback [][]string
}

func (p *fakeProvider) Kind() string { return p.kind }

func (p *fakeProvider) Local(context.Context) ([]Record, error) { return p.locals, nil }

func (p *fakeProvider) Apply(_ context.Context, r Record) (bool, error) {
	p.applied = append(p.applied, r)
	return true, nil
}

func (p *fakeProvider) Flush(context.Context) error {
	p.flushes++
	return p.flushErr
}

func (p *fakeProvider) Rollback(_ context.Context, keys []string) error {
	p.rollback = append(p.rollback, keys)
	return nil
}

func rec(key string, primary int64) Record {
	return Record{Kind: "k", Key: key, Version: Version{Primary: primary}}
}

func newEngine(kv ClusterKV, p Provider) *Engine {
	e := New(kv, nil, Options{Interval: time.Hour, Now: func() time.Time { return time.Unix(0, 0) }})
	e.Register(p)
	return e
}

func TestLocalNewerPublishes(t *testing.T) {
	kv := newFakeKV()
	p := &fakeProvider{kind: "k", locals: []Record{rec("a", 100)}}
	newEngine(kv, p).reconcile(context.Background())

	got, ok, _ := kv.Get(context.Background(), "k", "a")
	if !ok || got.Version.Primary != 100 {
		t.Fatalf("expected local record published to cluster, got %+v ok=%v", got, ok)
	}
	if len(p.applied) != 1 {
		t.Fatalf("expected apply called once, got %d", len(p.applied))
	}
	if p.flushes != 1 {
		t.Fatalf("expected flush once, got %d", p.flushes)
	}
}

func TestClusterNewerApplies(t *testing.T) {
	kv := newFakeKV()
	_ = kv.Put(context.Background(), &Record{Kind: "k", Key: "a", Version: Version{Primary: 200}})
	// local is older
	p := &fakeProvider{kind: "k", locals: []Record{rec("a", 100)}}
	newEngine(kv, p).reconcile(context.Background())

	if len(p.applied) != 1 || p.applied[0].Version.Primary != 200 {
		t.Fatalf("expected cluster (newer) record applied, got %+v", p.applied)
	}
	// cluster must not be overwritten with the older local
	got, _, _ := kv.Get(context.Background(), "k", "a")
	if got.Version.Primary != 200 {
		t.Fatalf("cluster regressed to %d", got.Version.Primary)
	}
}

func TestReceiveOnlyNodeApplies(t *testing.T) {
	kv := newFakeKV()
	_ = kv.Put(context.Background(), &Record{Kind: "k", Key: "a", Version: Version{Primary: 200}})
	// no locals at all (receive-only node)
	p := &fakeProvider{kind: "k"}
	newEngine(kv, p).reconcile(context.Background())

	if len(p.applied) != 1 || p.applied[0].Key != "a" {
		t.Fatalf("receive-only node should apply cluster record, got %+v", p.applied)
	}
}

func TestVerifyFailureRollsBack(t *testing.T) {
	kv := newFakeKV()
	p := &fakeProvider{kind: "k", locals: []Record{rec("a", 100)}, flushErr: context.DeadlineExceeded}
	newEngine(kv, p).reconcile(context.Background())

	if len(p.rollback) != 1 {
		t.Fatalf("expected rollback once on flush error, got %d", len(p.rollback))
	}
	if len(p.rollback[0]) != 1 || p.rollback[0][0] != "a" {
		t.Fatalf("expected rollback of key a, got %+v", p.rollback)
	}
}
