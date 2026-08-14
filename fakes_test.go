package kv

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/roadrunner-server/api-plugins/v6/kv"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// mockCfg satisfies Configurer.
type mockCfg struct {
	data         map[string]any  // returned through UnmarshalKey
	has          map[string]bool // drives Serve's local/global/default switch
	unmarshalErr error           // when set, UnmarshalKey fails (Init error path)
}

func (c *mockCfg) UnmarshalKey(_ string, out any) error {
	if c.unmarshalErr != nil {
		return c.unmarshalErr
	}

	if p, ok := out.(*map[string]any); ok {
		*p = c.data
	}

	return nil
}

func (c *mockCfg) Has(name string) bool { return c.has[name] }

// capHandler is a slog.Handler that records emitted records so tests can assert
// which warnings the plugin logged.
type capHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())

	return nil
}

func (h *capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capHandler) WithGroup(string) slog.Handler      { return h }

// hasWarn reports whether a warn-level record whose message contains sub was emitted.
func (h *capHandler) hasWarn(sub string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.ContainsFunc(h.records, func(r slog.Record) bool {
		return r.Level == slog.LevelWarn && strings.Contains(r.Message, sub)
	})
}

// mockLogger satisfies Logger.
type mockLogger struct{ h slog.Handler }

func (l *mockLogger) NamedLogger(string) *slog.Logger { return slog.New(l.h) }

// fakeTracer satisfies Tracer, the dependency the OTEL plugin fills in.
type fakeTracer struct{ tp *sdktrace.TracerProvider }

func (f *fakeTracer) Tracer() *sdktrace.TracerProvider { return f.tp }

// itemSnapshot holds what a kv.Item carried when the storage received it.
type itemSnapshot struct {
	key     string
	value   []byte
	timeout string
}

func snapshot(items []kv.Item) []itemSnapshot {
	out := make([]itemSnapshot, 0, len(items))
	for _, it := range items {
		out = append(out, itemSnapshot{key: it.Key(), value: it.Value(), timeout: it.Timeout()})
	}

	return out
}

// recordedCalls is what a fakeStorage was asked to do.
type recordedCalls struct {
	hasKeys     []string
	mgetKeys    []string
	ttlKeys     []string
	deleteKeys  []string
	setItems    []itemSnapshot
	expireItems []itemSnapshot
	clearCalls  int
	stopCalls   int
}

// fakeStorage implements kv.Storage. It records every call and answers from the
// canned maps; err, when set, is returned by every method that can fail.
// stopBlock, when set, holds Stop until the channel is closed.
type fakeStorage struct {
	hasRet    map[string]bool
	mgetRet   map[string][]byte
	ttlRet    map[string]string
	err       error
	stopBlock chan struct{}

	mu    sync.Mutex
	calls recordedCalls
}

// recorded returns a copy of the recorded calls, safe to read while the plugin
// still holds a reference to the storage.
func (f *fakeStorage) recorded() recordedCalls {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

func (f *fakeStorage) Has(_ context.Context, keys ...string) (map[string]bool, error) {
	f.mu.Lock()
	f.calls.hasKeys = keys
	f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	return f.hasRet, nil
}

func (f *fakeStorage) Get(context.Context, string) ([]byte, error) {
	return nil, f.err
}

func (f *fakeStorage) MGet(_ context.Context, keys ...string) (map[string][]byte, error) {
	f.mu.Lock()
	f.calls.mgetKeys = keys
	f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	return f.mgetRet, nil
}

func (f *fakeStorage) Set(_ context.Context, items ...kv.Item) error {
	f.mu.Lock()
	f.calls.setItems = snapshot(items)
	f.mu.Unlock()

	return f.err
}

func (f *fakeStorage) MExpire(_ context.Context, items ...kv.Item) error {
	f.mu.Lock()
	f.calls.expireItems = snapshot(items)
	f.mu.Unlock()

	return f.err
}

func (f *fakeStorage) TTL(_ context.Context, keys ...string) (map[string]string, error) {
	f.mu.Lock()
	f.calls.ttlKeys = keys
	f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	return f.ttlRet, nil
}

func (f *fakeStorage) Clear(context.Context) error {
	f.mu.Lock()
	f.calls.clearCalls++
	f.mu.Unlock()

	return f.err
}

func (f *fakeStorage) Delete(_ context.Context, keys ...string) error {
	f.mu.Lock()
	f.calls.deleteKeys = keys
	f.mu.Unlock()

	return f.err
}

func (f *fakeStorage) Stop(context.Context) {
	f.mu.Lock()
	f.calls.stopCalls++
	f.mu.Unlock()

	if f.stopBlock != nil {
		<-f.stopBlock
	}
}

// fakeConstructor implements kv.Constructor. It hands out storage when that
// field is set and otherwise builds a fresh fakeStorage per call, recording the
// config keys the plugin passed in.
type fakeConstructor struct {
	name    string
	storage kv.Storage
	err     error

	cfgKeys []string
	created []*fakeStorage
}

func (c *fakeConstructor) Name() string { return c.name }

func (c *fakeConstructor) KvFromConfig(_ context.Context, key string) (kv.Storage, error) {
	c.cfgKeys = append(c.cfgKeys, key)

	if c.err != nil {
		return nil, c.err
	}

	if c.storage != nil {
		return c.storage, nil
	}

	st := &fakeStorage{}
	c.created = append(c.created, st)

	return st, nil
}
