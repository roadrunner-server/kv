package tests

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	rrerrors "github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/kv/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests drive the kv.Plugin directly (no Endure container) to cover the
// configuration-validation branches of Serve()/Init() that the integration
// tests never reach — they always feed well-formed config.

// mockCfg satisfies kv.Configurer.
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

// mockLogger satisfies kv.Logger.
type mockLogger struct{ h slog.Handler }

func (l *mockLogger) NamedLogger(string) *slog.Logger { return slog.New(l.h) }

// newServingPlugin returns an Init'd plugin backed by the given config data and
// Has() answers. The "kv" section is always present so Init succeeds.
func newServingPlugin(t *testing.T, data map[string]any, has map[string]bool) (*kv.Plugin, *capHandler) {
	t.Helper()
	if has == nil {
		has = map[string]bool{}
	}
	has[kv.PluginName] = true

	h := &capHandler{}
	p := &kv.Plugin{}
	require.NoError(t, p.Init(&mockCfg{data: data, has: has}, &mockLogger{h: h}))
	return p, h
}

// serveErr reads the (buffered) Serve channel without blocking: an error means
// Serve aborted, nil means every storage was processed (saved or skipped).
func serveErr(errCh chan error) error {
	select {
	case e := <-errCh:
		return e
	default:
		return nil
	}
}

func TestPluginServeBranches(t *testing.T) {
	cases := []struct {
		name    string
		data    map[string]any
		has     map[string]bool
		wantErr bool
		errSub  string // expected substring of the error (when wantErr)
		warnSub string // expected substring of a warn record (when non-empty)
	}{
		{
			name: "nil value is skipped",
			data: map[string]any{"s": nil},
		},
		{
			name:    "non-map value warns and skips",
			data:    map[string]any{"s": "not-a-map"},
			warnSub: "wrong type detected",
		},
		{
			name:    "missing driver field errors",
			data:    map[string]any{"s": map[string]any{}},
			wantErr: true,
			errSub:  "could not find mandatory driver field",
		},
		{
			// the branch added by this PR: a non-string driver is now logged and
			// skipped instead of being silently ignored.
			name:    "non-string driver warns and skips",
			data:    map[string]any{"s": map[string]any{"driver": 123}},
			warnSub: "driver field is not a string",
		},
		{
			name:    "unknown driver via local config errors",
			data:    map[string]any{"s": map[string]any{"driver": "nope"}},
			has:     map[string]bool{"kv.s.config": true},
			wantErr: true,
			errSub:  "no such constructor",
		},
		{
			name:    "unknown driver via global config errors",
			data:    map[string]any{"s": map[string]any{"driver": "nope"}},
			has:     map[string]bool{"s": true},
			wantErr: true,
			errSub:  "no such constructor",
		},
		{
			name:    "unknown driver via default branch warns then errors",
			data:    map[string]any{"s": map[string]any{"driver": "nope"}},
			wantErr: true,
			errSub:  "no such constructor",
			warnSub: "can't find local or global",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, h := newServingPlugin(t, tc.data, tc.has)

			err := serveErr(p.Serve())
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errSub)
			} else {
				require.NoError(t, err)
			}

			if tc.warnSub != "" {
				assert.Truef(t, h.hasWarn(tc.warnSub), "expected a warn record containing %q", tc.warnSub)
			}
		})
	}
}

func TestPluginInitDisabled(t *testing.T) {
	p := &kv.Plugin{}
	err := p.Init(&mockCfg{has: map[string]bool{}}, &mockLogger{h: &capHandler{}})
	require.Error(t, err)
	assert.Truef(t, rrerrors.Is(rrerrors.Disabled, err), "expected Disabled kind, got %v", err)
}

func TestPluginInitUnmarshalError(t *testing.T) {
	p := &kv.Plugin{}
	err := p.Init(&mockCfg{
		has:          map[string]bool{kv.PluginName: true},
		unmarshalErr: errors.New("boom"),
	}, &mockLogger{h: &capHandler{}})
	require.Error(t, err)
	assert.ErrorContains(t, err, "boom")
}

func TestPluginWeightName(t *testing.T) {
	p := &kv.Plugin{}
	assert.Equal(t, uint(10), p.Weight())
	assert.Equal(t, "kv", p.Name())
}

func TestPluginStopNoStorages(t *testing.T) {
	p, _ := newServingPlugin(t, map[string]any{}, nil)
	require.NoError(t, p.Stop(t.Context()))
}
