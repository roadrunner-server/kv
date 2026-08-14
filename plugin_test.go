package kv

import (
	"context"
	stderr "errors"
	"maps"
	"slices"
	"testing"

	kvV1 "github.com/roadrunner-server/api-go/v6/kv/v1"
	rrerrors "github.com/roadrunner-server/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newInitedPlugin returns a plugin initialized over the given kv section and
// Has() answers. The "kv" section is always reported as present so Init succeeds.
func newInitedPlugin(t *testing.T, data map[string]any, has map[string]bool) (*Plugin, *capHandler) {
	t.Helper()

	if has == nil {
		has = map[string]bool{}
	}
	has[PluginName] = true

	h := &capHandler{}
	p := &Plugin{}
	require.NoError(t, p.Init(&mockCfg{data: data, has: has}, &mockLogger{h: h}))

	return p, h
}

// serveErr reads the buffered Serve channel without blocking: an error means
// Serve aborted, nil means every storage was processed (saved or skipped).
func serveErr(errCh chan error) error {
	select {
	case e := <-errCh:
		return e
	default:
		return nil
	}
}

// newSpanRecorder returns a tracer plugin writing into the returned recorder.
func newSpanRecorder() (*fakeTracer, *tracetest.SpanRecorder) {
	rec := tracetest.NewSpanRecorder()

	return &fakeTracer{tp: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))}, rec
}

func TestPluginServeBranches(t *testing.T) {
	cases := []struct {
		name         string
		data         map[string]any
		has          map[string]bool
		constructor  *fakeConstructor
		errSubs      []string // substrings the Serve error must contain
		warnSub      string   // substring of a warn record, when non-empty
		wantStorages []string
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
			errSubs: []string{"could not find mandatory driver field"},
		},
		{
			name:    "non-string driver warns and skips",
			data:    map[string]any{"s": map[string]any{"driver": 123}},
			warnSub: "driver field is not a string",
		},
		{
			name:    "unknown driver via local config errors",
			data:    map[string]any{"s": map[string]any{"driver": "nope"}},
			has:     map[string]bool{"kv.s.config": true},
			errSubs: []string{"no such constructor"},
		},
		{
			name:    "unknown driver via global config errors",
			data:    map[string]any{"s": map[string]any{"driver": "nope"}},
			has:     map[string]bool{"s": true},
			errSubs: []string{"no such constructor"},
		},
		{
			name:    "unknown driver via default branch warns then errors",
			data:    map[string]any{"s": map[string]any{"driver": "nope"}},
			errSubs: []string{"no such constructor"},
			warnSub: "can't find local or global",
		},
		{
			name:        "constructor failure aborts serve",
			data:        map[string]any{"s": map[string]any{"driver": "fake"}},
			has:         map[string]bool{"s": true},
			constructor: &fakeConstructor{name: "fake", err: stderr.New("driver is unhappy")},
			errSubs:     []string{"kv_plugin_serve", "driver is unhappy"},
		},
		{
			name:         "storage is filed under its config name",
			data:         map[string]any{"s": map[string]any{"driver": "fake"}},
			has:          map[string]bool{"s": true},
			constructor:  &fakeConstructor{name: "fake"},
			wantStorages: []string{"s"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, h := newInitedPlugin(t, tc.data, tc.has)
			if tc.constructor != nil {
				p.Collects()[0].Callback(tc.constructor)
			}

			err := serveErr(p.Serve())
			if len(tc.errSubs) > 0 {
				require.Error(t, err)
				for _, sub := range tc.errSubs {
					assert.Contains(t, err.Error(), sub)
				}
			} else {
				require.NoError(t, err)
			}

			if tc.warnSub != "" {
				assert.Truef(t, h.hasWarn(tc.warnSub), "expected a warn record containing %q", tc.warnSub)
			}

			assert.ElementsMatch(t, tc.wantStorages, slices.Collect(maps.Keys(p.storages)))
		})
	}
}

func TestPluginCollects(t *testing.T) {
	p, _ := newInitedPlugin(t, map[string]any{"south": map[string]any{"driver": "fake"}}, map[string]bool{"south": true})

	collects := p.Collects()
	require.Len(t, collects, 2)

	tracer, rec := newSpanRecorder()
	collects[1].Callback(tracer)

	ctor := &fakeConstructor{name: "fake"}
	collects[0].Callback(ctor)

	require.NoError(t, serveErr(p.Serve()))
	assert.Equal(t, []string{"south"}, ctor.cfgKeys)
	require.Len(t, ctor.created, 1)

	// The storage the collected constructor built answers through the rpc
	// adapter, and the adapter traces into the collected provider.
	r, ok := p.RPC().(*rpc)
	require.True(t, ok)
	require.NoError(t, r.Clear(&kvV1.Request{Storage: "south"}, &kvV1.Response{}))
	assert.Equal(t, 1, ctor.created[0].recorded().clearCalls)

	ended := rec.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, "kv:clear", ended[0].Name())
}

func TestPluginInitDisabled(t *testing.T) {
	p := &Plugin{}
	err := p.Init(&mockCfg{has: map[string]bool{}}, &mockLogger{h: &capHandler{}})
	require.Error(t, err)
	assert.Truef(t, rrerrors.Is(rrerrors.Disabled, err), "expected Disabled kind, got %v", err)
}

func TestPluginInitUnmarshalError(t *testing.T) {
	p := &Plugin{}
	err := p.Init(&mockCfg{
		has:          map[string]bool{PluginName: true},
		unmarshalErr: stderr.New("boom"),
	}, &mockLogger{h: &capHandler{}})
	require.Error(t, err)
	assert.ErrorContains(t, err, "boom")
}

func TestPluginWeightName(t *testing.T) {
	p := &Plugin{}
	assert.Equal(t, uint(10), p.Weight())
	assert.Equal(t, "kv", p.Name())
}

func TestPluginStopNoStorages(t *testing.T) {
	p, _ := newInitedPlugin(t, map[string]any{}, nil)
	require.NoError(t, p.Stop(t.Context()))
}

func TestPluginStopStopsStorages(t *testing.T) {
	p, _ := newInitedPlugin(t, map[string]any{
		"north": map[string]any{"driver": "fake"},
		"south": map[string]any{"driver": "fake"},
	}, map[string]bool{"north": true, "south": true})

	ctor := &fakeConstructor{name: "fake"}
	p.Collects()[0].Callback(ctor)
	require.NoError(t, serveErr(p.Serve()))
	require.Len(t, ctor.created, 2)

	require.NoError(t, p.Stop(t.Context()))

	for _, st := range ctor.created {
		assert.Equal(t, 1, st.recorded().stopCalls)
	}
	assert.Empty(t, p.storages)
}

func TestPluginStopCancelledContext(t *testing.T) {
	// The storage never returns from Stop, so the stopCh arm of the select can
	// never become ready and the ctx.Done arm is the only reachable one.
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	p, _ := newInitedPlugin(t, map[string]any{"south": map[string]any{"driver": "fake"}}, map[string]bool{"south": true})
	p.Collects()[0].Callback(&fakeConstructor{name: "fake", storage: &fakeStorage{stopBlock: blocked}})
	require.NoError(t, serveErr(p.Serve()))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	assert.ErrorIs(t, p.Stop(ctx), context.Canceled)
}
