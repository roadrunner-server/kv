package kv

import (
	stderr "errors"
	"testing"

	kvV1 "github.com/roadrunner-server/api-go/v6/kv/v1"
	"github.com/roadrunner-server/api-plugins/v6/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const (
	// servedStorage is the storage name the rpc fixture declares in its config.
	servedStorage = "south"
	firstKey      = "alpha"
	secondKey     = "beta"
	rfc3339Expiry = "2026-01-01T00:00:00Z"
)

// rpcMethod is one entry of the goridge surface: the adapter method, the span it
// opens and the errors.Op it wraps driver failures with.
type rpcMethod struct {
	name string
	span string
	op   string
	call func(r *rpc, in *kvV1.Request, out *kvV1.Response) error
}

func rpcMethods() []rpcMethod {
	return []rpcMethod{
		{name: "has", span: "kv:has", op: "rpc_has", call: (*rpc).Has},
		{name: "set", span: "kv:set", op: "rpc_set", call: (*rpc).Set},
		{name: "mget", span: "kv:mget", op: "rpc_mget", call: (*rpc).MGet},
		{name: "mexpire", span: "kv:mexpire", op: "rpc_mexpire", call: (*rpc).MExpire},
		{name: "ttl", span: "kv:ttl", op: "rpc_ttl", call: (*rpc).TTL},
		{name: "delete", span: "kv:delete", op: "rpc_delete", call: (*rpc).Delete},
		{name: "clear", span: "kv:clear", op: "rpc_clear", call: (*rpc).Clear},
	}
}

// newRPC returns the rpc adapter of a plugin serving a single storage backed by
// st, together with the recorder collecting the spans that adapter emits.
func newRPC(t *testing.T, st kv.Storage) (*rpc, *tracetest.SpanRecorder) {
	t.Helper()

	p, _ := newInitedPlugin(t,
		map[string]any{servedStorage: map[string]any{"driver": "fake"}},
		map[string]bool{servedStorage: true},
	)

	tracer, rec := newSpanRecorder()
	collects := p.Collects()
	collects[1].Callback(tracer)
	collects[0].Callback(&fakeConstructor{name: "fake", storage: st})

	require.NoError(t, serveErr(p.Serve()))

	r, ok := p.RPC().(*rpc)
	require.True(t, ok)

	return r, rec
}

// twoItems is the request payload shared by the rpc tests: one item carrying a
// timeout and one without.
func twoItems() []*kvV1.Item {
	return []*kvV1.Item{
		{Key: firstKey, Value: []byte("a"), Timeout: rfc3339Expiry},
		{Key: secondKey, Value: []byte("b")},
	}
}

// responseKeys lists the keys of the items an rpc call wrote into out.
func responseKeys(out *kvV1.Response) []string {
	keys := make([]string, 0, len(out.GetItems()))
	for _, it := range out.GetItems() {
		keys = append(keys, it.GetKey())
	}

	return keys
}

func TestRPCSuccessPaths(t *testing.T) {
	sentItems := []itemSnapshot{
		{key: firstKey, value: []byte("a"), timeout: rfc3339Expiry},
		{key: secondKey, value: []byte("b")},
	}

	cases := []struct {
		name  string
		call  func(r *rpc, in *kvV1.Request, out *kvV1.Response) error
		check func(t *testing.T, calls recordedCalls, out *kvV1.Response)
	}{
		{
			name: "has answers one item per key the storage knows",
			call: (*rpc).Has,
			check: func(t *testing.T, calls recordedCalls, out *kvV1.Response) {
				assert.Equal(t, []string{firstKey, secondKey}, calls.hasKeys)
				assert.ElementsMatch(t, []string{firstKey, secondKey}, responseKeys(out))
			},
		},
		{
			name: "set forwards key, value and timeout of every item",
			call: (*rpc).Set,
			check: func(t *testing.T, calls recordedCalls, _ *kvV1.Response) {
				assert.Equal(t, sentItems, calls.setItems)
			},
		},
		{
			name: "mget answers the values the storage returned",
			call: (*rpc).MGet,
			check: func(t *testing.T, calls recordedCalls, out *kvV1.Response) {
				assert.Equal(t, []string{firstKey, secondKey}, calls.mgetKeys)

				values := make(map[string][]byte, len(out.GetItems()))
				for _, it := range out.GetItems() {
					values[it.GetKey()] = it.GetValue()
				}
				assert.Equal(t, map[string][]byte{firstKey: []byte("a"), secondKey: []byte("b")}, values)
			},
		},
		{
			name: "mexpire forwards key, value and timeout of every item",
			call: (*rpc).MExpire,
			check: func(t *testing.T, calls recordedCalls, _ *kvV1.Response) {
				assert.Equal(t, sentItems, calls.expireItems)
			},
		},
		{
			name: "ttl answers the timeouts the storage returned",
			call: (*rpc).TTL,
			check: func(t *testing.T, calls recordedCalls, out *kvV1.Response) {
				assert.Equal(t, []string{firstKey, secondKey}, calls.ttlKeys)

				timeouts := make(map[string]string, len(out.GetItems()))
				for _, it := range out.GetItems() {
					timeouts[it.GetKey()] = it.GetTimeout()
				}
				assert.Equal(t, map[string]string{firstKey: rfc3339Expiry}, timeouts)
			},
		},
		{
			name: "delete forwards the requested keys",
			call: (*rpc).Delete,
			check: func(t *testing.T, calls recordedCalls, _ *kvV1.Response) {
				assert.Equal(t, []string{firstKey, secondKey}, calls.deleteKeys)
			},
		},
		{
			name: "clear reaches the storage once",
			call: (*rpc).Clear,
			check: func(t *testing.T, calls recordedCalls, _ *kvV1.Response) {
				assert.Equal(t, 1, calls.clearCalls)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStorage{
				hasRet:  map[string]bool{firstKey: true, secondKey: true},
				mgetRet: map[string][]byte{firstKey: []byte("a"), secondKey: []byte("b")},
				ttlRet:  map[string]string{firstKey: rfc3339Expiry},
			}

			r, _ := newRPC(t, st)

			var out kvV1.Response
			require.NoError(t, tc.call(r, &kvV1.Request{Storage: servedStorage, Items: twoItems()}, &out))

			tc.check(t, st.recorded(), &out)
		})
	}
}

func TestRPCStorageLookup(t *testing.T) {
	for _, m := range rpcMethods() {
		t.Run(m.name, func(t *testing.T) {
			st := &fakeStorage{}
			r, _ := newRPC(t, st)

			var outMissingName kvV1.Response
			err := m.call(r, &kvV1.Request{Items: twoItems()}, &outMissingName)
			require.ErrorIs(t, err, errEmptyStorage)
			assert.Empty(t, outMissingName.GetItems())

			var outUnknownName kvV1.Response
			err = m.call(r, &kvV1.Request{Storage: "ghost", Items: twoItems()}, &outUnknownName)
			require.ErrorIs(t, err, errNoSuchStore)
			assert.ErrorContains(t, err, "ghost")
			assert.Empty(t, outUnknownName.GetItems())

			// the lookup fails before any driver call, so the storage serving the
			// configured name is left untouched
			assert.Equal(t, recordedCalls{}, st.recorded())
		})
	}
}

func TestRPCDriverErrorsRecordSpans(t *testing.T) {
	for _, m := range rpcMethods() {
		t.Run(m.name, func(t *testing.T) {
			r, rec := newRPC(t, &fakeStorage{err: stderr.New("driver is unhappy")})

			var out kvV1.Response
			err := m.call(r, &kvV1.Request{Storage: servedStorage, Items: twoItems()}, &out)
			require.Error(t, err)
			assert.ErrorContains(t, err, "driver is unhappy")
			assert.ErrorContains(t, err, m.op)

			ended := rec.Ended()
			require.Len(t, ended, 1)
			assert.Equal(t, m.span, ended[0].Name())

			require.Len(t, ended[0].Events(), 1)
			assert.Equal(t, "exception", ended[0].Events()[0].Name)
		})
	}
}
