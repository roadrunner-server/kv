package tests

import (
	"net/rpc"
	"os"
	"testing"

	kvV1 "github.com/roadrunner-server/api-go/v6/kv/v1"
	"github.com/roadrunner-server/boltdb/v6"
	"github.com/roadrunner-server/kv/v6"
	"github.com/roadrunner-server/memcached/v6"
	"github.com/roadrunner-server/memory/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tests/helpers"
)

// rpcAddr is the listener every config in configs/ binds.
const rpcAddr = "127.0.0.1:6371"

// cleanDBs removes the boltdb files the configs point at, once before the test
// runs and once after its containers have been stopped.
func cleanDBs(t *testing.T) {
	t.Helper()

	remove := func() {
		for _, f := range []string{"rr.db", "africa.db"} {
			require.NoError(t, os.RemoveAll(f))
		}
	}

	remove()
	t.Cleanup(remove)
}

// setItem stores value under key in the named storage.
func setItem(t *testing.T, client *rpc.Client, storage, key, value string) {
	t.Helper()

	var out kvV1.Response
	require.NoError(t, client.Call("kv.Set", &kvV1.Request{
		Storage: storage,
		Items:   []*kvV1.Item{{Key: key, Value: []byte(value)}},
	}, &out))
}

func keyItems(keys []string) []*kvV1.Item {
	items := make([]*kvV1.Item, 0, len(keys))
	for _, k := range keys {
		items = append(items, &kvV1.Item{Key: k})
	}

	return items
}

// mget reads the given keys from the named storage as a key/value map.
func mget(t *testing.T, client *rpc.Client, storage string, keys ...string) map[string]string {
	t.Helper()

	var out kvV1.Response
	require.NoError(t, client.Call("kv.MGet", &kvV1.Request{Storage: storage, Items: keyItems(keys)}, &out))

	values := make(map[string]string, len(out.GetItems()))
	for _, it := range out.GetItems() {
		values[it.GetKey()] = string(it.GetValue())
	}

	return values
}

// hasKeys returns the subset of keys the named storage reports.
func hasKeys(t *testing.T, client *rpc.Client, storage string, keys ...string) []string {
	t.Helper()

	var out kvV1.Response
	require.NoError(t, client.Call("kv.Has", &kvV1.Request{Storage: storage, Items: keyItems(keys)}, &out))

	found := make([]string, 0, len(out.GetItems()))
	for _, it := range out.GetItems() {
		found = append(found, it.GetKey())
	}

	return found
}

func TestKVStorageRouting(t *testing.T) {
	cleanDBs(t)

	helpers.Start(t, "configs/.rr-kv-init.yaml", []any{
		&memory.Plugin{},
		&boltdb.Plugin{},
		&memcached.Plugin{},
		&rpcPlugin.Plugin{},
		&kv.Plugin{},
	}, helpers.WithTCPProbe(rpcAddr))

	client := helpers.RPC(t, rpcAddr)

	setItem(t, client, "default", "in-memory", "memory value")
	setItem(t, client, "boltdb-south", "on-disk", "bolt value")

	assert.Equal(t, map[string]string{"in-memory": "memory value"}, mget(t, client, "default", "in-memory"))
	assert.Equal(t, map[string]string{"on-disk": "bolt value"}, mget(t, client, "boltdb-south", "on-disk"))
	assert.Equal(t, []string{"in-memory"}, hasKeys(t, client, "default", "in-memory"))

	// every configured name addresses its own driver instance, so a key written
	// through one is invisible through the other
	assert.Empty(t, mget(t, client, "default", "on-disk"))
	assert.Empty(t, mget(t, client, "boltdb-south", "in-memory"))
	assert.Empty(t, hasKeys(t, client, "boltdb-south", "in-memory"))
}

func TestKVUnknownStorageOverRPC(t *testing.T) {
	cleanDBs(t)

	helpers.Start(t, "configs/.rr-kv-init.yaml", []any{
		&memory.Plugin{},
		&boltdb.Plugin{},
		&memcached.Plugin{},
		&rpcPlugin.Plugin{},
		&kv.Plugin{},
	}, helpers.WithTCPProbe(rpcAddr))

	var out kvV1.Response
	err := helpers.RPC(t, rpcAddr).Call("kv.Set", &kvV1.Request{
		Storage: "ghost",
		Items:   []*kvV1.Item{{Key: "key", Value: []byte("val")}},
	}, &out)
	require.Error(t, err)
	assert.ErrorContains(t, err, "no such storage: ghost")
}

func TestBoltReopenKeepsData(t *testing.T) {
	cleanDBs(t)

	// boltdb-africa is opened with permissions: 0777. The resulting file mode is
	// not asserted: the process umask masks it, so the value is not portable.
	plugins := func() []any {
		return []any{&boltdb.Plugin{}, &rpcPlugin.Plugin{}, &kv.Plugin{}}
	}

	stop := helpers.Start(t, "configs/.rr-kv-bolt-perms.yaml", plugins(), helpers.WithTCPProbe(rpcAddr))

	beforeRestart := helpers.RPC(t, rpcAddr)
	setItem(t, beforeRestart, "boltdb-africa", "survivor", "written before restart")
	require.NoError(t, beforeRestart.Close())
	stop()

	helpers.Start(t, "configs/.rr-kv-bolt-perms.yaml", plugins(), helpers.WithTCPProbe(rpcAddr))

	assert.Equal(t,
		map[string]string{"survivor": "written before restart"},
		mget(t, helpers.RPC(t, rpcAddr), "boltdb-africa", "survivor"),
	)
}

func TestKVNoInterval(t *testing.T) {
	cleanDBs(t)

	helpers.Start(t, "configs/.rr-kv-bolt-no-interval.yaml", []any{
		&boltdb.Plugin{},
		&rpcPlugin.Plugin{},
		&kv.Plugin{},
	}, helpers.WithTCPProbe(rpcAddr))

	client := helpers.RPC(t, rpcAddr)

	setItem(t, client, "boltdb-south", "key", "value")
	assert.Equal(t, map[string]string{"key": "value"}, mget(t, client, "boltdb-south", "key"))
	assert.Equal(t, []string{"key"}, hasKeys(t, client, "boltdb-south", "key"))
}
