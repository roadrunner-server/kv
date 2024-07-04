package tests

import (
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	kvProto "github.com/roadrunner-server/api/v4/build/kv/v1"
	"github.com/roadrunner-server/boltdb/v4"
	"github.com/roadrunner-server/config/v4"
	"github.com/roadrunner-server/endure/v2"
	goridgeRpc "github.com/roadrunner-server/goridge/v3/pkg/rpc"
	"github.com/roadrunner-server/kv/v5"
	"github.com/roadrunner-server/logger/v4"
	"github.com/roadrunner-server/memcached/v4"
	"github.com/roadrunner-server/memory/v4"
	"github.com/roadrunner-server/redis/v4"
	rpcPlugin "github.com/roadrunner-server/rpc/v4"
	"github.com/stretchr/testify/assert"
)

func TestKVInit(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-kv-init.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&memory.Plugin{},
		&boltdb.Plugin{},
		&memcached.Plugin{},
		&redis.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&kv.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 1)
	t.Run("KvSetTest", kvSetTest)
	t.Run("KvHasTest", kvHasTest)

	stopCh <- struct{}{}

	wg.Wait()

	t.Cleanup(func() {
		_ = os.RemoveAll("rr.db")
		_ = os.RemoveAll("africa.db")
	})
}

func TestKVNoInterval(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-kv-bolt-no-interval.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&boltdb.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&kv.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 1)
	t.Run("KvSetTest", kvSetTest)
	t.Run("KvHasTest", kvHasTest)

	stopCh <- struct{}{}

	wg.Wait()

	_ = os.RemoveAll("rr.db")
	_ = os.RemoveAll("africa.db")
}

func TestKVCreateToReopenWithPerms(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-kv-bolt-perms.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&boltdb.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&kv.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 1)
	stopCh <- struct{}{}
	wg.Wait()
}

func TestKVCreateToReopenWithPerms2(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-kv-bolt-perms.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&boltdb.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&kv.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 1)
	t.Run("KvSetTest", kvSetTest)
	t.Run("KvHasTest", kvHasTest)

	stopCh <- struct{}{}

	wg.Wait()

	t.Cleanup(func() {
		_ = os.RemoveAll("rr.db")
		_ = os.RemoveAll("africa.db")
	})
}

func kvSetTest(t *testing.T) {
	conn, err := net.Dial("tcp", "127.0.0.1:6001")
	assert.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	// WorkerList contains a list of workers.
	p := &kvProto.Request{
		Storage: "boltdb-south",
		Items: []*kvProto.Item{
			{
				Key:   "key",
				Value: []byte("val"),
			},
		},
	}

	resp := &kvProto.Response{}
	err = client.Call("kv.Set", p, resp)
	assert.NoError(t, err)
}

func kvHasTest(t *testing.T) {
	conn, err := net.Dial("tcp", "127.0.0.1:6001")
	assert.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	// WorkerList contains a list of workers.
	p := &kvProto.Request{
		Storage: "boltdb-south",
		Items: []*kvProto.Item{
			{
				Key:   "key",
				Value: []byte("val"),
			},
		},
	}

	ret := &kvProto.Response{}
	err = client.Call("kv.Has", p, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 1)
}
