package tests

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	kvV2 "github.com/roadrunner-server/api-go/v6/kv/v2"
	"github.com/roadrunner-server/api-go/v6/kv/v2/kvV2connect"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/kv/v6"
	"github.com/roadrunner-server/logger/v6"
	"github.com/roadrunner-server/memory/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const kvAPIAddr = "127.0.0.1:6001"

// startKvAPIContainer brings up rpc + kv + memory storage on kvAPIAddr.
// Returns a stop function the test must defer.
func startKvAPIContainer(t *testing.T) func() {
	t.Helper()

	cont := endure.New(slog.LevelError)
	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-kv-api.yaml",
	}

	require.NoError(t, cont.RegisterAll(
		cfg,
		&logger.Plugin{},
		&memory.Plugin{},
		&rpcPlugin.Plugin{},
		&kv.Plugin{},
	))
	require.NoError(t, cont.Init())

	ch, err := cont.Serve()
	require.NoError(t, err)

	wg := &sync.WaitGroup{}
	stop := make(chan struct{})
	wg.Go(func() {
		select {
		case e := <-ch:
			require.NoError(t, e.Error, "container reported error")
		case <-stop:
		}
	})

	// give rpc server a beat to start listening
	time.Sleep(500 * time.Millisecond)

	return func() {
		close(stop)
		require.NoError(t, cont.Stop())
		wg.Wait()
	}
}

// TestKVHTTPApi exercises Set/Has/Delete/Has over the Connect-RPC HTTP/2
// cleartext wire — same shape PHP clients use.
func TestKVHTTPApi(t *testing.T) {
	stop := startKvAPIContainer(t)
	defer stop()

	httpc := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{Timeout: 30 * time.Second}).DialContext(ctx, network, addr)
			},
		},
	}
	client := kvV2connect.NewKvServiceClient(httpc, "http://"+kvAPIAddr)
	ctx := t.Context()

	const (
		store = "in-memory"
		key   = "http-key"
	)
	val := []byte("http-value")

	// Set
	_, err := client.Set(ctx, connect.NewRequest(&kvV2.KvRequest{
		Storage: store,
		Items:   []*kvV2.KvItem{{Key: key, Value: val}},
	}))
	require.NoError(t, err)

	// Has -> present
	resp, err := client.Has(ctx, connect.NewRequest(&kvV2.KvRequest{
		Storage: store,
		Items:   []*kvV2.KvItem{{Key: key}},
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.GetItems(), 1)
	require.Equal(t, key, resp.Msg.GetItems()[0].GetKey())

	// Delete
	_, err = client.Delete(ctx, connect.NewRequest(&kvV2.KvRequest{
		Storage: store,
		Items:   []*kvV2.KvItem{{Key: key}},
	}))
	require.NoError(t, err)

	// Has -> gone
	resp, err = client.Has(ctx, connect.NewRequest(&kvV2.KvRequest{
		Storage: store,
		Items:   []*kvV2.KvItem{{Key: key}},
	}))
	require.NoError(t, err)
	require.Empty(t, resp.Msg.GetItems())
}

// TestKVGRPCApi exercises the same Set/Has/Delete/Has cycle using a regular
// gRPC client (google.golang.org/grpc), not the Connect client. The same
// rpc-plugin handler serves both protocols because Connect handlers register
// gRPC bindings by default.
func TestKVGRPCApi(t *testing.T) {
	stop := startKvAPIContainer(t)
	defer stop()

	conn, err := grpc.NewClient(kvAPIAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := kvV2.NewKvServiceClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	const (
		store = "in-memory"
		key   = "grpc-key"
	)
	val := []byte("grpc-value")

	// Set
	_, err = client.Set(ctx, &kvV2.KvRequest{
		Storage: store,
		Items:   []*kvV2.KvItem{{Key: key, Value: val}},
	})
	require.NoError(t, err)

	// Has -> present
	hasResp, err := client.Has(ctx, &kvV2.KvRequest{
		Storage: store,
		Items:   []*kvV2.KvItem{{Key: key}},
	})
	require.NoError(t, err)
	require.Len(t, hasResp.GetItems(), 1)
	require.Equal(t, key, hasResp.GetItems()[0].GetKey())

	// Delete
	_, err = client.Delete(ctx, &kvV2.KvRequest{
		Storage: store,
		Items:   []*kvV2.KvItem{{Key: key}},
	})
	require.NoError(t, err)

	// Has -> gone
	hasResp, err = client.Has(ctx, &kvV2.KvRequest{
		Storage: store,
		Items:   []*kvV2.KvItem{{Key: key}},
	})
	require.NoError(t, err)
	require.Empty(t, hasResp.GetItems())
}
