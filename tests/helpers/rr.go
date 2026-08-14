package helpers

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/logger/v6"
	"github.com/stretchr/testify/require"
)

const (
	// configVersion is the config schema version used by the test configs.
	configVersion = "2024.2.0"
	// probeTimeout caps how long Start waits for the probe to answer.
	probeTimeout = time.Second * 15
	probeTick    = time.Millisecond * 20
	probeDial    = time.Second
)

// bootCfg holds the options applied to a container before it is started.
type bootCfg struct {
	probe func(ctx context.Context) bool
}

// Option customizes the container built by Start.
type Option func(*bootCfg)

// WithTCPProbe makes Start return only once addr accepts a connection. The rpc
// plugin binds its listener during Serve, so a successful dial is proof that the
// container is ready to answer calls.
func WithTCPProbe(addr string) Option {
	return func(b *bootCfg) {
		b.probe = func(ctx context.Context) bool {
			d := net.Dialer{Timeout: probeDial}
			conn, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return false
			}

			_ = conn.Close()

			return true
		}
	}
}

// Start registers the config plugin, a logger and the caller's plugins, boots the
// container and waits for the probe, if any, to answer. Errors arriving on the
// container channel are reported through t.Errorf and stop the container, but
// they do not abort the test.
//
// The returned stop is idempotent and also registered with t.Cleanup, so a test
// can stop the container mid-test and boot another one over the same config.
func Start(t *testing.T, cfgPath string, plugins []any, opts ...Option) func() {
	t.Helper()

	bc := &bootCfg{}
	for _, o := range opts {
		o(bc)
	}

	cont := endure.New(slog.LevelDebug)
	all := append([]any{&config.Plugin{Version: configVersion, Path: cfgPath}, &logger.Plugin{}}, plugins...)
	require.NoError(t, cont.RegisterAll(all...))
	require.NoError(t, cont.Init())

	// Init has already built every plugin, so from here on the container has to be
	// stopped even when Serve fails. Stopping twice is a no-op, and a failing Stop
	// is reported once.
	stopCont := sync.OnceFunc(func() {
		if errS := cont.Stop(); errS != nil {
			t.Errorf("container stop: %v", errS)
		}
	})
	t.Cleanup(stopCont)

	ch, err := cont.Serve()
	require.NoError(t, err)

	done := make(chan struct{})
	wg := &sync.WaitGroup{}

	wg.Go(func() {
		for {
			select {
			case res := <-ch:
				if res == nil {
					return
				}
				t.Errorf("plugin %s reported an error: %v", res.VertexID, res.Error)
				stopCont()
			case <-done:
				stopCont()

				return
			}
		}
	})

	// The drain goroutine calls t.Errorf, so it has to be joined while the test
	// is still running.
	stop := sync.OnceFunc(func() {
		close(done)
		wg.Wait()
	})
	t.Cleanup(stop)

	if bc.probe != nil {
		require.Eventually(t, func() bool { return bc.probe(t.Context()) }, probeTimeout, probeTick, "container did not become ready")
	}

	return stop
}
