// Package natstest runs a real JetStream server for tests.
package natstest

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// Start runs a throwaway JetStream server and returns its URL.
func Start(t testing.TB) string {
	t.Helper()
	return run(t, &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
}

// StartConfig runs a throwaway JetStream server from a real server config file
// and returns its URL. The config should not set a port or a store directory.
func StartConfig(t testing.TB, path string) string {
	t.Helper()
	opts, err := server.ProcessConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	opts.Host, opts.Port = "127.0.0.1", -1
	opts.JetStream, opts.StoreDir = true, t.TempDir()
	opts.NoLog, opts.NoSigs = true, true
	return run(t, opts)
}

func run(t testing.TB, opts *server.Options) string {
	t.Helper()
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server did not start")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}
