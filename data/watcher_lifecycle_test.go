package data

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
)

func newWatcherListener(t *testing.T) (net.Listener, chan net.Conn, chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	return listener, accepted, acceptErr
}

func waitWatcherConn(t *testing.T, accepted chan net.Conn, acceptErr chan error, addr string) net.Conn {
	t.Helper()
	select {
	case conn := <-accepted:
		return conn
	case err := <-acceptErr:
		t.Fatalf("accept watcher connection %s: %v", addr, err)
	case <-time.After(time.Second):
		t.Fatalf("watcher did not connect to %s", addr)
	}
	return nil
}

func TestSeriesWatcherRuntimePingStopsWithRuntimeState(t *testing.T) {
	listener, accepted, acceptErr := newWatcherListener(t)
	state, err := core.NewState(context.Background())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	watcher, watchErr := NewSeriesWatcherWithRuntimeDeps(&RuntimeDeps{Core: state}, listener.Addr().String())
	if watchErr != nil {
		state.Close()
		_ = listener.Close()
		t.Fatal(watchErr)
	}
	serverConn := waitWatcherConn(t, accepted, acceptErr, listener.Addr().String())
	t.Cleanup(func() {
		_ = watcher.Stop()
		watcher.Join()
		state.Close()
		_ = serverConn.Close()
		_ = listener.Close()
	})

	stopDone := make(chan struct{})
	go func() {
		state.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("runtime stop did not cancel watcher ping loop")
	}
	joined := make(chan struct{})
	go func() {
		watcher.ClientIO.Join()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("BanConn.Join did not wait for the canceled watcher ping loop")
	}
	watcher.Join()
}

func TestSeriesWatcherRunForeverJoinsPingAfterClientStop(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeOther
	core.LiveMode = false
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	listener, accepted, acceptErr := newWatcherListener(t)
	state, err := core.NewState(context.Background())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	state.SetRunMode(core.RunModeLive)
	watcher, watchErr := NewSeriesWatcherWithRuntimeDeps(&RuntimeDeps{Core: state}, listener.Addr().String())
	if watchErr != nil {
		state.Close()
		_ = listener.Close()
		t.Fatal(watchErr)
	}
	serverConn := waitWatcherConn(t, accepted, acceptErr, listener.Addr().String())
	t.Cleanup(func() {
		_ = watcher.Stop()
		watcher.Join()
		state.Close()
		_ = serverConn.Close()
		_ = listener.Close()
	})

	runDone := make(chan struct{})
	go func() {
		_ = watcher.RunForever()
		close(runDone)
	}()
	server := &utils.BanConn{Conn: serverConn, Ready: true}
	if err := server.Write(&utils.IOMsgRaw{Action: "watcher_test"}); err != nil {
		t.Fatal(err)
	}

	if err := watcher.ClientIO.Stop(); err != nil {
		t.Fatal(err)
	}
	watcher.ClientIO.Join()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("RunForever did not finish after direct ClientIO.Stop")
	}
}
