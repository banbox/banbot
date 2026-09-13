package utils

import (
	"net"
	"testing"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

func TestServerIOStopBeforeRunPreventsListener(t *testing.T) {
	server := NewBanServer("127.0.0.1:0", "")
	server.Stop()
	if err := server.RunForever(0, 0); err != nil {
		t.Fatal(err)
	}
	if server.ListenAddr() != "" {
		t.Fatal("stopped server opened a listener")
	}
	server.Join()
}

func TestServerIOStopJoinsAcceptedClient(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode, core.LiveMode = core.RunModeLive, true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })
	server := NewBanServer("127.0.0.1:0", "")
	done := make(chan struct{})
	server.OnConnExit = func(*BanConn, *errs.Error) { close(done) }
	go func() { _ = server.RunForever(0, 0) }()
	deadline := time.Now().Add(time.Second)
	for server.ListenAddr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}
	conn, err := net.Dial("tcp", server.ListenAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for len(server.ConnectionsSnapshot()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("server did not admit client")
		}
		time.Sleep(time.Millisecond)
	}
	server.Stop()
	server.Join()
	select {
	case <-done:
	default:
		t.Fatal("Join returned before client exit")
	}
}
