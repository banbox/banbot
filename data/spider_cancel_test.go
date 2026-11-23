package data

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/banbox/banbot/utils"
)

// RunLiveSpiderWithRuntimeDeps needs real storage (timescale), so this test
// reproduces its shutdown wiring on the smallest unit: a LiveSpider whose
// RunForever blocks in Accept, plus context.AfterFunc(ctx, spider.Stop).
// Without that AfterFunc, cancelling ctx never closes the listener and
// RunForever stays blocked (the hub/spider process ignored SIGTERM).
func TestLiveSpiderRunForeverReturnsOnContextCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	spider := NewLiveSpider(utils.NewServerIO(addr, ""), nil, nil)
	stopOnCancel := context.AfterFunc(ctx, spider.Stop)
	defer stopOnCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = spider.RunForever(0, 0)
	}()

	// Wait until the listener is really accepting, so that the test cannot
	// pass merely because Stop ran before RunForever started.
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, derr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if derr == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("spider never started listening on %s: %v", addr, derr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("RunForever returned before context cancellation")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunForever did not return within 2s after context cancel")
	}
	spider.Join()
}
