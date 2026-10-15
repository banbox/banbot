package rpc

import (
	"fmt"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"testing"
	"time"
)

func TestTrySendExc(t *testing.T) {
	CleanUp()
	t.Cleanup(CleanUp)
	// This is a local throttle test. No external config or notification channel
	// should be loaded by an ordinary unit-test invocation.
	err := InitRPCWithConfig(&config.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = core.Setup()
	if err != nil {
		t.Fatal(err)
	}
	for count := 1; count <= 3; count++ {
		msg := fmt.Sprintf("this is tpl: %d", count)
		TrySendExc("testMsg", msg)
		core.Cache.Wait()
	}
	core.Cache.Wait()
	if got := core.GetCacheVal("testMsg_text", ""); got != "this is tpl: 3" {
		t.Fatalf("latest exception content = %q", got)
	}
}

func TestInitRPCWithConfigOwnsSendSettings(t *testing.T) {
	CleanUp()
	t.Cleanup(CleanUp)
	cfg := &config.Config{
		Name: "isolated",
		RPCChannels: map[string]map[string]interface{}{
			"mail": {"type": "email", "touser": "test@example.com"},
		},
		Webhook: map[string]map[string]string{
			MsgTypeStatus: {"content": "hello {name}"},
		},
	}
	if err := InitRPCWithConfig(cfg, nil); err != nil {
		t.Fatal(err)
	}
	channelsMu.RLock()
	if len(channels) != 1 {
		channelsMu.RUnlock()
		t.Fatalf("created %d channels, want 1", len(channels))
	}
	email := channels[0].(*Email)
	channelsMu.RUnlock()
	sent := make(chan string, 1)
	email.doSendMsgs = func(msgs []map[string]string) []map[string]string {
		if len(msgs) > 0 {
			sent <- msgs[0]["content"]
		}
		return nil
	}
	// Mutating the caller's config after initialization must not change the
	// running RPC session's name or templates.
	cfg.Name = "mutated"
	cfg.Webhook[MsgTypeStatus]["content"] = "changed"
	SendMsg(map[string]interface{}{"type": MsgTypeStatus})
	select {
	case got := <-sent:
		if got != "hello isolated" {
			t.Fatalf("sent content = %q, want isolated snapshot", got)
		}
	case <-time.After(time.Second):
		t.Fatal("RPC channel did not receive message")
	}
}
