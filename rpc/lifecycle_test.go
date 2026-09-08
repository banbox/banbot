package rpc

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/go-telegram/bot"
)

func newLifecycleWebHook() *WebHook {
	return NewWebHook("lifecycle", map[string]interface{}{"type": "test"})
}

func TestWebHookEnqueueCloseRaceDoesNotPanic(t *testing.T) {
	hook := newLifecycleWebHook()

	start := make(chan struct{})
	var senders sync.WaitGroup
	for i := 0; i < 8; i++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			<-start
			for j := 0; j < 128; j++ {
				hook.SendMsg(MsgTypeStatus, "", map[string]string{"content": "test"})
			}
		}()
	}
	close(start)

	cleanupDone := make(chan struct{})
	go func() {
		hook.CleanUp()
		close(cleanupDone)
	}()
	senders.Wait()

	select {
	case <-cleanupDone:
	case <-time.After(2 * time.Second):
		t.Fatal("CleanUp did not finish after enqueue admission closed")
	}

	hook.CleanUp()
}

func TestWebHookFailedSendCompletesEnqueue(t *testing.T) {
	hook := newLifecycleWebHook()
	attempted := make(chan struct{})
	hook.doSendMsgs = func(msgs []map[string]string) []map[string]string {
		close(attempted)
		return msgs
	}
	go hook.ConsumeForever()

	if !hook.SendMsg(MsgTypeStatus, "", map[string]string{"content": "test"}) {
		t.Fatal("SendMsg rejected an open webhook")
	}
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("failed send was not attempted")
	}

	wgDone := make(chan struct{})
	go func() {
		hook.wg.Wait()
		close(wgDone)
	}()
	select {
	case <-wgDone:
	case <-time.After(time.Second):
		t.Fatal("failed send left the enqueue wait group pending")
	}
	hook.CleanUp()
}

func TestWebHookStopIsNonBlockingAndJoinWaits(t *testing.T) {
	hook := newLifecycleWebHook()
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		hook.Stop()
		hook.Join()
	})
	hook.doSendMsgs = func(msgs []map[string]string) []map[string]string {
		close(entered)
		<-release
		return msgs
	}
	go hook.ConsumeForever()
	if !hook.SendMsg(MsgTypeStatus, "", map[string]string{"content": "test"}) {
		t.Fatal("SendMsg rejected an open webhook")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("send callback did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		hook.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return while a send callback was blocked")
	}

	joinDone := make(chan struct{})
	go func() {
		hook.Join()
		close(joinDone)
	}()
	select {
	case <-joinDone:
		t.Fatal("Join returned before the send callback completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-joinDone:
	case <-time.After(time.Second):
		t.Fatal("Join did not return after the blocked send was released")
	}
}

type blockingTelegramHTTPClient struct {
	started chan struct{}
	once    sync.Once
}

func (c *blockingTelegramHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.once.Do(func() { close(c.started) })
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func TestTelegramCloseOwnsSenderAndListener(t *testing.T) {
	hook := newLifecycleWebHook()
	ctx, cancel := context.WithCancel(context.Background())
	tgHTTP := &blockingTelegramHTTPClient{started: make(chan struct{})}
	tgBot, err := bot.New("1:test",
		bot.WithSkipGetMe(),
		bot.WithHTTPClient(time.Second, tgHTTP),
	)
	if err != nil {
		t.Fatal(err)
	}
	tg := &Telegram{
		WebHook:       hook,
		bot:           tgBot,
		ctx:           ctx,
		cancel:        cancel,
		chanSend:      make(chan *bot.SendMessageParams, 1),
		activeAccount: "default",
	}
	tg.setupCommandHandlers()
	go tg.loopSend()

	deadline := time.Now().Add(time.Second)
	for {
		hook.owner.mu.Lock()
		tasks := hook.owner.taskCount
		hook.owner.mu.Unlock()
		if tasks == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Telegram sender and listener were not both registered with owner")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-tgHTTP.started:
	case <-time.After(time.Second):
		t.Fatal("Telegram listener did not reach its HTTP client")
	}

	stopDone := make(chan struct{})
	go func() {
		tg.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Telegram Stop did not return promptly")
	}
	tg.Join()
	tg.Stop()
	tg.Join()
	hook.owner.mu.Lock()
	tasks := hook.owner.taskCount
	hook.owner.mu.Unlock()
	if tasks != 0 {
		t.Fatalf("Telegram Close left %d owned tasks running", tasks)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("Telegram Close did not cancel its context")
	}
}

func TestRPCFacadeReinitializesAfterCleanUp(t *testing.T) {
	oldChannels := config.RPCChannels
	oldAccounts := config.Accounts
	oldWebhook := config.Webhook
	oldName := config.Name
	t.Cleanup(func() {
		CleanUp()
		config.RPCChannels = oldChannels
		config.Accounts = oldAccounts
		config.Webhook = oldWebhook
		config.Name = oldName
	})

	CleanUp()
	config.RPCChannels = map[string]map[string]interface{}{
		"lifecycle": {
			"type":   "email",
			"touser": "test@example.com",
		},
	}
	config.Accounts = nil
	config.Webhook = map[string]map[string]string{
		MsgTypeStatus: {"content": "ok"},
	}
	config.Name = "lifecycle"

	if err := InitRPC(); err != nil {
		t.Fatal(err)
	}
	channelsMu.RLock()
	firstGeneration := rpcGeneration
	if len(channels) != 1 {
		channelsMu.RUnlock()
		t.Fatalf("first initialization created %d channels, want 1", len(channels))
	}
	first := channels[0]
	channelsMu.RUnlock()

	CleanUp()
	if err := InitRPC(); err != nil {
		t.Fatal(err)
	}
	channelsMu.RLock()
	secondGeneration := rpcGeneration
	if len(channels) != 1 {
		channelsMu.RUnlock()
		t.Fatalf("reinitialization created %d channels, want 1", len(channels))
	}
	second := channels[0]
	channelsMu.RUnlock()
	if secondGeneration <= firstGeneration {
		t.Fatalf("RPC generation did not advance: first=%d second=%d", firstGeneration, secondGeneration)
	}
	if first == second {
		t.Fatal("reinitialization reused the stopped channel")
	}

	email, ok := second.(*Email)
	if !ok {
		t.Fatalf("reinitialized channel has type %T, want *Email", second)
	}
	sent := make(chan struct{})
	email.doSendMsgs = func(msgs []map[string]string) []map[string]string {
		close(sent)
		return nil
	}
	SendMsg(map[string]interface{}{"type": MsgTypeStatus})
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("reinitialized channel did not receive SendMsg")
	}

	Stop()
	Join()
}

func TestRPCFacadeConcurrentSendStopJoin(t *testing.T) {
	oldRPCChannels := config.RPCChannels
	oldWebhook := config.Webhook
	oldName := config.Name
	t.Cleanup(func() {
		CleanUp()
		config.RPCChannels = oldRPCChannels
		config.Webhook = oldWebhook
		config.Name = oldName
	})

	CleanUp()
	config.RPCChannels = nil
	config.Webhook = map[string]map[string]string{
		MsgTypeStatus: {"content": "ok"},
	}
	config.Name = "lifecycle"
	hook := newLifecycleWebHook()
	hook.doSendMsgs = func(msgs []map[string]string) []map[string]string {
		return nil
	}

	rpcInitMu.Lock()
	channelsMu.Lock()
	rpcGeneration++
	channels = []IWebHook{hook}
	rpcReady = true
	rpcClosed = false
	rpcStopDone = closedRPCSignal()
	rpcJoined = false
	channelsMu.Unlock()
	rpcInitMu.Unlock()
	go hook.ConsumeForever()

	start := make(chan struct{})
	var callers sync.WaitGroup
	for i := 0; i < 8; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-start
			for j := 0; j < 64; j++ {
				SendMsg(map[string]interface{}{"type": MsgTypeStatus})
			}
		}()
	}
	callers.Add(2)
	go func() {
		defer callers.Done()
		<-start
		Stop()
	}()
	go func() {
		defer callers.Done()
		<-start
		Join()
	}()
	close(start)
	callers.Wait()

	Stop()
	Join()
}
