package rpc

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	utils2 "github.com/banbox/banbot/utils"
)

func TestDashboardClientUsesSessionState(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
	}()

	state, stateErr := core.NewState(context.Background())
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	state.SetRunMode(core.RunModeBackTest)
	session := &Session{Core: state}
	legacyClientPresent := utils2.HasBanConn()
	defer func() {
		state.Close()
	}()

	client, clientErr := newDashboardClientIO(session, listener.Addr().String(), "")
	if clientErr != nil {
		t.Fatal(clientErr)
	}
	defer func() {
		_ = client.Stop()
		client.Join()
	}()
	serverConnection := <-accepted
	defer serverConnection.Close()

	if client.RunMode != core.RunModeBackTest {
		t.Fatalf("dashboard client run mode = %q, want %q", client.RunMode, core.RunModeBackTest)
	}
	if utils2.HasBanConn() != legacyClientPresent {
		t.Fatal("explicit dashboard client installed the legacy global client")
	}

	telegramMutex.Lock()
	previousDashboard := dashBot
	dashBot = nil
	telegramMutex.Unlock()
	t.Cleanup(func() {
		telegramMutex.Lock()
		dashBot = previousDashboard
		telegramMutex.Unlock()
	})
	tg := &Telegram{session: session, dashboard: client, chatId: 42}
	if remaining := makeDoSendMsgTelegram(tg)([]map[string]string{{"content": "runtime"}}); remaining != nil {
		t.Fatalf("explicit dashboard message was not sent: %#v", remaining)
	}
	if err := serverConnection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	frameHeader := make([]byte, 4)
	if _, err := io.ReadFull(serverConnection, frameHeader); err != nil {
		t.Fatalf("explicit dashboard message did not reach its session connection: %v", err)
	}
}

func TestTelegramSessionStateIsolation(t *testing.T) {
	first := NewSession(config.NewSnapshot(&config.Config{Name: "first"}), nil)
	second := NewSession(config.NewSnapshot(&config.Config{Name: "second"}), nil)
	defer first.Close()
	defer second.Close()
	first.Core = &core.State{}
	first.Core.SetNoEnterUntil("account", 200)
	second.Core = &core.State{}
	first.Clock = btime.NewClockState(true, nil)
	first.Clock.SetTimeMS(100)
	second.Clock = btime.NewClockState(true, nil)
	second.Clock.SetTimeMS(300)
	first.messages = map[string]string{"status": "first status"}
	second.messages = map[string]string{"status": "second status"}
	firstBot, secondBot := &Telegram{session: first}, &Telegram{session: second}
	if !firstBot.IsTradingDisabled("account") || secondBot.IsTradingDisabled("account") {
		t.Fatal("telegram command reads another runtime trading state")
	}
	if firstBot.langMsg("status", "") != "first status" || secondBot.langMsg("status", "") != "second status" {
		t.Fatal("telegram language resources are shared")
	}
	if firstBot.configView().Name != "first" || secondBot.configView().Name != "second" {
		t.Fatal("telegram identity is shared")
	}
	first.Stop()
	if second.closed {
		t.Fatal("stopping one notification session stopped another")
	}
}

func TestTelegramSessionAccountsAreIsolated(t *testing.T) {
	previousAccounts := config.Accounts
	config.Accounts = map[string]*config.AccountConfig{
		"legacy": {},
	}
	t.Cleanup(func() { config.Accounts = previousAccounts })

	first := &Session{accounts: map[string]*config.AccountConfig{
		"first": {}, "first-other": {},
	}}
	second := &Session{accounts: map[string]*config.AccountConfig{
		"second": {},
	}}
	firstBot := &Telegram{session: first, activeAccount: "first"}
	secondBot := &Telegram{session: second, activeAccount: "second"}

	if firstBot.accountConfigs()["first"] == nil || secondBot.accountConfigs()["legacy"] != nil {
		t.Fatal("telegram session account views crossed runtime boundaries")
	}
	if !strings.Contains(firstBot.getAccountList(), "first-other") || strings.Contains(firstBot.getAccountList(), "legacy") {
		t.Fatal("telegram account list used the legacy global accounts")
	}
	firstBot.switchAccount("first-other")
	secondBot.switchAccount("first-other")
	if firstBot.activeAccount != "first-other" || secondBot.activeAccount != "second" {
		t.Fatalf("telegram account switch leaked: first=%q second=%q", firstBot.activeAccount, secondBot.activeAccount)
	}
}
