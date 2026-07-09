package biz

import (
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

func TestRemoteCommandServiceIdempotentClose(t *testing.T) {
	oldAccounts := config.Accounts
	config.Accounts = map[string]*config.AccountConfig{"acc": {}}
	defer func() { config.Accounts = oldAccounts }()

	svc := NewRemoteCommandService()
	calls := 0
	cmd := RemoteCommand{
		Source:      RemoteSourceWeb,
		Actor:       "user",
		Account:     "acc",
		Action:      RemoteActionCloseOrder,
		Idempotency: "same",
		OrderID:     1,
		Confirmed:   true,
		ExecClose: func() (int, int, *errs.Error) {
			calls++
			return 1, 0, nil
		},
	}
	res, err := svc.Run(cmd)
	if err != nil || res.CloseNum != 1 || calls != 1 {
		t.Fatalf("first run res=%+v err=%v calls=%d", res, err, calls)
	}
	res, err = svc.Run(cmd)
	if err != nil || !res.Idempotent || res.CloseNum != 1 || calls != 1 {
		t.Fatalf("second run res=%+v err=%v calls=%d", res, err, calls)
	}
}

func TestRemoteCommandServiceRequiresCloseAllConfirmation(t *testing.T) {
	oldAccounts := config.Accounts
	config.Accounts = map[string]*config.AccountConfig{"acc": {}}
	defer func() { config.Accounts = oldAccounts }()

	_, err := NewRemoteCommandService().Run(RemoteCommand{
		Source:    RemoteSourceWeb,
		Actor:     "user",
		Account:   "acc",
		Action:    RemoteActionCloseOrder,
		All:       true,
		ExecClose: func() (int, int, *errs.Error) { return 1, 0, nil },
	})
	if err == nil {
		t.Fatal("expected missing confirmation error")
	}
}

func TestRemoteCommandServiceCloseAllWithoutKeyCanRunAgainAfterRateWindow(t *testing.T) {
	oldAccounts := config.Accounts
	config.Accounts = map[string]*config.AccountConfig{"acc": {}}
	defer func() { config.Accounts = oldAccounts }()

	now := time.Unix(1, 0)
	svc := NewRemoteCommandService()
	svc.now = func() time.Time { return now }
	calls := 0
	cmd := RemoteCommand{
		Source:    RemoteSourceWeb,
		Actor:     "user",
		Account:   "acc",
		Action:    RemoteActionCloseOrder,
		All:       true,
		Confirmed: true,
		ExecClose: func() (int, int, *errs.Error) {
			calls++
			return calls, 0, nil
		},
	}
	res, err := svc.Run(cmd)
	if err != nil || res.CloseNum != 1 {
		t.Fatalf("first close all res=%+v err=%v", res, err)
	}
	now = now.Add(time.Second)
	res, err = svc.Run(cmd)
	if err != nil || res.CloseNum != 2 || res.Idempotent {
		t.Fatalf("second close all res=%+v err=%v", res, err)
	}
}

func TestRemoteCommandServiceRateLimit(t *testing.T) {
	oldAccounts := config.Accounts
	config.Accounts = map[string]*config.AccountConfig{"acc": {}}
	defer func() { config.Accounts = oldAccounts }()

	now := time.Unix(1, 0)
	svc := NewRemoteCommandService()
	svc.now = func() time.Time { return now }

	cmd := RemoteCommand{Source: RemoteSourceTelegram, Actor: "u", Account: "acc", Action: RemoteActionTradingSwitch, DisableHours: 1}
	if _, err := svc.Run(cmd); err != nil {
		t.Fatalf("first switch failed: %v", err)
	}
	if _, err := svc.Run(cmd); err == nil {
		t.Fatal("expected rate limit")
	}
	now = now.Add(time.Second)
	if _, err := svc.Run(cmd); err != nil {
		t.Fatalf("switch after rate window failed: %v", err)
	}
}

func TestRemoteCommandServiceRejectsNoTradeAccount(t *testing.T) {
	oldAccounts := config.Accounts
	config.Accounts = map[string]*config.AccountConfig{"acc": {NoTrade: true}}
	defer func() { config.Accounts = oldAccounts }()

	_, err := NewRemoteCommandService().Run(RemoteCommand{
		Source:  RemoteSourceWeb,
		Actor:   "user",
		Account: "acc",
		Action:  RemoteActionTradingSwitch,
		Enable:  true,
	})
	if err == nil {
		t.Fatal("expected no-trade account rejection")
	}
}

func TestRemoteCommandServiceTradingSwitch(t *testing.T) {
	oldAccounts := config.Accounts
	oldNoEnter := core.NoEnterUntil
	config.Accounts = map[string]*config.AccountConfig{"acc": {}}
	core.NoEnterUntil = map[string]int64{}
	defer func() {
		config.Accounts = oldAccounts
		core.NoEnterUntil = oldNoEnter
	}()

	res, err := NewRemoteCommandService().Run(RemoteCommand{
		Source:  RemoteSourceWeb,
		Actor:   "user",
		Account: "acc",
		Action:  RemoteActionTradingSwitch,
		UntilMS: 1234,
	})
	if err != nil || res.UntilMS != 1234 || core.NoEnterUntil["acc"] != 1234 {
		t.Fatalf("disable res=%+v err=%v map=%v", res, err, core.NoEnterUntil)
	}
	_, err = NewRemoteCommandService().Run(RemoteCommand{
		Source:  RemoteSourceWeb,
		Actor:   "user",
		Account: "acc",
		Action:  RemoteActionTradingSwitch,
		Enable:  true,
	})
	if err != nil {
		t.Fatalf("enable failed: %v", err)
	}
	if _, ok := core.NoEnterUntil["acc"]; ok {
		t.Fatal("expected enable to clear NoEnterUntil")
	}
}
