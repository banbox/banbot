package live

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type accessExchange struct {
	banexg.BanExchange
	access *banexg.AccountAccess
}

func (e accessExchange) FetchBalance(map[string]interface{}) (*banexg.Balances, *errs.Error) {
	return &banexg.Balances{}, nil
}

func (e accessExchange) FetchAccountAccess(map[string]interface{}) (*banexg.AccountAccess, *errs.Error) {
	return e.access, nil
}

func verifyDeps(access *banexg.AccountAccess, cfg *config.AccountConfig) biz.RuntimeDeps {
	return biz.RuntimeDeps{
		Core:     &core.State{EnvReal: true, Market: banexg.MarketLinear},
		Exchange: accessExchange{access: access},
		Accounts: map[string]*config.AccountConfig{"acc": cfg},
		Config:   config.NewSnapshot(&config.Config{}),
	}
}

// Embedded execution-backed robots must fail closed: the order managers there
// ignore NoTrade, so the verifier reports instead of flagging the account.
func TestVerifyLiveAccountsFailsClosedWithoutTouchingNoTrade(t *testing.T) {
	cfg := &config.AccountConfig{}
	err := VerifyLiveAccountsWithRuntimeDeps(verifyDeps(&banexg.AccountAccess{TradeKnown: true, TradeAllowed: false}, cfg))
	if err == nil || !strings.Contains(err.Error(), "acc") {
		t.Fatalf("expected a failure naming the account, got %v", err)
	}
	if cfg.NoTrade {
		t.Fatal("verify must not mutate NoTrade")
	}
}

func TestVerifyLiveAccountsPassesAllowedAccount(t *testing.T) {
	cfg := &config.AccountConfig{}
	if err := VerifyLiveAccountsWithRuntimeDeps(verifyDeps(&banexg.AccountAccess{TradeKnown: true, TradeAllowed: true}, cfg)); err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
}

func TestVerifyLiveAccountsSkipsNonRealEnv(t *testing.T) {
	deps := verifyDeps(&banexg.AccountAccess{TradeKnown: true}, &config.AccountConfig{})
	deps.Core = &core.State{}
	if err := VerifyLiveAccountsWithRuntimeDeps(deps); err != nil {
		t.Fatalf("non-real env must not be checked: %v", err)
	}
}
