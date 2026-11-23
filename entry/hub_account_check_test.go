package entry

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type accountCheckFake struct {
	banexg.BanExchange
	access    *banexg.AccountAccess
	accessErr *errs.Error
	calls     int
}

func (f *accountCheckFake) FetchBalance(map[string]interface{}) (*banexg.Balances, *errs.Error) {
	f.calls++
	return &banexg.Balances{}, nil
}

func (f *accountCheckFake) FetchAccountAccess(map[string]interface{}) (*banexg.AccountAccess, *errs.Error) {
	f.calls++
	return f.access, f.accessErr
}

func accountCheckRequest(env string) execution.HubAccountRegistration {
	return execution.HubAccountRegistration{
		Env:     env,
		Account: execution.AccountKey{VenueSessionIdentity: "binance/linear/" + env, Account: "acc", SettlementDomain: "USDT"},
	}
}

func accountCheckCredential() *config.AccountConfig {
	return &config.AccountConfig{Exchanges: map[string]*config.ExgApiSecrets{
		"binance": {Prod: &config.ApiSecretConfig{APIKey: "KEY-MATERIAL-1", APISecret: "SECRET-MATERIAL-2"}},
	}}
}

func TestVerifyHubProductionAccountNoTradePermission(t *testing.T) {
	f := &accountCheckFake{access: &banexg.AccountAccess{TradeKnown: true, TradeAllowed: false, PosMode: banexg.PosModeOneWay}}
	err := verifyHubProductionAccount(f, accountCheckRequest(core.RunEnvProd), accountCheckCredential(), "binance")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "trad") {
		t.Fatalf("want trade permission error, got %v", err)
	}
	if strings.Contains(err.Error(), "KEY-MATERIAL") || strings.Contains(err.Error(), "SECRET-MATERIAL") {
		t.Fatalf("error leaks credentials: %v", err)
	}
}

func TestVerifyHubProductionAccountRedactsCredentialsFromVenueErrors(t *testing.T) {
	f := &accountCheckFake{access: &banexg.AccountAccess{}, accessErr: errs.NewMsg(core.ErrRunTime, "bad key KEY-MATERIAL-1 rejected")}
	err := verifyHubProductionAccount(f, accountCheckRequest(core.RunEnvProd), accountCheckCredential(), "binance")
	if err == nil {
		t.Fatal("expected access fetch failure to fail registration")
	}
	if strings.Contains(err.Error(), "KEY-MATERIAL") {
		t.Fatalf("error leaks credentials: %v", err)
	}
}

func TestVerifyHubProductionAccountRejectsHedgeMode(t *testing.T) {
	f := &accountCheckFake{access: &banexg.AccountAccess{TradeKnown: true, TradeAllowed: true, PosMode: banexg.PosModeHedge}}
	err := verifyHubProductionAccount(f, accountCheckRequest(core.RunEnvProd), accountCheckCredential(), "binance")
	if err == nil || !strings.Contains(err.Error(), "hedge") {
		t.Fatalf("want hedge error, got %v", err)
	}
}

func TestVerifyHubProductionAccountWarningsOnlySucceeds(t *testing.T) {
	f := &accountCheckFake{access: &banexg.AccountAccess{
		TradeKnown: true, TradeAllowed: true,
		WithdrawKnown: true, WithdrawAllowed: true,
		IPKnown: true, IPAny: true,
		PosMode: banexg.PosModeOneWay,
	}}
	if err := verifyHubProductionAccount(f, accountCheckRequest(core.RunEnvProd), accountCheckCredential(), "binance"); err != nil {
		t.Fatalf("warnings must not fail registration: %v", err)
	}
}

func TestVerifyHubProductionAccountUnknownCapabilityMatchesLegacy(t *testing.T) {
	f := &accountCheckFake{access: &banexg.AccountAccess{}}
	if err := verifyHubProductionAccount(f, accountCheckRequest(core.RunEnvProd), accountCheckCredential(), "binance"); err != nil {
		t.Fatalf("unknown capability must not block: %v", err)
	}
	f = &accountCheckFake{access: nil}
	if err := verifyHubProductionAccount(f, accountCheckRequest(core.RunEnvProd), accountCheckCredential(), "binance"); err != nil {
		t.Fatalf("nil access must not block: %v", err)
	}
}

func TestVerifyHubProductionAccountSkipsDryRun(t *testing.T) {
	f := &accountCheckFake{access: &banexg.AccountAccess{TradeKnown: true, TradeAllowed: false, PosMode: banexg.PosModeHedge}}
	if err := verifyHubProductionAccount(f, accountCheckRequest(core.RunEnvDryRun), nil, "binance"); err != nil {
		t.Fatalf("dry-run must not be checked: %v", err)
	}
	if f.calls != 0 {
		t.Fatalf("dry-run performed %d exchange calls", f.calls)
	}
}
