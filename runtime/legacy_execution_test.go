package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type legacyRuntimeExchange struct {
	banexg.BanExchange
	calls int
}

func (*legacyRuntimeExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "legacytest", MarketType: banexg.MarketLinear}
}
func (*legacyRuntimeExchange) GetCurMarkets() banexg.MarketMap { return nil }
func (e *legacyRuntimeExchange) CreateOrder(_, _, _ string, _, _ float64, _ map[string]any) (*banexg.Order, *errs.Error) {
	e.calls++
	return &banexg.Order{ID: "ack"}, nil
}

func legacyRuntimeOptions(t *testing.T, raw *legacyRuntimeExchange) Options {
	t.Helper()
	dir := t.TempDir()
	return Options{Config: &config.Config{Env: core.RunEnvProd, Exchange: &config.ExchangeConfig{Name: "legacytest"}, MarketType: banexg.MarketLinear, Accounts: map[string]*config.AccountConfig{"trade": {}}, StakeCurrency: []string{"USDT"}}, Mode: core.RunModeLive, Exchange: raw, LegacyExecution: &LegacyExecutionOptions{VenueSessionIdentity: "legacytest:prod", SenderLeaseDir: filepath.Join(dir, "leases")}}
}
func TestLegacyRuntimeSharesPhysicalSenderAndRetainsSibling(t *testing.T) {
	raw := &legacyRuntimeExchange{}
	opts := legacyRuntimeOptions(t, raw)
	p := NewProcess()
	defer p.Close()
	one, err := p.NewRuntime(opts)
	if err != nil {
		t.Fatal(err)
	}
	two, err := p.NewRuntime(opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.legacySenders) != 1 {
		t.Fatal("same physical account opened duplicate senders")
	}
	changed := opts
	changed.Exchange = &legacyRuntimeExchange{}
	if _, err := p.NewRuntime(changed); err == nil {
		t.Fatal("physical sender rebound to another SDK")
	}
	one.Close()
	one.Join()
	if _, err := two.Exchange.CreateOrder("BTC", "market", "buy", 1, 0, nil); err != nil {
		t.Fatal("borrower close stopped sibling", err)
	}
	if _, err := one.Exchange.CreateOrder("BTC", "market", "buy", 1, 0, nil); err == nil {
		t.Fatal("closed facade mutated venue")
	}
	key := execution.AccountKey{VenueSessionIdentity: opts.LegacyExecution.VenueSessionIdentity, Account: "trade", SettlementDomain: "USD"}
	if _, err := p.BorrowAccount(key, execution.SharedExecutionOptions{StorePath: filepath.Join(t.TempDir(), "shared.db"), SenderLeaseDir: opts.LegacyExecution.SenderLeaseDir, Adapter: &sharedTestAdapter{}, AuthoritativeSnapshot: true}); err == nil {
		t.Fatal("same process admitted shared sender in another settlement")
	}
	if raw.calls != 1 {
		t.Fatal("unexpected SDK writes", raw.calls)
	}
}

func TestLegacyBindingFailedConstructionReleasesOnlyUnestablishedServices(t *testing.T) {
	raw := &legacyRuntimeExchange{}
	opts := legacyRuntimeOptions(t, raw)
	p := NewProcess()
	defer p.Close()
	state, stateErr := core.NewState(context.Background())
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	defer state.Close()
	state.SetRunEnv(core.RunEnvProd)
	rt := &Runtime{Core: state, Config: config.NewSnapshot(opts.Config), Accounts: opts.Config.Accounts, defaultAccount: "trade"}
	_, finishOne, err := p.bindLegacyExchange(rt, raw, *opts.LegacyExecution)
	if err != nil {
		t.Fatal(err)
	}
	_, finishTwo, err := p.bindLegacyExchange(rt, raw, *opts.LegacyExecution)
	if err != nil {
		t.Fatal(err)
	}
	finishOne(false)
	if len(p.legacySenders) != 1 {
		t.Fatal("failed constructor stole concurrent constructor's lease")
	}
	finishTwo(false)
	if len(p.legacySenders) != 0 {
		t.Fatal("failed constructors leaked services")
	}
	key := execution.AccountKey{VenueSessionIdentity: opts.LegacyExecution.VenueSessionIdentity, Account: "trade", SettlementDomain: "USDT"}
	store, err := execution.OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "rollback.db"), key, opts.LegacyExecution.SenderLeaseDir)
	if err != nil {
		t.Fatal("failed constructor retained lease", err)
	}
	store.Close()
	_, finishOne, err = p.bindLegacyExchange(rt, raw, *opts.LegacyExecution)
	if err != nil {
		t.Fatal(err)
	}
	_, finishTwo, err = p.bindLegacyExchange(rt, raw, *opts.LegacyExecution)
	if err != nil {
		t.Fatal(err)
	}
	finishOne(true)
	finishTwo(false)
	if len(p.legacySenders) != 1 {
		t.Fatal("failed sibling dropped committed process service")
	}
}
