package dev

import (
	"context"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

func TestNewDevServerWithDepsRequiresSnapshotDirectory(t *testing.T) {
	if _, err := NewDevServer(DevDeps{}); err == nil {
		t.Fatal("missing snapshot directory was accepted")
	}
	server := newDevServer(DevDeps{Data: &data.RuntimeDeps{Config: config.NewSnapshotWithDirs(nil, t.TempDir(), "")}})
	if server.DataDir() == "" {
		t.Fatal("server did not retain its snapshot directory")
	}
}

func TestDevServerDataForUsesAlternateRuntime(t *testing.T) {
	primarySymbols := orm.NewSymbolStateWithIdentity("primary", "spot")
	if err := primarySymbols.SetExSymbols([]*orm.ExSymbol{{ID: 1, Exchange: "primary", Market: "spot", Symbol: "BTC/USDT"}}); err != nil {
		t.Fatal(err)
	}
	alternateSymbols := orm.NewSymbolStateWithIdentity("alternate", "linear")
	if err := alternateSymbols.SetExSymbols([]*orm.ExSymbol{{ID: 2, Exchange: "alternate", Market: "linear", Symbol: "ETH/USDT:USDT"}}); err != nil {
		t.Fatal(err)
	}
	primary := &data.RuntimeDeps{ExchangeName: "primary", MarketType: "spot", Symbols: primarySymbols}
	alternate := &data.RuntimeDeps{ExchangeName: "alternate", MarketType: "linear", Symbols: alternateSymbols}
	called := false
	cleaned := false
	server := newDevServer(DevDeps{Data: primary, RuntimeFor: func(_ context.Context, name, market string) (*data.RuntimeDeps, func(), *errs.Error) {
		called = name == "alternate" && market == "linear"
		return alternate, func() { cleaned = true }, nil
	}})
	got, cleanup, err := server.dataFor(context.Background(), "alternate", "linear")
	if err != nil {
		t.Fatal(err)
	}
	cleanup()
	if !called || got != alternate {
		t.Fatal("alternate runtime was not selected")
	}
	if !cleaned || got.Symbols.GetSymbolByID(1) != nil || primary.Symbols.GetSymbolByID(2) != nil {
		t.Fatal("alternate runtime state was shared with the primary runtime")
	}
}

func TestDevServerDataForRejectsMismatchedRuntime(t *testing.T) {
	primary := &data.RuntimeDeps{ExchangeName: "primary", MarketType: "spot", Symbols: orm.NewSymbolStateWithIdentity("primary", "spot")}
	wrong := &data.RuntimeDeps{ExchangeName: "wrong", MarketType: "linear", Symbols: orm.NewSymbolStateWithIdentity("wrong", "linear")}
	cleaned := false
	server := newDevServer(DevDeps{Data: primary, RuntimeFor: func(context.Context, string, string) (*data.RuntimeDeps, func(), *errs.Error) {
		return wrong, func() { cleaned = true }, nil
	}})

	got, cleanup, err := server.dataFor(context.Background(), "alternate", "linear")
	if err == nil {
		t.Fatal("mismatched runtime identity was accepted")
	}
	if got != nil || cleanup != nil {
		t.Fatalf("mismatched runtime returned deps=%#v cleanup=%v", got, cleanup != nil)
	}
	if !cleaned {
		t.Fatal("mismatched runtime was not cleaned up")
	}
}
