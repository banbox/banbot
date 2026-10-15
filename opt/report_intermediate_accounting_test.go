package opt

import (
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// An in-flight report only has available cash in FinBalance; margin and open
// positions are still held by the wallet. The accounting invariant is valid
// after the backtest closes those positions, but not for cronDumpBtStatus.
func TestPrintBtResultSkipsFinalAccountingInvariantForIntermediateReports(t *testing.T) {
	logCore, logs := observer.New(zap.ErrorLevel)
	deps := &ReportDeps{
		Config:         config.NewSnapshot(&config.Config{}),
		Core:           &core.State{Logger: zap.New(logCore)},
		Trading:        biz.NewTradingState(),
		DefaultAccount: "default",
	}
	deps.Trading.Wallet("default").Items["USDT"] = &biz.ItemWallet{
		Coin:      "USDT",
		Available: 0.013628589873732722,
		Pendings:  map[string]float64{"open-order": 996.4224095600014},
		Frozens:   map[string]float64{},
	}

	result := NewBTResult()
	result.reportDeps = deps
	result.OutDir = t.TempDir()
	result.TotalInvest = 1000
	result.TotProfit = -3.563961850124908
	result.FinBalance = 0.013628589873732722
	result.CalcDiff = 0.9964224095600014

	result.printBtResult(false)
	if got := logs.FilterLevelExact(zap.ErrorLevel).Len(); got != 0 {
		t.Errorf("intermediate report emitted %d accounting errors; final-only invariant expected", got)
	}

	result.printBtResult(true)
	if got := logs.FilterLevelExact(zap.ErrorLevel).Len(); got != 1 {
		t.Errorf("final report error count = %d, want 1", got)
	}
}
