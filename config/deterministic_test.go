package config

import (
	"testing"

	"github.com/banbox/banbot/core"
)

func TestBTStrictConfigAndCLIOverride(t *testing.T) {
	cfg, err := ParseYmlConfig([]byte("bt_strict: true\n"), "")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.BTStrict || !cfg.Clone().BTStrict {
		t.Fatal("bt_strict was not parsed or cloned")
	}

	cfg = &Config{TimeStart: "20240101"}
	if err := cfg.Apply(&CmdArgs{BTStrict: true, BTStrictSet: true}); err != nil {
		t.Fatal(err)
	}
	if !cfg.BTStrict {
		t.Fatal("command-line strict override was not applied")
	}

	cfg.BTStrict = true
	if err := cfg.Apply(&CmdArgs{BTStrictSet: true}); err != nil {
		t.Fatal(err)
	}
	if cfg.BTStrict {
		t.Fatal("explicit command-line false did not override YAML true")
	}

	cfg.BTStrict = true
	if err := cfg.Apply(&CmdArgs{}); err != nil {
		t.Fatal(err)
	}
	if !cfg.BTStrict {
		t.Fatal("omitted command-line flag overrode YAML true")
	}
}

func TestStrictBacktestRequiresModeAndFlag(t *testing.T) {
	oldMode, oldData := core.BackTestMode, Data
	t.Cleanup(func() {
		core.BackTestMode = oldMode
		Data = oldData
	})

	core.BackTestMode = true
	Data.BTStrict = false
	if StrictBacktest() {
		t.Fatal("strict mode enabled without config flag")
	}
	Data.BTStrict = true
	if !StrictBacktest() {
		t.Fatal("strict mode disabled in configured backtest")
	}
	core.BackTestMode = false
	if StrictBacktest() {
		t.Fatal("strict backtest behavior leaked into non-backtest mode")
	}
}

func TestStrictHistoricalReplayRequiresCompleteContext(t *testing.T) {
	oldMode, oldData, oldCoverage := core.BackTestMode, Data, HistoricalCoverage
	t.Cleanup(func() {
		core.BackTestMode, Data, HistoricalCoverage = oldMode, oldData, oldCoverage
	})

	strictCoverage := &HistoricalCoverageConfig{
		BaselineEndMS: 2,
		Bars: map[string]map[string][]HistoricalCoverageRange{
			"BTC/USDT:USDT": {"1h": {{StartMS: 0, StopMS: 2}}},
		},
	}
	resetContext := func() {
		core.BackTestMode = true
		Data = Config{BTStrict: true, BTNoKlineDownload: true}
		HistoricalCoverage = strictCoverage
	}
	resetContext()
	if !StrictHistoricalReplay(HistoricalCoverage) {
		t.Fatal("complete strict historical replay context was rejected")
	}

	for _, test := range []struct {
		name   string
		change func()
	}{
		{name: "non-backtest", change: func() { core.BackTestMode = false }},
		{name: "non-strict", change: func() { Data.BTStrict = false }},
		{name: "download-enabled", change: func() { Data.BTNoKlineDownload = false }},
		{name: "missing coverage", change: func() { HistoricalCoverage = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			resetContext()
			test.change()
			if StrictHistoricalReplay(HistoricalCoverage) {
				t.Fatal("incomplete historical replay context was accepted")
			}
		})
	}
}

func TestInitExgAccsSelectsCanonicalBacktestAccount(t *testing.T) {
	oldAccounts, oldBak := Accounts, BakAccounts
	oldExchange, oldDefAcc := Exchange, DefAcc
	oldEnvReal := core.EnvReal
	t.Cleanup(func() {
		Accounts, BakAccounts = oldAccounts, oldBak
		Exchange, DefAcc, core.EnvReal = oldExchange, oldDefAcc, oldEnvReal
	})
	Exchange = &ExchangeConfig{Name: "binance"}
	core.EnvReal = false

	explicit := &AccountConfig{StakeRate: 2}
	if err := initExgAccs(&CmdArgs{}, map[string]*AccountConfig{
		"zeta": {}, "default": explicit, "alpha": {},
	}); err != nil {
		t.Fatal(err)
	}
	if Accounts["default"] != explicit {
		t.Fatal("explicit default account was not selected")
	}

	alpha := &AccountConfig{StakeRate: 3}
	if err := initExgAccs(&CmdArgs{}, map[string]*AccountConfig{
		"zeta": {}, "alpha": alpha,
	}); err != nil {
		t.Fatal(err)
	}
	if Accounts["default"] != alpha {
		t.Fatal("lexicographically first enabled account was not selected")
	}
}
