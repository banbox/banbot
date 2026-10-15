package biz

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"go.uber.org/zap"
)

type calendarWriterStub struct {
	calls  []calendarWrite
	failOn int
}

type calendarWrite struct {
	name  string
	items [][2]int64
}

func (s *calendarWriterStub) SetCalendars(name string, items [][2]int64) *errs.Error {
	s.calls = append(s.calls, calendarWrite{name: name, items: append([][2]int64(nil), items...)})
	if s.failOn == len(s.calls) {
		return errs.NewMsg(core.ErrDbExecFail, "calendar write failed")
	}
	return nil
}

func TestLoadCalendarRowsGroupsRecordsAndReturnsWriteFailures(t *testing.T) {
	store := &calendarWriterStub{}
	rows := [][]string{
		{"NYSE", "2024-01-01", "2024-01-02"},
		{"NYSE", "2024-02-01", "2024-02-02"},
		{"SSE", "2024-03-01", "2024-03-02"},
	}
	if err := LoadCalendarRows(rows, store, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	if got, want := []string{store.calls[0].name, store.calls[1].name}, []string{"NYSE", "SSE"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("calendar writes = %v, want %v", got, want)
	}
	store = &calendarWriterStub{failOn: 2}
	if err := LoadCalendarRows(rows, store, zap.NewNop()); err == nil || !strings.Contains(err.Error(), "calendar write failed") {
		t.Fatalf("final calendar write error = %v, want failure", err)
	}
}

func TestLoadCalendarRowsRejectsShortRows(t *testing.T) {
	if err := LoadCalendarRows([][]string{{"NYSE", "2024-01-01"}}, &calendarWriterStub{}, zap.NewNop()); err == nil || err.Code != errs.CodeParamInvalid {
		t.Fatalf("short calendar row error = %v, want parameter error", err)
	}
}

func TestKlineConsistencyRegistersDumpTypesOnlyOnce(t *testing.T) {
	path := t.TempDir() + "/empty.gob"
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	deps := KlineConsistencyDeps{Queries: orm.New(nil), Symbols: orm.NewSymbolState(), Logger: zap.NewNop()}
	for i := 0; i < 2; i++ {
		if err := TestKLineConsistencyWithRuntimeDeps(path, deps); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	if err := os.WriteFile(path, []byte("not a gob stream"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := TestKLineConsistencyWithRuntimeDeps(path, deps); err == nil {
		t.Fatal("invalid dump unexpectedly succeeded")
	}
}

func TestKlineMaintenanceRuntimeDepsAreRequired(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func() *errs.Error
	}{
		{
			name: "export",
			run: func() *errs.Error {
				return ExportKlinesWithRuntimeDeps(&config.CmdArgs{OutPath: t.TempDir()}, nil, nil)
			},
		},
		{
			name: "purge",
			run: func() *errs.Error {
				return PurgeKlinesWithRuntimeDeps(&config.CmdArgs{}, nil)
			},
		},
		{
			name: "adj-export",
			run: func() *errs.Error {
				return ExportAdjFactorsWithRuntimeDeps(&config.CmdArgs{OutPath: t.TempDir(), Pairs: []string{"BTC/USDT"}}, &KlineMaintenanceDeps{Context: context.Background()})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.run()
			if err == nil || err.Code != core.ErrBadConfig {
				t.Fatalf("error = %v, want ErrBadConfig", err)
			}
		})
	}
}

func TestRunHistSeriesWithRuntimeDepsRequiresOwnedDependencies(t *testing.T) {
	err := RunHistSeriesWithRuntimeDeps(&RunHistSeriesArgs{OnData: func(*orm.DataSeries, []*orm.DataSeries) {}}, RuntimeDeps{})
	if err == nil || err.Code != core.ErrBadConfig {
		t.Fatalf("error = %v, want ErrBadConfig", err)
	}
	for _, field := range []string{"core", "clock", "config", "market", "symbols", "storage", "strategies", "exchange"} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("error %q does not name missing %s dependency", err, field)
		}
	}
}

func TestRunHistSeriesRejectsMissingCallback(t *testing.T) {
	err := RunHistSeries(&RunHistSeriesArgs{})
	if err == nil || err.Code != errs.CodeParamRequired {
		t.Fatalf("error = %v, want parameter error", err)
	}
}

func TestNewExgOrderSetWithRuntimeDepsUsesOwnedState(t *testing.T) {
	dir := t.TempDir()
	deps := RuntimeDeps{
		Core:     &core.State{ExgName: "owned", Market: banexg.MarketSpot, RunEnv: core.RunEnvProd},
		Config:   config.NewSnapshotWithDirs(&config.Config{}, dir, ""),
		Accounts: map[string]*config.AccountConfig{"owned": {Exchanges: map[string]*config.ExgApiSecrets{"owned": {Prod: &config.ApiSecretConfig{APIKey: "abcdef"}}}}},
		Exchange: &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "owned", MarketType: banexg.MarketSpot}},
	}
	set, err := NewExgOrderSetWithRuntimeDeps(deps, "owned", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if set.exchange != deps.Exchange || set.Name != "owned" || set.Market != banexg.MarketSpot {
		t.Fatalf("set escaped runtime dependencies: %+v", set)
	}
	if !strings.HasPrefix(set.path, dir) {
		t.Fatalf("cache path %q is outside runtime data directory %q", set.path, dir)
	}
}

func TestGetExgOrderSetRejectsMissingLegacyAccount(t *testing.T) {
	previous := config.Accounts
	config.Accounts = map[string]*config.AccountConfig{}
	t.Cleanup(func() { config.Accounts = previous })

	if _, err := GetExgOrderSet("missing", "binance", "spot"); err == nil {
		t.Fatal("missing legacy account was accepted")
	}
}

func TestGetExgOrderSetRejectsUnsafeCacheIdentity(t *testing.T) {
	if _, err := GetExgOrderSet("../escape", "binance", "spot"); err == nil {
		t.Fatal("unsafe order-cache account was accepted")
	}
}

func TestNewExgOrderSetWithRuntimeDepsRejectsWrongExchangeIdentity(t *testing.T) {
	deps := RuntimeDeps{
		Core:     &core.State{ExgName: "primary", Market: banexg.MarketSpot, RunEnv: core.RunEnvProd},
		Config:   config.NewSnapshotWithDirs(&config.Config{}, t.TempDir(), ""),
		Accounts: map[string]*config.AccountConfig{"owned": {Exchanges: map[string]*config.ExgApiSecrets{"other": {Prod: &config.ApiSecretConfig{APIKey: "abcdef"}}}}},
		Exchange: &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "primary", MarketType: banexg.MarketSpot}},
	}
	if _, err := NewExgOrderSetWithRuntimeDeps(deps, "owned", "other", banexg.MarketSpot); err == nil || err.Code != core.ErrBadConfig {
		t.Fatalf("wrong exchange identity error = %v, want bad config", err)
	}
}

func TestNewExgOrderSetWithRuntimeDepsRejectsCorruptCache(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "exgOrders")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cacheDir, "owned_spot_owned_abcde.gob"), []byte("not a gob"), 0644); err != nil {
		t.Fatal(err)
	}
	deps := RuntimeDeps{
		Core:     &core.State{ExgName: "owned", Market: banexg.MarketSpot, RunEnv: core.RunEnvProd},
		Config:   config.NewSnapshotWithDirs(&config.Config{}, dir, ""),
		Accounts: map[string]*config.AccountConfig{"owned": {Exchanges: map[string]*config.ExgApiSecrets{"owned": {Prod: &config.ApiSecretConfig{APIKey: "abcdef"}}}}},
		Exchange: &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "owned", MarketType: banexg.MarketSpot}},
	}
	if _, err := NewExgOrderSetWithRuntimeDeps(deps, "owned", "", ""); err == nil || err.Code != errs.CodeIOReadFail {
		t.Fatalf("corrupt cache error = %v, want read failure", err)
	}
}

func TestPurgeKlinesWithRuntimeDepsDeclinesBeforeQuery(t *testing.T) {
	exchange := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "owned-exchange", MarketType: banexg.MarketSpot}}
	symbols := orm.NewSymbolState()
	if err := symbols.SetExSymbols([]*orm.ExSymbol{{ID: 1, Exchange: "owned-exchange", Market: banexg.MarketSpot, Symbol: "BTC/USDT"}}); err != nil {
		t.Fatal(err)
	}
	var prompt []string
	err := PurgeKlinesWithRuntimeDeps(&config.CmdArgs{Pairs: []string{"BTC/USDT"}, TimeFrames: []string{"1m"}}, &KlineMaintenanceDeps{
		Context: context.Background(), Queries: orm.New(nil), Symbols: symbols,
		Config:   &config.Config{Exchange: &config.ExchangeConfig{Name: "owned-exchange"}, MarketType: banexg.MarketSpot},
		Exchange: exchange, Logger: zap.NewNop(), Location: time.UTC,
		Confirm: func(lines []string, _, _ string, _ bool) bool { prompt = append([]string(nil), lines...); return false },
	})
	if err != nil {
		t.Fatalf("declined purge returned error: %v", err)
	}
	got := strings.Join(prompt, "\n")
	for _, want := range []string{"owned-exchange", "BTC/USDT", "1m"} {
		if !strings.Contains(got, want) {
			t.Fatalf("confirmation = %q, want %q", got, want)
		}
	}
}
