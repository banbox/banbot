package opt

import (
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
)

func TestReportDepsDoesNotReadLegacyOrderHistory(t *testing.T) {
	previous := ormo.HistODs
	ormo.HistODs = []*ormo.InOutOrder{{IOrder: &ormo.IOrder{ID: 99}}}
	t.Cleanup(func() { ormo.HistODs = previous })

	result := &BTResult{reportDeps: &ReportDeps{}}
	if got := result.historyOrders(); got != nil {
		t.Fatalf("explicit report history = %#v, want nil when order state is missing", got)
	}
}

func TestReportDepsRequiresExplicitStorageForSeriesQueries(t *testing.T) {
	deps := &ReportDeps{Symbols: orm.NewSymbolStateWithIdentity("binance", "spot")}
	if _, _, err := deps.queries(); err == nil || !strings.Contains(err.Error(), "storage") {
		t.Fatalf("missing explicit storage error = %v, want storage error", err)
	}
	if _, err := calcPairStats(nil, 0, 1, "1m", deps); err == nil || !strings.Contains(err.Error(), "storage") {
		t.Fatalf("missing explicit storage pair-stats error = %v, want storage error", err)
	}
}

func TestReportDepsRejectsUnboundSymbolsWithExplicitStorage(t *testing.T) {
	storage := orm.NewStorage(nil, false, "runtime-report")
	deps := &ReportDeps{
		Symbols: orm.NewSymbolStateWithIdentity("binance", "spot"),
		Storage: storage,
	}
	if err := deps.validateSeries(); err == nil || !strings.Contains(err.Error(), "share an owner") {
		t.Fatalf("unbound symbol state validation error = %v, want ownership error", err)
	}
}

func TestRuntimeReportRejectsMissingOwnedDependencies(t *testing.T) {
	base := &ReportDeps{
		Config:  config.NewSnapshotWithDirs(&config.Config{}, t.TempDir(), ""),
		Market:  com.NewMarketState("binance"),
		Symbols: orm.NewSymbolStateWithIdentity("binance", "spot"),
		Storage: orm.NewStorage(nil, false, "runtime-report"),
		Trading: biz.NewTradingState(),
		Orders:  ormo.NewOrderState(),
	}
	cases := []struct {
		name   string
		mutate func(*ReportDeps)
		want   string
	}{
		{name: "orders", mutate: func(deps *ReportDeps) { deps.Orders = nil }, want: "orders"},
		{name: "storage", mutate: func(deps *ReportDeps) { deps.Storage = nil }, want: "storage"},
		{name: "market", mutate: func(deps *ReportDeps) { deps.Market = nil }, want: "market prices"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			deps := *base
			testCase.mutate(&deps)
			if _, err := calcBtResultWithDeps(nil, nil, "", &deps); err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("missing %s report dependency error = %v", testCase.name, err)
			}
		})
	}
}

func TestReportDatesUseSnapshotLocation(t *testing.T) {
	previous := btime.LocShow
	btime.LocShow = time.FixedZone("legacy", -8*60*60)
	t.Cleanup(func() { btime.LocShow = previous })

	snapshot := config.NewSnapshotWithDirs(&config.Config{
		Exchange: &config.ExchangeConfig{Name: "china"},
	}, t.TempDir(), "")
	deps := &ReportDeps{Config: snapshot}
	stamp := int64(1704069000000) // 2024-01-01 00:30:00 UTC
	want := btime.MSToTime(stamp).In(snapshot.Location()).Format("2006-01-02 15:04:05")
	if got := deps.dateStrLoc(stamp, ""); got != want {
		t.Fatalf("runtime report date = %q, want snapshot location %q", got, want)
	}
	if got := deps.dateStrLoc(stamp, ""); got == btime.ToDateStrLoc(stamp, "") {
		t.Fatalf("runtime report date unexpectedly used legacy location: %q", got)
	}
}
