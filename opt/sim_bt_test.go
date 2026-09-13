package opt

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormo"
)

func TestSectionPairsUsesCanonicalOrderInStrictBacktest(t *testing.T) {
	pairs := map[string]bool{
		"SOL/USDT": true,
		"BTC/USDT": true,
		"ETH/USDT": true,
	}
	want := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	for range 32 {
		if got := sectionPairs(pairs, true); !slices.Equal(got, want) {
			t.Fatalf("section pairs = %v, want %v", got, want)
		}
	}
}

func TestDeriveSimulationSnapshotDisablesPairRotation(t *testing.T) {
	source := config.NewSnapshot(&config.Config{PairMgr: &config.PairMgrConfig{
		Cron: "0 * * * * *", UseLatest: true,
	}})
	derived := deriveSimulationSnapshot(source, 1, 2, []string{"BTC/USDT"}, nil)
	if derived == nil || derived.View() == nil || derived.View().PairMgr == nil {
		t.Fatal("simulation snapshot did not retain an explicit pair manager policy")
	}
	if derived.View().PairMgr.Cron != "" || derived.View().PairMgr.UseLatest {
		t.Fatalf("simulation pair rotation was not disabled: %#v", derived.View().PairMgr)
	}
	if source.View().PairMgr.Cron == "" || !source.View().PairMgr.UseLatest {
		t.Fatal("simulation snapshot mutated the source pair manager")
	}
}

func TestSaveOrdersUsesReportLocation(t *testing.T) {
	const startMS = int64(1704069000000) // 2024-01-01 00:30:00 UTC
	path := t.TempDir() + "/orders"
	deps := &ReportDeps{Config: config.NewSnapshotWithDirs(&config.Config{
		Exchange: &config.ExchangeConfig{Name: "china"},
	}, t.TempDir(), "")}
	if err := saveOrders([]*ormo.InOutOrder{
		reportTestOrder(1, startMS, startMS+1000, "zone", 1),
	}, path, deps); err != nil {
		t.Fatalf("save orders: %v", err)
	}
	data, err := os.ReadFile(path + ".csv")
	if err != nil {
		t.Fatalf("read orders csv: %v", err)
	}
	if !strings.Contains(string(data), "2024-01-01 08:30:00") {
		t.Fatalf("orders CSV did not use snapshot location: %s", data)
	}
}
