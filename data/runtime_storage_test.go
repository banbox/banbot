package data

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
)

type unavailableIdentityExchange struct {
	banexg.BanExchange
	info  *banexg.ExgInfo
	panic bool
}

func (e *unavailableIdentityExchange) Info() *banexg.ExgInfo {
	if e.panic {
		panic("metadata unavailable")
	}
	return e.info
}

func TestExplicitRuntimeRequiresStorage(t *testing.T) {
	deps := &RuntimeDeps{}
	queries, conn, err := deps.conn()
	if err == nil || queries != nil || conn != nil {
		t.Fatal("explicit data dependencies must reject missing storage")
	}
}

func TestExplicitRuntimeRequiresClockBeforeStorageConnection(t *testing.T) {
	deps := &RuntimeDeps{Storage: orm.NewStorage(nil, true, "runtime:no-clock")}
	queries, conn, err := deps.conn()
	if err == nil || queries != nil || conn != nil || !strings.Contains(err.Error(), "clock") {
		t.Fatalf("explicit data dependencies without clock = (%v, %v, %v), want clock error", queries, conn, err)
	}
}

func TestExplicitRuntimeTimeRequiresClock(t *testing.T) {
	if got := (&RuntimeDeps{}).timeMS(); got != 0 {
		t.Fatalf("missing explicit runtime clock returned %d, want 0", got)
	}
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(123)
	if got := (&RuntimeDeps{Clock: clock}).timeMS(); got != 123 {
		t.Fatalf("explicit runtime clock returned %d, want 123", got)
	}
}

func TestExplicitRuntimeLoggerWithoutCoreUsesFallback(t *testing.T) {
	if logger := (&RuntimeDeps{}).logger(); logger == nil {
		t.Fatal("runtime logger returned nil without core state")
	}
}

func TestExplicitRuntimeIdentityRejectsAdapterMismatch(t *testing.T) {
	deps := &RuntimeDeps{
		ExchangeName: "runtime",
		MarketType:   banexg.MarketSpot,
		Exchange:     &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "adapter", MarketType: banexg.MarketSpot}},
	}
	name, market := deps.identity()
	if name != "" || market != "" {
		t.Fatalf("mismatched runtime identity = %q/%q, want empty identity", name, market)
	}
}

func TestExplicitRuntimeIdentityRejectsPartialIdentity(t *testing.T) {
	for name, deps := range map[string]*RuntimeDeps{
		"exchange only": {ExchangeName: "runtime"},
		"market only":   {MarketType: banexg.MarketSpot},
		"core partial":  {Core: &core.State{ExgName: "runtime"}},
	} {
		t.Run(name, func(t *testing.T) {
			gotName, gotMarket := deps.identity()
			if gotName != "" || gotMarket != "" {
				t.Fatalf("partial runtime identity = %q/%q, want empty identity", gotName, gotMarket)
			}
		})
	}
}

func TestExplicitRuntimeIdentityRejectsUnavailableAdapterMetadata(t *testing.T) {
	for name, exchange := range map[string]banexg.BanExchange{
		"nil":   &unavailableIdentityExchange{},
		"empty": &unavailableIdentityExchange{info: &banexg.ExgInfo{}},
		"panic": &unavailableIdentityExchange{panic: true},
		"partial": &unavailableIdentityExchange{info: &banexg.ExgInfo{
			ID: "runtime",
		}},
	} {
		t.Run(name, func(t *testing.T) {
			deps := &RuntimeDeps{
				ExchangeName: "runtime",
				MarketType:   banexg.MarketSpot,
				Exchange:     exchange,
			}
			gotName, gotMarket := deps.identity()
			if gotName != "" || gotMarket != "" {
				t.Fatalf("unavailable adapter identity = %q/%q, want empty identity", gotName, gotMarket)
			}
		})
	}
}
