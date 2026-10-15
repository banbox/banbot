package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// BenchmarkSharedAdmission isolates the committed-order admission scan from
// the checkpoint decoding already performed by SharedOrderMgr.mutate. There
// are always four active bridge orders (two held, two pending) and two lots;
// only the number of ended checkpoint orders changes. It excludes account
// owner lock waits, stores/SQLite, quote/network IO and full runner execution.
func BenchmarkSharedAdmission(b *testing.B) {
	for _, ended := range []int{0, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("EndedOrders=%d", ended), func(b *testing.B) {
			manager, checkpoint, snapshot, symbol, request := sharedAdmissionBenchmarkFixture(b, ended)
			payload, err := json.Marshal(checkpoint)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("AllowEntry", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				b.ReportMetric(float64(len(payload)), "checkpoint_bytes")
				for i := 0; i < b.N; i++ {
					// Keep the same bar and persisted limits, without allocating a
					// fresh map or letting counters grow until requests are rejected.
					// These two reset assignments are included in the measurement.
					checkpoint.Admission.SimulOpen = 0
					checkpoint.Admission.Strategies[request.StratName] = 0
					allowed, err := manager.allowEntry(checkpoint, snapshot, symbol, "1m", request)
					if err != nil || !allowed {
						b.Fatalf("admission must stay on the accepted path: allowed=%v error=%v", allowed, err)
					}
				}
			})
			b.Run("CheckpointUnmarshal", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				b.ReportMetric(float64(len(payload)), "checkpoint_bytes")
				for i := 0; i < b.N; i++ {
					// Match mutate's fresh destination; reusing a decoded map would
					// hide the allocations and object creation of the real path.
					decoded := sharedTSCheckpoint{Version: checkpoint.Version, Orders: map[string]*sharedTSOrder{}}
					if err := json.Unmarshal(payload, &decoded); err != nil {
						b.Fatal(err)
					}
					if len(decoded.Orders) != ended+4 {
						b.Fatalf("checkpoint lost orders: got %d, want %d", len(decoded.Orders), ended+4)
					}
				}
			})
		})
	}
}

func sharedAdmissionBenchmarkFixture(b *testing.B, ended int) (*SharedOrderMgr, *sharedTSCheckpoint, execution.AccountSnapshot, *orm.ExSymbol, *strat.EnterReq) {
	b.Helper()
	const strategyName, accountName = "BenchTS", "bench-account"
	const barMS int64 = 1700000040000 // Aligned to the fixed one-minute clock.
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(barMS)
	state, err := core.NewState(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(state.Close)
	state.SetRunMode(core.RunModeBackTest)
	policy := &config.RunPolicyConfig{Name: strategyName, MaxOpen: 16, MaxSimulOpen: 4}
	cfg := &config.Config{
		MaxOpenOrders: 64, MaxSimulOpen: 8,
		Accounts:  map[string]*config.AccountConfig{accountName: {MaxOpenOrders: 32}},
		RunPolicy: []*config.RunPolicyConfig{policy},
	}
	manager := &SharedOrderMgr{deps: RuntimeDeps{
		Core: state, Clock: clock, Config: config.NewSnapshotWithDirs(cfg, "", ""), DefaultAccount: accountName,
	}}
	checkpoint := &sharedTSCheckpoint{
		Version: "admission-benchmark-v1", Serial: int64(ended + 4),
		Orders: make(map[string]*sharedTSOrder, ended+4),
		Admission: sharedEntryAdmission{
			BarMS: barMS, Strategies: map[string]int{strategyName: 0},
			AccountLimits: &sharedEntryLimits{MaxOpen: 32, MaxSimul: 8},
			PolicyLimits:  map[string]sharedEntryLimits{strategyName: {MaxOpen: 16, MaxSimul: 4}},
		},
	}
	account := execution.AccountKey{VenueSessionIdentity: "benchmark-paper", Account: accountName, SettlementDomain: "USDT"}
	instrument := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: decimal.NewFromInt(1), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1)}
	snapshot := execution.AccountSnapshot{AccountSettledCash: decimal.NewFromInt(100000), Checkpoint: int64(ended + 4)}
	for i := 0; i < ended+4; i++ {
		id := int64(i + 1)
		lot := execution.VirtualLotID(fmt.Sprintf("lot-%d", id))
		entry := execution.EligibleIntent{
			ID: execution.VirtualIntentID(fmt.Sprintf("entry-%d", id)), Account: account,
			Strategy: strategyName, Lot: lot, Instrument: instrument.ID, Kind: execution.EntryIntent,
			Side: execution.Buy, QuantitySteps: 1, FilledSteps: 1, State: execution.Filled,
		}
		order := &sharedTSOrder{ID: id, StrategyName: strategyName, Strategy: strategyName, Lot: lot, Symbol: "BTC/USDT:USDT", SID: 1, TimeFrame: "1m", Entry: entry, CreatedMS: barMS - 60000, Request: strat.EnterReq{StratName: strategyName, Amount: 1}}
		if i < 2 {
			order.Desired = 1
			snapshot.Lots = append(snapshot.Lots, execution.VirtualLot{Strategy: strategyName, ID: lot, Instrument: instrument, SignedSteps: 1})
		} else if i < 4 {
			order.Entry.FilledSteps, order.Entry.State = 0, execution.PendingCondition
		} else {
			exit := entry
			exit.ID, exit.Kind, exit.Side = execution.VirtualIntentID(fmt.Sprintf("exit-%d", id)), execution.ExitIntent, execution.Sell
			order.Exit = &exit
		}
		checkpoint.Orders[sharedOrderKey(strategyName, id)] = order
	}
	if len(snapshot.Lots) != 2 || len(checkpoint.Orders) != ended+4 {
		b.Fatal("invalid active/ended order fixture")
	}
	return manager, checkpoint, snapshot, &orm.ExSymbol{ID: 1, Symbol: "BTC/USDT:USDT"}, &strat.EnterReq{StratName: strategyName, Amount: 1}
}
