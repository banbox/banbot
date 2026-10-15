package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/shopspring/decimal"
	"os"
	"path/filepath"
	"testing"
)

type migrationRuntimeAdapter struct {
	execution.ExecutionAdapter
	venue   execution.LegacyVenueSnapshot
	queries int
}

func (a *migrationRuntimeAdapter) MigrationSnapshot(context.Context) (execution.LegacyVenueSnapshot, error) {
	a.queries++
	return a.venue, nil
}
func (a *migrationRuntimeAdapter) Snapshot(context.Context) (execution.VenueSnapshot, error) {
	positions := map[string]int64{}
	for _, p := range a.venue.Positions {
		positions[p.Instrument.ID] = p.SignedSteps
	}
	return execution.VenueSnapshot{Cash: a.venue.AccountCash.String(), Positions: positions}, nil
}

func TestSharedLegacyCutoverJoinsImportsAndRestores(t *testing.T) {
	dir := t.TempDir()
	p := NewProcess()
	defer p.Close()
	old, err := p.NewRuntime(Options{Mode: core.RunModeBackTest})
	if err != nil {
		t.Fatal(err)
	}
	// The old manager is a real registered facade, removed only after its runtime joins.
	old.Trading.SetOrderManager("default", &biz.LocalOrderMgr{})
	i := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	row := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: 42, TaskID: 1, Symbol: "BTC", Sid: 1, Timeframe: "1m", Strategy: "legacy", EnterTag: "old", Leverage: 1, Status: ormo.InOutStatusFullEnter}, Enter: &ormo.ExOrder{Enter: true, Symbol: "BTC", Side: "buy", Amount: 1, Filled: 1, Average: 100}, Info: map[string]any{"custom": nil}}
	raw, _ := json.Marshal(row)
	backup := filepath.Join(dir, "backup.json")
	if err := os.WriteFile(backup, raw, 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	lot := execution.VirtualLot{Strategy: "ts", ID: "old-42", Instrument: i, SignedSteps: 10, CostBasis: decimal.NewFromInt(100)}
	actual := lot
	actual.Strategy = ""
	actual.ID = ""
	request := execution.LegacyMigration{ID: "cutover", SourceVersion: "0.3.8", SourceSnapshotID: "preserved", StoppedOwnerProof: "runtime-close-join", BackupPath: backup, BackupSHA256: hex.EncodeToString(digest[:]), AccountCash: decimal.NewFromInt(1000), StrategyCash: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}, Lots: []execution.VirtualLot{lot}, ActualPositions: []execution.VirtualLot{actual}, RawLegacyMap: []execution.LegacySourceMapping{{Strategy: "ts", Lot: lot.ID, TaskID: 1, IOrderID: 42, RawJSON: raw}}, VenueSnapshot: execution.LegacyVenueSnapshot{Complete: true, AccountCash: decimal.NewFromInt(1000), Positions: []execution.VirtualLot{actual}, AtMS: 100}}
	paper, _ := runner.NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	adapter := &migrationRuntimeAdapter{ExecutionAdapter: paper, venue: request.VenueSnapshot}
	adapter.venue.Positions = append([]execution.VirtualLot(nil), adapter.venue.Positions...)
	adapter.venue.Positions[0].CostBasis = decimal.NewFromInt(110)
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: "default", SettlementDomain: "USDT"}
	bridge := &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{"BTC": i}, Strategies: map[string]biz.SharedStrategyBinding{"legacy": {ID: "ts", StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(1000)}}, Risk: execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(1000), MaxVirtualGross: decimal.NewFromInt(1000), StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{"ts": decimal.NewFromInt(1000)}}, IntentTTLMS: 1000, Quote: func(_ string, now int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000}, nil
	}}
	rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &key, SharedExecution: &biz.SharedExecutionOptions{StorePath: filepath.Join(dir, "ledger.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Adapter: adapter, AuthoritativeSnapshot: true}, SharedOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.CutoverLegacyExecution(old, request); err == nil {
		t.Fatal("changed actual basis accepted")
	}
	select {
	case <-old.Done():
	default:
		t.Fatal("old callback intake still open")
	}
	if len(old.Trading.OrderManagersSnapshot()) != 0 {
		t.Fatal("dual manager survived")
	}
	failed, err := rt.SharedExecution().Snapshot(context.Background())
	if err != nil || !failed.RiskFrozen || len(failed.Lots) != 0 {
		t.Fatal("pending migration partially imported", failed, err)
	}
	adapter.venue = request.VenueSnapshot
	adapter.venue.Positions = append([]execution.VirtualLot(nil), request.VenueSnapshot.Positions...)
	adapter.venue.Positions[0].Instrument.Version = "changed"
	if _, err := rt.CutoverLegacyExecution(old, request); err == nil {
		t.Fatal("changed venue instrument descriptor accepted")
	}
	failed, err = rt.SharedExecution().Snapshot(context.Background())
	if err != nil || !failed.RiskFrozen || len(failed.Lots) != 0 {
		t.Fatal("descriptor mismatch partially imported", failed, err)
	}
	adapter.venue = request.VenueSnapshot
	if applied, err := rt.CutoverLegacyExecution(old, request); err != nil || !applied {
		t.Fatal(applied, err)
	}
	if adapter.queries < 3 {
		t.Fatal("fresh preflight was not checked at both import gates")
	}
	if applied, err := rt.CutoverLegacyExecution(old, request); err != nil || applied {
		t.Fatal("migration not idempotent", applied, err)
	}
	rt.Clock.SetTimeMS(101)
	if err := rt.SharedExecution().Reconcile("import-ready", 101); err != nil {
		t.Fatal(err)
	}
	policy, err := rt.SharedExecution().AccountRisk(context.Background(), 101)
	if err != nil || !policy.StrategyGrossLimits["ts"].Equal(bridge.Risk.StrategyGrossLimits["ts"]) {
		t.Fatal("migration lost explicit account strategy policy", policy, err)
	}
	callbacks := 0
	biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), func(*ormo.InOutOrder, bool) { callbacks++ }, false)
	rows, lock := rt.Orders.GetOpenODs("default")
	lock.Lock()
	restored := rows[42]
	lock.Unlock()
	if restored == nil || restored.TaskID != 1 || restored.Enter.Filled != 1 || restored.Enter.Side != "buy" || restored.Info["shared_lot"] != "old-42" || callbacks != 0 {
		t.Fatal("source identity/projection restore failed", restored, callbacks)
	}
	if value, ok := restored.Info["custom"]; !ok || value != nil {
		t.Fatal("legacy custom NULL metadata not restored", restored.Info)
	}
	bytes, err := os.ReadFile(backup)
	if err != nil || string(bytes) != string(raw) {
		t.Fatal("source backup modified", err)
	}
}
