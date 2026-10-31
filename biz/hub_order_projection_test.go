package biz

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func hubProjectionTestManager(t *testing.T) *HubOrderMgr {
	t.Helper()
	i := execution.Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	return &HubOrderMgr{deps: RuntimeDeps{Orders: ormo.NewOrderState(), Strategies: strat.NewState(), Clock: btime.NewClockState(true, nil), DefaultAccount: "robot"}, config: HubOrderBridgeConfig{StatePath: filepath.Join(t.TempDir(), "robot.json"), Bridge: SharedOrderBridgeConfig{Instruments: map[string]execution.Instrument{"BTC": i}, Strategies: map[string]SharedStrategyBinding{"ts": {ID: "a"}}}}, journal: hubOrderJournal{Session: "session", Lots: map[string]*hubProjectedLot{}, FillTotals: map[string]hubFillTotal{}, OrderKinds: map[string]map[string]execution.IntentKind{}}, jobs: map[sharedJobIdentity]*strat.StratJob{}, pending: map[string]execution.PendingDefinition{}, ctx: context.Background()}
}
func TestHubProjectionExactCumulativeNotionalExcludesAccountPostings(t *testing.T) {
	m := hubProjectionTestManager(t)
	records := map[string]execution.TimeSeriesProjectionRecord{hubLotKey("a", "lot"): {TimeSeriesLotKey: execution.TimeSeriesLotKey{Strategy: "a", Lot: "lot"}, Metadata: execution.TimeSeriesMetadata{Symbol: "BTC"}}}
	cost := func(report execution.FillReport, a, b int64) map[int64]decimal.Decimal {
		body, _ := json.Marshal(report)
		event := execution.CommittedEvent{Kind: "ExchangeFill", Payload: body, Ledger: []execution.LedgerEntry{{ID: 1, Strategy: "a", Lot: "lot", QuantityDelta: a}, {ID: 2, Strategy: "factor", Lot: "other", QuantityDelta: b}, {ID: 3, Kind: "RealAccountFill", QuantityDelta: a + b}}}
		got, err := m.eventCostsLocked(event, records)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	first := cost(execution.FillReport{OrderID: "venue", Steps: 5, Price: decimal.NewFromInt(100)}, 3, 2)
	if !first[1].Equal(decimal.NewFromInt(30)) || !first[2].Equal(decimal.NewFromInt(20)) || !first[3].IsZero() {
		t.Fatal("actual position posting counted twice", first)
	}
	next := cost(execution.FillReport{OrderID: "venue", Steps: 10, Cumulative: true, Price: decimal.NewFromInt(120), Cost: decimal.NewFromInt(120)}, 2, 3)
	if !next[1].Equal(decimal.NewFromInt(28)) || !next[2].Equal(decimal.NewFromInt(42)) {
		t.Fatal("cumulative report treated lifetime average as incremental fill", next)
	}
	body, _ := json.Marshal(m.journal)
	var restored hubOrderJournal
	if err := json.Unmarshal(body, &restored); err != nil {
		t.Fatal(err)
	}
	m.journal = restored
	final := cost(execution.FillReport{OrderID: "venue", Steps: 12, Cumulative: true, Price: decimal.NewFromInt(130), Cost: decimal.NewFromInt(156)}, 1, 1)
	if !final[1].Equal(decimal.NewFromInt(18)) || !final[2].Equal(decimal.NewFromInt(18)) {
		t.Fatal("cumulative highwater lost on restart", final)
	}
}
func TestHubProjectionEventTimeExitMetadataAndLostAcknowledgement(t *testing.T) {
	m := hubProjectionTestManager(t)
	payload, _ := json.Marshal(TimeSeriesEntryPayload{QuantitySteps: 10, Request: strat.EnterReq{Amount: 1}, SID: 1})
	entry := execution.TimeSeriesMetadata{OrderID: 1, StrategyName: "ts", Symbol: "BTC", TimeFrame: "1m", Payload: payload}
	exit := entry
	exit.ExitTag = "exit"
	exit.ExitQuantitySteps = 5
	exit.ExitConditions = &execution.IntentConditions{Limit: decimal.NewFromInt(110)}
	latest := exit
	latest.ExitQuantitySteps = 3
	latest.ExitConditions = &execution.IntentConditions{Limit: decimal.NewFromInt(120)}
	record := execution.TimeSeriesProjectionRecord{TimeSeriesLotKey: execution.TimeSeriesLotKey{Strategy: "a", Lot: "lot"}, Metadata: latest}
	acceptEntry, _ := json.Marshal(execution.StrategyAcceptedEvent{Strategy: "a", Lot: "lot", Kind: execution.EntryIntent, Metadata: &entry})
	acceptExit, _ := json.Marshal(execution.StrategyAcceptedEvent{Strategy: "a", Lot: "lot", Kind: execution.ExitIntent, Metadata: &exit})
	fillEntry, _ := json.Marshal(execution.FillReport{OrderID: "entry", Steps: 10, Price: decimal.NewFromInt(100)})
	fillExit, _ := json.Marshal(execution.FillReport{OrderID: "exit", Steps: 5, Price: decimal.NewFromInt(110)})
	events := []execution.CommittedEvent{{ID: "accepted-entry", Kind: "StrategyAccepted", Checkpoint: 1, Payload: acceptEntry}, {ID: "filled-entry", Kind: "ExchangeFill", Checkpoint: 2, Payload: fillEntry, Ledger: []execution.LedgerEntry{{ID: 1, Strategy: "a", Lot: "lot", QuantityDelta: 10}}}, {ID: "accepted-exit", Kind: "StrategyAccepted", Checkpoint: 3, Payload: acceptExit}, {ID: "filled-exit", Kind: "ExchangeFill", Checkpoint: 4, Payload: fillExit, Ledger: []execution.LedgerEntry{{ID: 2, Strategy: "a", Lot: "lot", QuantityDelta: -5, RealizedPnL: decimal.NewFromInt(5)}}}}
	page := execution.TimeSeriesProjectionPage{Snapshot: execution.AccountSnapshot{Checkpoint: 4}, Records: []execution.TimeSeriesProjectionRecord{record}, Events: events, Pending: []execution.PendingDefinition{{Order: execution.PendingOrder{Strategy: "a", Lot: "lot", Entry: execution.EligibleIntent{QuantitySteps: 10}, Exit: &execution.EligibleIntent{QuantitySteps: 3, Conditions: execution.IntentConditions{Limit: decimal.NewFromInt(120)}}}}}}
	dir, err := os.MkdirTemp("", "hub-project-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	listener, err := net.Listen("unix", filepath.Join(dir, "s"))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	cursor := int64(0)
	acks := 0
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/v1/ts-projection" {
			p := page
			p.Cursor = cursor
			json.NewEncoder(w).Encode(map[string]any{"TSProjection": p})
		} else {
			acks++
			if acks == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			var req struct{ Through int64 }
			json.NewDecoder(r.Body).Decode(&req)
			cursor = max(cursor, req.Through)
			w.Write([]byte(`{}`))
		}
	})}
	go server.Serve(listener)
	defer server.Close()
	client := execution.NewHubClient(filepath.Join(dir, "s"))
	defer client.Close()
	m.config.Client = client
	var callbacks []*ormo.InOutOrder
	m.callback = func(od *ormo.InOutOrder, _ bool) { callbacks = append(callbacks, od) }
	job := &strat.StratJob{Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, Strat: &strat.TradeStrat{Name: "ts"}, TimeFrame: "1m"}
	var accepted *ormo.InOutOrder
	job.Strat.OnOrderChange = func(_ *strat.StratJob, od *ormo.InOutOrder, kind int) {
		if kind == strat.OdChgExit {
			accepted = od
		}
	}
	m.jobs[sharedJobIdentity{"ts", "BTC", "1m"}] = job
	// First ACK is lost after its callback/state journal commit. Retry skips the
	// committed fact rather than delivering acceptance/fills a second time.
	if err = m.poll(false); err == nil {
		t.Fatal("injected ACK failure missing")
	}
	if err = m.poll(false); err != nil {
		t.Fatal(err)
	}
	if len(callbacks) != 2 {
		t.Fatal("lost ACK duplicated fill callbacks", len(callbacks))
	}
	if callbacks[0].Exit != nil || callbacks[0].Enter.Average != 100 || callbacks[0].Enter.Filled != 1 || callbacks[0].Profit != 0 {
		t.Fatal("entry callback saw later exit snapshot", callbacks[0])
	}
	if accepted == nil || accepted.Exit == nil || accepted.Exit.Amount != 0.5 || accepted.Exit.Price != 110 {
		t.Fatal("exit acceptance leaked later edit", accepted)
	}
	if callbacks[1].Enter.Average != 100 || callbacks[1].Exit.Average != 110 || callbacks[1].Exit.Filled != 0.5 || callbacks[1].Profit != 5 {
		t.Fatal("event-time exit costs lost", callbacks[1])
	}
}
