package biz

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// hubViewTestLot builds a lot of one long BTC order (step 0.1, 1 BTC entered at 100).
func hubViewTestLot(t *testing.T, m *HubOrderMgr, md func(*execution.TimeSeriesMetadata)) *hubProjectedLot {
	t.Helper()
	payload, _ := json.Marshal(TimeSeriesEntryPayload{QuantitySteps: 10, Request: strat.EnterReq{Amount: 1}, SID: 1, SourceInfo: map[string]any{"src": "request"}})
	meta := execution.TimeSeriesMetadata{OrderID: 7, StrategyName: "ts", Symbol: "BTC", TimeFrame: "ws", Payload: payload}
	if md != nil {
		md(&meta)
	}
	lot := &hubProjectedLot{Record: execution.TimeSeriesProjectionRecord{TimeSeriesLotKey: execution.TimeSeriesLotKey{Strategy: "a", Lot: "lot"}, Metadata: meta}, Entered: 10, EntryCost: decimal.NewFromInt(100)}
	m.journal.Lots[hubLotKey("a", "lot")] = lot
	return lot
}

func TestHubViewProfitFieldsOpenPartialAndClosed(t *testing.T) {
	m := hubProjectionTestManager(t)
	lot := hubViewTestLot(t, m, nil)
	view := func() *ormo.InOutOrder {
		od, err := m.viewLocked(lot, true)
		if err != nil {
			t.Fatal(err)
		}
		return od
	}
	if od := view(); od.Profit != 0 || od.ProfitRate != 0 {
		t.Fatal("no price known must stay realized-only", od.Profit, od.ProfitRate)
	}
	m.marks = map[string]float64{"BTC": 110}
	od := view()
	if od.Profit != 10 || od.ProfitRate != 0.1 || od.MaxPftRate != 0.1 {
		t.Fatal("open view lacks live profit", od.Profit, od.ProfitRate, od.MaxPftRate)
	}
	m.marks["BTC"] = 95
	od = view()
	if od.Profit != -5 || od.ProfitRate != -0.05 || od.MaxPftRate != 0.1 || od.MaxDrawDown != -5 {
		t.Fatal("running extremes not carried", od.Profit, od.ProfitRate, od.MaxPftRate, od.MaxDrawDown)
	}
	// Half exited at 110 (realized 5, fee 1): remaining 0.5 BTC marked at 120.
	lot.Exited, lot.ExitCost, lot.Realized, lot.Fees, lot.ExitFees = 5, decimal.NewFromInt(55), decimal.NewFromInt(5), decimal.NewFromInt(1), decimal.NewFromInt(1)
	m.marks["BTC"] = 120
	od = view()
	if od.Status != ormo.InOutStatusPartExit || od.Profit != 14 || od.ProfitRate != 0.14 || od.MaxPftRate != 0.14 {
		t.Fatal("partially exited view mis-valued", od.Status, od.Profit, od.ProfitRate, od.MaxPftRate)
	}
	// Fully exited: ledger result, rate like the archive row, price-independent.
	lot.Exited, lot.ExitCost, lot.Realized = 10, decimal.NewFromInt(115), decimal.NewFromInt(15)
	m.marks["BTC"] = 1
	od, err := m.viewLocked(lot, false)
	if err != nil {
		t.Fatal(err)
	}
	if od.Status != ormo.InOutStatusFullExit || od.Profit != 14 || od.ProfitRate != 0.14 || od.MaxPftRate != 0.14 {
		t.Fatal("closed view profit", od.Status, od.Profit, od.ProfitRate, od.MaxPftRate)
	}
	// Short orders profit from a falling price.
	short := hubViewTestLot(t, m, func(md *execution.TimeSeriesMetadata) { md.Short = true })
	short.Entered, short.EntryCost = 10, decimal.NewFromInt(100)
	m.marks["BTC"] = 90
	od, _ = m.viewLocked(short, true)
	if od.Profit != 10 || od.ProfitRate != 0.1 {
		t.Fatal("short profit", od.Profit, od.ProfitRate)
	}
}

func TestHubViewStrategyInfoSurvivesRebuildCloneAndRestart(t *testing.T) {
	m := hubProjectionTestManager(t)
	lot := hubViewTestLot(t, m, nil)
	view1, _ := m.viewLocked(lot, true)
	view1.SetInfo("grid", 3)
	view1.SetInfo("src", "changed") // overrides an entry-request key
	view1.SetInfo("shared_lot", "evil")
	view1.SetInfo(ormo.OdInfoStopLoss, "evil")
	view1.SetInfo("bad", make(chan int))
	view2, _ := m.viewLocked(lot, true)
	if view2.Info["grid"] != 3.0 || view2.Info["src"] != "changed" {
		t.Fatal("strategy Info lost on rebuild", view2.Info)
	}
	if view2.Info["shared_lot"] != "lot" || view2.Info[ormo.OdInfoStopLoss] != nil {
		t.Fatal("strategy key overrode a system key", view2.Info)
	}
	if _, ok := view2.Info["bad"]; ok {
		t.Fatal("non-serialisable value must not be carried")
	}
	// Callback clones keep the hook: writes made on them persist too.
	clone := view2.Clone()
	if clone.Info["grid"] != 3.0 {
		t.Fatal("clone lacks strategy Info", clone.Info)
	}
	clone.SetInfo("fromCallback", "yes")
	clone.SetInfo("grid", nil)
	view3, _ := m.viewLocked(lot, false)
	if view3.Info["fromCallback"] != "yes" {
		t.Fatal("callback write lost", view3.Info)
	}
	if _, ok := view3.Info["grid"]; ok {
		t.Fatal("deleted key reappeared", view3.Info)
	}
	view3.SetInfo("src", nil)
	if v, _ := m.viewLocked(lot, true); v.Info["src"] != nil {
		t.Fatal("deleted entry-request key reappeared", v.Info)
	}
	// Restart: the journal alone restores it.
	view3.SetInfo("grid", 5)
	if _, err := m.viewLocked(lot, true); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(m.journal)
	if err != nil {
		t.Fatal(err)
	}
	restored := hubProjectionTestManager(t)
	if err = json.Unmarshal(body, &restored.journal); err != nil {
		t.Fatal(err)
	}
	restored.journal.Lots = map[string]*hubProjectedLot{hubLotKey("a", "lot"): restored.journal.Lots[hubLotKey("a", "lot")]}
	if err = restored.restoreViewsLocked(); err != nil {
		t.Fatal(err)
	}
	ods, lock := restored.deps.Orders.GetOpenODs("robot")
	lock.Lock()
	od := ods[7]
	lock.Unlock()
	if od == nil || od.Info["grid"] != 5.0 || od.Info["fromCallback"] != "yes" {
		t.Fatal("strategy Info lost across restart", od)
	}
}

func TestHubViewPartialExplicitExitKeepsRemainderClosable(t *testing.T) {
	m := hubProjectionTestManager(t)
	lot := hubViewTestLot(t, m, func(md *execution.TimeSeriesMetadata) {
		md.ExitTag, md.ExitQuantitySteps = "half", 5
	})
	od, _ := m.viewLocked(lot, false) // acceptance view: 5 of 10 steps
	if od.ExitTag != "" || !od.CanClose() {
		t.Fatal("partial exit marked the whole order as closing", od.ExitTag)
	}
	if tags, _ := od.Info["exit_tags"].([]string); len(tags) != 1 || tags[0] != "half" {
		t.Fatal("partial exit tag not recorded", od.Info["exit_tags"])
	}
	// The 5 steps filled: the command size now equals what is held, but it was
	// partial when accepted.
	lot.Exited, lot.ExitCost, lot.Realized = 5, decimal.NewFromInt(55), decimal.NewFromInt(5)
	od, _ = m.viewLocked(lot, true)
	if od.Status != ormo.InOutStatusPartExit || od.ExitTag != "" || !od.CanClose() {
		t.Fatal("remainder after a filled partial exit is not closable", od.Status, od.ExitTag)
	}
	// A protection then ends the lot: its reason labels the order.
	lot.Exited, lot.ExitCost, lot.Realized = 10, decimal.NewFromInt(115), decimal.NewFromInt(15)
	lot.AutoExitTag, lot.AutoExitTags = "stop_loss", []string{"stop_loss"}
	od, _ = m.viewLocked(lot, true)
	if od.Status != ormo.InOutStatusFullExit || od.ExitTag != "stop_loss" {
		t.Fatal("protection exit after partial exit mislabelled", od.ExitTag)
	}
}

func TestHubViewFullExplicitExitKeepsTag(t *testing.T) {
	m := hubProjectionTestManager(t)
	lot := hubViewTestLot(t, m, func(md *execution.TimeSeriesMetadata) {
		md.ExitTag, md.ExitQuantitySteps = "all", 10
	})
	od, _ := m.viewLocked(lot, true)
	if od.ExitTag != "all" || od.CanClose() {
		t.Fatal("in-flight full exit lost its tag", od.ExitTag)
	}
	lot.Exited = 4 // partially filled full exit
	od, _ = m.viewLocked(lot, true)
	if od.ExitTag != "all" || od.CanClose() {
		t.Fatal("part-filled full exit lost its tag", od.ExitTag)
	}
	lot.Exited = 10
	if od, _ = m.viewLocked(lot, true); od.ExitTag != "all" {
		t.Fatal("closed order lost its tag", od.ExitTag)
	}
	// Journals / metadata without a reduction size keep the old behaviour.
	legacy := hubViewTestLot(t, m, func(md *execution.TimeSeriesMetadata) { md.ExitTag = "oco_cancel" })
	if od, _ = m.viewLocked(legacy, true); od.ExitTag != "oco_cancel" {
		t.Fatal("tag without size must stay", od.ExitTag)
	}
}

func TestHubViewCancelledEntryRemainderBecomesFullEnter(t *testing.T) {
	m := hubProjectionTestManager(t)
	lot := hubViewTestLot(t, m, nil)
	lot.Entered, lot.EntryCost = 4, decimal.NewFromInt(40)
	key := hubLotKey("a", "lot")
	m.pending[key] = execution.PendingDefinition{Order: execution.PendingOrder{Strategy: "a", Lot: "lot", Entry: execution.EligibleIntent{QuantitySteps: 10, FilledSteps: 4, State: execution.Partial}}}
	if od, _ := m.viewLocked(lot, true); od.Status != ormo.InOutStatusPartEnter || od.Enter.Status != ormo.OdStatusPartOK {
		t.Fatal("live partial entry must stay PartEnter", od.Status)
	}
	for _, state := range []execution.IntentState{execution.Canceled, execution.Expired} {
		m.pending[key] = execution.PendingDefinition{Order: execution.PendingOrder{Strategy: "a", Lot: "lot", Entry: execution.EligibleIntent{QuantitySteps: 4, FilledSteps: 4, State: state}}}
		od, _ := m.viewLocked(lot, true)
		if od.Status != ormo.InOutStatusFullEnter || od.Enter.Status != ormo.OdStatusClosed || od.Enter.Amount != 0.4 {
			t.Fatal("cancelled remainder keeps PartEnter", state, od.Status, od.Enter.Amount)
		}
	}
	m.pending[key] = execution.PendingDefinition{Order: execution.PendingOrder{Strategy: "a", Lot: "lot", Canceled: true, Entry: execution.EligibleIntent{QuantitySteps: 10, State: execution.Partial}}}
	if od, _ := m.viewLocked(lot, true); od.Status != ormo.InOutStatusFullEnter {
		t.Fatal("cancelled order flag ignored", od.Status)
	}
}

// Through poll: the registry view and the exit-fill callback carry profit
// fields, and strategy Info set on the registry object survives the next poll.
func TestHubPollPublishesProfitFieldsAndKeepsStrategyInfo(t *testing.T) {
	m := hubProjectionTestManager(t)
	payload, _ := json.Marshal(TimeSeriesEntryPayload{QuantitySteps: 10, Request: strat.EnterReq{Amount: 1}, SID: 1})
	entry := execution.TimeSeriesMetadata{OrderID: 1, StrategyName: "ts", Symbol: "BTC", TimeFrame: "ws", Payload: payload}
	record := execution.TimeSeriesProjectionRecord{TimeSeriesLotKey: execution.TimeSeriesLotKey{Strategy: "a", Lot: "lot"}, Metadata: entry}
	fillEntry, _ := json.Marshal(execution.FillReport{OrderID: "entry", Steps: 10, Price: decimal.NewFromInt(100)})
	fillExit, _ := json.Marshal(execution.FillReport{OrderID: "exit", Steps: 10, Price: decimal.NewFromInt(110)})
	events := []execution.CommittedEvent{{ID: "filled-entry", Kind: "ExchangeFill", Checkpoint: 1, Payload: fillEntry, Ledger: []execution.LedgerEntry{{ID: 1, Strategy: "a", Lot: "lot", QuantityDelta: 10}}}}
	var mu sync.Mutex
	page := execution.TimeSeriesProjectionPage{Snapshot: execution.AccountSnapshot{Checkpoint: 1, Lots: []execution.VirtualLot{{Strategy: "a", ID: "lot", SignedSteps: 10}}}, Records: []execution.TimeSeriesProjectionRecord{record}, Events: events, Pending: []execution.PendingDefinition{{Order: execution.PendingOrder{Strategy: "a", Lot: "lot", Entry: execution.EligibleIntent{QuantitySteps: 10, FilledSteps: 10}}}}}
	cursor := int64(0)
	dir, err := os.MkdirTemp("", "hub-view-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	listener, err := net.Listen("unix", filepath.Join(dir, "s"))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/v1/ts-projection" {
			p := page
			p.Cursor = cursor
			json.NewEncoder(w).Encode(map[string]any{"TSProjection": p})
			return
		}
		var req struct{ Through int64 }
		json.NewDecoder(r.Body).Decode(&req)
		cursor = max(cursor, req.Through)
		w.Write([]byte(`{}`))
	})}
	go server.Serve(listener)
	defer server.Close()
	client := execution.NewHubClient(filepath.Join(dir, "s"))
	defer client.Close()
	m.config.Client = client
	var exitFill *ormo.InOutOrder
	job := &strat.StratJob{Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, Strat: &strat.TradeStrat{Name: "ts"}, TimeFrame: "ws"}
	job.Strat.OnOrderChange = func(_ *strat.StratJob, od *ormo.InOutOrder, kind int) {
		if kind == strat.OdChgExitFill {
			exitFill = od
		}
	}
	m.jobs[sharedJobIdentity{"ts", "BTC", "ws"}] = job
	m.marks = map[string]float64{"BTC": 120}
	if err = m.poll(false); err != nil {
		t.Fatal(err)
	}
	ods, lock := m.deps.Orders.GetOpenODs("robot")
	registry := func() *ormo.InOutOrder {
		lock.Lock()
		defer lock.Unlock()
		return ods[1]
	}
	od := registry()
	if od == nil || od.ProfitRate != 0.2 || od.MaxPftRate != 0.2 {
		t.Fatal("registry view lacks live profit", od)
	}
	mu.Lock()
	page.Events = nil // the acknowledged event is not served again
	mu.Unlock()
	od.SetInfo("grid", 2) // a strategy callback writes bookkeeping
	if err = m.poll(false); err != nil {
		t.Fatal(err)
	}
	if next := registry(); next == od || next.Info["grid"] != 2.0 {
		t.Fatal("strategy Info lost when the registry view was rebuilt", next.Info)
	}
	// The whole lot exits at 110: callback sees final profit and the high-water mark.
	mu.Lock()
	page.Snapshot.Checkpoint = 2
	page.Snapshot.Lots = nil
	page.Events = []execution.CommittedEvent{{ID: "filled-exit", Kind: "ExchangeFill", Checkpoint: 2, Payload: fillExit, Ledger: []execution.LedgerEntry{{ID: 2, Strategy: "a", Lot: "lot", QuantityDelta: -10, RealizedPnL: decimal.NewFromInt(10)}}}}
	page.Pending = nil
	mu.Unlock()
	if err = m.poll(false); err != nil {
		t.Fatal(err)
	}
	if exitFill == nil || exitFill.Status != ormo.InOutStatusFullExit || exitFill.ProfitRate != 0.1 || exitFill.MaxPftRate != 0.2 || exitFill.Info["grid"] != 2.0 {
		t.Fatal("exit-fill callback order lacks profit fields / strategy Info", exitFill)
	}
	if final := registry(); final.ProfitRate != 0.1 || final.MaxPftRate != 0.2 {
		t.Fatal("closed registry view", final.ProfitRate, final.MaxPftRate)
	}
}
