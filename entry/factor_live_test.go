package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

func TestFactorLiveUnsupportedBindingFailsClosedBeforeRuntimeIO(t *testing.T) {
	err := runFactorLive(context.Background(), runner.Config{}, "unsupported-binding", []string{"does-not-exist.yml"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), `binding "unsupported-binding" is not registered`) {
		t.Fatalf("unsupported binding: %v", err)
	}
	err = runFactorLive(context.Background(), runner.Config{Chunks: []runner.Chunk{{Path: "archive.gob"}}}, "", nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "refuses archive") {
		t.Fatalf("real archive execution admitted: %v", err)
	}
}

func TestFactorLiveStaticPreflightRunsBeforeSessionAndFunding(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	spec, err := loadFactorRunSpec([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Events)
	if err != nil {
		t.Fatal(err)
	}
	c := configs[0]
	c.Mode = runner.Trade
	c.Chunks = nil
	c.Execution = runner.ExecutionConfig{StorePath: filepath.Join(dir, "ledger", "live.db"), SenderLeaseDir: filepath.Join(dir, "leases"), Instruments: map[int32]execution.Instrument{}, MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(10000), MaxVirtualGross: decimal.NewFromInt(20000), StrategyGrossLimit: decimal.NewFromInt(10000)}
	for sid, symbol := range c.Snapshot.SIDMap {
		c.Execution.Instruments[sid] = execution.Instrument{ID: symbol, Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.01"), PriceTick: decimal.RequireFromString("0.01"), ContractSize: decimal.NewFromInt(1), MoneyScale: 8}
	}
	if err := validateFactorLiveConfig(c); err != nil {
		t.Fatal("invalid baseline fixture", err)
	}
	name := t.Name()
	var factories, funding atomic.Int32
	if err := RegisterFactorLiveBinding(name, func(context.Context, banexg.BanExchange, *config.Snapshot, runner.Config) (FactorLiveBinding, error) {
		factories.Add(1)
		return FactorLiveBinding{}, errors.New("unexpected binding IO")
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { factorLiveBindings.Lock(); delete(factorLiveBindings.items, name); factorLiveBindings.Unlock() })
	binding := FactorLiveBinding{VerifyFunding: func(context.Context, string) (string, error) { funding.Add(1); return "verified", nil }}
	for _, test := range []struct {
		name, reason string
		change       func(*runner.Config)
	}{
		{"risk", "positive absolute risk", func(c *runner.Config) { c.Execution.MarginRate = decimal.Zero }},
		{"path", "must be absolute", func(c *runner.Config) { c.Execution.StorePath = "relative.db" }},
		{"window", "execution window", func(c *runner.Config) { c.DecisionInterval = 0 }},
		{"unit", "instrument identity", func(c *runner.Config) { c.Manifest.Currency = "OTHER" }},
		{"history-IC", "matured history provider", func(c *runner.Config) { c.Combo.Method = research.HistoryIC }},
		{"manifest", "manifest", func(c *runner.Config) { c.Manifest.CodeRevision = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := c
			test.change(&invalid)
			if err := runFactorLiveSpec(context.Background(), spec, []runner.Config{invalid}, name, io.Discard); err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("invalid config reached session initialization: %v", err)
			}
			if err := (&explicitEntrySession{}).runFactorLive(context.Background(), nil, invalid, binding, io.Discard); err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("embedded entry deferred static validation: %v", err)
			}
		})
	}
	other := c
	other.StrategyID += "-other"
	other.Execution.MaxVirtualGross = decimal.NewFromInt(30000)
	if err := runFactorLiveSpec(context.Background(), spec, []runner.Config{c, other}, name, io.Discard); err == nil || !strings.Contains(err.Error(), "incompatible shared live") {
		t.Fatal("group incompatibility checked after session initialization", err)
	}
	if factories.Load() != 0 || funding.Load() != 0 {
		t.Fatal("invalid config invoked capability IO", factories.Load(), funding.Load())
	}
	for _, resource := range []string{"ledger", "leases"} {
		if _, err := os.Stat(filepath.Join(dir, resource)); !os.IsNotExist(err) {
			t.Fatal("preflight created execution resources", resource, err)
		}
	}
}

func TestFactorLivePreflightKeepsLiveSemantics(t *testing.T) {
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	c.Mode, c.Chunks, c.InitialNAV = runner.Trade, nil, 0
	c.Execution.StorePath = filepath.Join(dir, "live.db")
	c.Execution.SenderLeaseDir = filepath.Join(dir, "leases")
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	// A live definition can retain several research labels; the replay-only
	// exactly-one executable-label rule does not apply to live computation.
	c.Manifest.Labels = []research.LabelSpec{
		{Name: "close", Kind: research.CloseToClose, Horizon: 100, PeriodsPerYear: 8760},
		{Name: "forward", Kind: research.ExecutableReturn, Horizon: 200, PeriodsPerYear: 8760},
	}
	c.Snapshot.Universe = factor.Universe{Version: "live", Static: true, Tracked: []int32{1}, Investable: []int32{1, 2}, Tradable: []int32{1}, Reference: []int32{3}}
	unit := c.Execution.Instruments[1]
	// Venue symbols and stable execution instrument IDs are separate identities.
	unit.ID = "stable-contract-id"
	c.Execution.Instruments = map[int32]execution.Instrument{1: unit}
	if err := validateFactorLiveConfig(c); err != nil {
		t.Fatalf("valid live config rejected before resource preparation: %v", err)
	}
	if _, err := os.Stat(c.Execution.StorePath); !os.IsNotExist(err) {
		t.Fatal("static validation created a ledger", err)
	}
}

func TestFactorLiveRejectsMissingUntrackedExecutionUnitsBeforeIO(t *testing.T) {
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	c.Chunks = nil
	c.Snapshot.Universe = factor.Universe{Version: "missing-unit", Static: true, Tracked: []int32{1}, Investable: []int32{2}, Tradable: []int32{2}, Reference: []int32{3}}
	delete(c.Execution.Instruments, 2)
	delete(c.Execution.Instruments, 3)
	c.Execution.StorePath, c.Execution.SenderLeaseDir = filepath.Join(dir, "live.db"), filepath.Join(dir, "leases")
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: c.AccountID, SettlementDomain: c.Manifest.Currency}
	transport := &liveEntryTransport{key: key}
	exchange := &liveEntryExchange{trades: make(chan *banexg.MyTrade)}
	binding := FactorLiveBinding{Account: key, Transport: transport, Symbols: map[int32]*orm.ExSymbol{}, VerifyFunding: func(context.Context, string) (string, error) { return "verified", nil }, Record: func(*orm.DataSeries, int64) (factor.VersionRecord, error) { return factor.VersionRecord{}, nil }}
	for sid, symbol := range c.Snapshot.SIDMap {
		binding.Symbols[sid] = &orm.ExSymbol{ID: sid, Exchange: "entrytest", Market: "linear", Symbol: symbol}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	session := &explicitEntrySession{process: runtime.NewProcess(), ctx: ctx, cancel: cancel, exchange: exchange}
	defer session.close()
	snapshot := config.NewSnapshotWithDirs(&config.Config{Exchange: &config.ExchangeConfig{Name: "entrytest"}, MarketType: "linear"}, dir, "", nil)
	err = session.runFactorLive(ctx, snapshot, c, binding, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "execution live SID 2 has no instrument units") {
		t.Fatal("missing investable-tradable units admitted", err)
	}
	if transport.verified.Load() != 0 || exchange.snapshots.Load() != 0 {
		t.Fatal("unit validation happened after transport/recovery IO")
	}
}

func TestFactorCommandPreservesRunnerAndCleanupErrors(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	spec, err := loadFactorRunSpec([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Events)
	if err != nil {
		t.Fatal(err)
	}
	c := configs[0]
	c.Execution = runner.ExecutionConfig{Instruments: map[int32]execution.Instrument{}, MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(10000), MaxVirtualGross: decimal.NewFromInt(20000), StrategyGrossLimit: decimal.NewFromInt(10000)}
	for sid, symbol := range c.Snapshot.SIDMap {
		c.Execution.Instruments[sid] = execution.Instrument{ID: symbol, Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USD", QuantityStep: decimal.RequireFromString("0.01"), PriceTick: decimal.RequireFromString("0.01"), ContractSize: decimal.NewFromInt(1), MoneyScale: 8}
	}
	body, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(dir, "valid.json")
	if err := os.WriteFile(legacy, body, 0o600); err != nil {
		t.Fatal(err)
	}
	primaryErr := errors.New("execution sink failed")
	cleanupErr := errors.New("ledger close failed")
	cmd := NewFactorCommandWithSink(func(context.Context, runner.Config, bool) (runner.Sink, func() error, error) {
		return &failingEntryFactorSink{primaryErr}, func() error { return cleanupErr }, nil
	})
	cmd.SetArgs([]string{"backtest", "--mode", "events", "--factor-config", legacy})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	err = cmd.Execute()
	if !errors.Is(err, cleanupErr) || !errors.Is(err, primaryErr) {
		t.Fatalf("primary/cleanup error lost: %v", err)
	}
}

type failingEntryFactorSink struct{ err error }

func (s *failingEntryFactorSink) StrategyNAV(context.Context, int64) (float64, error) {
	return 0, s.err
}

func (s *failingEntryFactorSink) StrategyState(context.Context, int64) (backtest.State, error) {
	return backtest.State{}, s.err
}

func (s *failingEntryFactorSink) ProcessSnapshot(context.Context, *factor.TargetPortfolio, map[int32]backtest.Quote, int64) error {
	return s.err
}

type liveEntryExchange struct {
	banexg.BanExchange
	trades    chan *banexg.MyTrade
	snapshots atomic.Int32
	cash      float64
}

func (e *liveEntryExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "entrytest", MarketType: "linear"}
}
func (e *liveEntryExchange) HasApi(string, string) bool { return true }
func (e *liveEntryExchange) FetchBalance(map[string]any) (*banexg.Balances, *errs.Error) {
	e.snapshots.Add(1)
	return &banexg.Balances{Total: map[string]float64{"USD": e.cash}}, nil
}
func (e *liveEntryExchange) FetchPositions([]string, map[string]any) ([]*banexg.Position, *errs.Error) {
	return nil, nil
}
func (e *liveEntryExchange) FetchOpenOrders(string, int64, int, map[string]any) ([]*banexg.Order, *errs.Error) {
	return nil, nil
}
func (e *liveEntryExchange) WatchMyTrades(map[string]any) (chan *banexg.MyTrade, *errs.Error) {
	return e.trades, nil
}
func (e *liveEntryExchange) Close() *errs.Error { return nil }

type liveEntryTransport struct {
	key      execution.AccountKey
	verified atomic.Int32
}

func (p *liveEntryTransport) Verify(context.Context, banexg.BanExchange, execution.AccountKey) (execution.BanexgExecutionProof, error) {
	p.verified.Add(1)
	return execution.BanexgExecutionProof{Account: p.key, EvidenceID: "entry-fake-verified", ContextBound: true, StableClientID: true, QueryClientID: true, CompleteCumulativeReports: true, CompleteAccountSnapshot: true, SettledCash: true, NetLinearPositions: true}, nil
}
func (*liveEntryTransport) Invoke(ctx context.Context, call func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return call()
}

type liveEntrySource struct {
	name           string
	cancel         context.CancelFunc
	emitted        *atomic.Int32
	joined         chan struct{}
	failure        error
	subscribed     func([]*strat.DataSub) error
	closed         bool
	mapped         *atomic.Int32
	expectedMapped int32
}

type liveEntryHandle struct {
	cancel   context.CancelFunc
	done     chan struct{}
	failures chan error
	err      error
}

func (h *liveEntryHandle) Stop()                { h.cancel() }
func (h *liveEntryHandle) Join() error          { <-h.done; return h.err }
func (h *liveEntryHandle) Errors() <-chan error { return h.failures }
func (s *liveEntrySource) SubscribeManaged(ctx context.Context, subs []*orm.Subscription, sink data.DataSink) (data.LiveSourceSubscription, error) {
	ctx, cancel := context.WithCancel(ctx)
	h := &liveEntryHandle{cancel: cancel, done: make(chan struct{}), failures: make(chan error, 1)}
	go func() {
		defer close(h.done)
		if ready, ok := sink.(interface{ AwaitLiveReady(context.Context) error }); ok {
			if err := ready.AwaitLiveReady(ctx); err != nil {
				return
			}
		}
		if err := s.SubscribeLive(ctx, subs, sink); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			h.err = err
			h.failures <- err
		}
	}()
	return h, nil
}

func (s *liveEntrySource) Info() *orm.SeriesInfo {
	return orm.NewSeriesInfo(s.name, "event", []orm.SeriesField{{Name: "close", Type: "float"}, {Name: "integer", Type: "int"}, {Name: "bid", Type: "float"}, {Name: "ask", Type: "float"}, {Name: "nullable", Type: "float"}})
}
func (*liveEntrySource) FetchHistory(context.Context, *strat.DataSub, int64, int64) ([]*orm.DataRecord, error) {
	return nil, nil
}
func (s *liveEntrySource) SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink data.DataSink) error {
	defer close(s.joined)
	if s.subscribed != nil {
		if err := s.subscribed(subs); err != nil {
			return err
		}
	}
	now := time.Now().UnixMilli() - 1
	if s.closed {
		subs = append([]*strat.DataSub(nil), subs...)
		slices.Reverse(subs)
	}
	for _, sub := range subs {
		if err := sink.Emit(sub, []*orm.DataRecord{{Sid: sub.ExSymbol.ID, TimeMS: now, EndMS: now, Closed: s.closed, Values: map[string]any{"close": 100.0, "integer": int64(9), "bid": 99.0, "ask": 101.0, "nullable": nil}}}); err != nil {
			return err
		}
		s.emitted.Add(1)
	}
	for s.mapped != nil && s.mapped.Load() < s.expectedMapped {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	if s.failure != nil {
		return s.failure
	}
	s.cancel()
	<-ctx.Done()
	return ctx.Err()
}

func TestRegisteredFactorLiveBindingAssemblesCurrentProviderAndJoins(t *testing.T) {
	testFactorLiveBinding(t, nil, false)
}

func TestFactorLiveSourceFailureIsPreservedAndJoined(t *testing.T) {
	testFactorLiveBinding(t, errors.New("current source unavailable"), false)
}

func TestFactorLiveEntrySubscribesDistinctDataAndExecutionPools(t *testing.T) {
	testFactorLiveBinding(t, nil, true)
}

func TestFactorLiveEntryTenStrategiesShareOneProviderAndAccount(t *testing.T) {
	testFactorLiveBinding(t, nil, true, 10)
}

func TestFactorLiveAccountDeclarationsValidateBeforeMutation(t *testing.T) {
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var first runner.Config
	if err := json.Unmarshal(raw, &first); err != nil {
		t.Fatal(err)
	}
	first.Chunks, first.StrategyID = nil, "first"
	baseline, _ := json.Marshal(first)
	clone := func() runner.Config {
		var second runner.Config
		if err := json.Unmarshal(baseline, &second); err != nil {
			t.Fatal(err)
		}
		second.StrategyID = "second"
		return second
	}
	for name, mutate := range map[string]func(*runner.Config){
		"duplicate strategy": func(c *runner.Config) { c.StrategyID = first.StrategyID },
		"SID identity":       func(c *runner.Config) { c.Snapshot.SIDMap[1] = "different" },
		"instrument version": func(c *runner.Config) {
			unit := c.Execution.Instruments[1]
			unit.Version = "different"
			c.Execution.Instruments[1] = unit
		},
		"account risk": func(c *runner.Config) {
			c.Execution.MaxAccountMargin = c.Execution.MaxAccountMargin.Add(decimal.NewFromInt(1))
		},
	} {
		t.Run(name, func(t *testing.T) {
			second := clone()
			mutate(&second)
			if _, err := factorLiveAccountConfig([]runner.Config{first, second}); err == nil {
				t.Fatal("conflicting account declaration accepted")
			}
		})
	}
	second := clone()
	second.Snapshot.SIDMap[9] = "nine"
	second.Snapshot.Universe.Reference = append(second.Snapshot.Universe.Reference, 9)
	merged, err := factorLiveAccountConfig([]runner.Config{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if merged.Snapshot.SIDMap[9] != "nine" || !slices.Contains(merged.Snapshot.Universe.Reference, 9) {
		t.Fatal("account merge lost second strategy universe")
	}
	merged.Snapshot.SIDMap[1] = "changed"
	after, _ := json.Marshal(first)
	if !bytes.Equal(after, baseline) {
		t.Fatal("account validation mutated input strategy")
	}
}

func testFactorLiveBinding(t *testing.T, failure error, scoped bool, counts ...int) {
	t.Helper()
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			_, _ = io.Copy(io.Discard, conn)
		}
	}()
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err = json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Chunks = nil
	c.Mode = runner.Trade
	name := fmt.Sprintf("entry_live_%d", time.Now().UnixNano())
	c.Factor.Source = name
	c.Factor.TimeFrame = "event"
	c.Factor.Window = 2
	c.Plan, err = factor.New().Add("close", factor.Field(name, "close", "event")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	c.Prices = runner.PriceStream{Source: name, TimeFrame: "event", Field: "close"}
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	strategyCount := 1
	if len(counts) > 0 {
		strategyCount = counts[0]
	}
	c.Snapshot.Schemas = map[string]string{name: "schema-v1"}
	c.Snapshot.SourceVersions = map[string]string{name: "v1"}
	c.Execution.StorePath = filepath.Join(dir, "live.db")
	c.Execution.SenderLeaseDir = filepath.Join(dir, "leases")
	expectedRows := int32(3)
	var subscribed func([]*strat.DataSub) error
	if scoped {
		c.DecisionInterval = 1
		c.Snapshot.Universe = factor.Universe{Version: "scoped", Static: true, Tracked: []int32{1}, Investable: []int32{2}, Tradable: []int32{2}, Reference: []int32{3}, Evaluation: []int32{4}}
		c.Snapshot.SIDMap[4] = "asset-4"
		delete(c.Execution.Instruments, 3)
		plan, planErr := factor.New().Add("integer", factor.Field(name, "integer", "event")).Compile()
		if planErr != nil {
			t.Fatal(planErr)
		}
		c.Plan = plan
		c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"integer"}, Weights: map[string]float64{"integer": 1}}
		expectedRows = 3
		subscribed = func(subs []*strat.DataSub) error {
			seen := make(map[int32]bool)
			for _, sub := range subs {
				sid := sub.ExSymbol.ID
				seen[sid] = true
				if slices.Contains(sub.Fields, "integer") != (sid != 1) {
					return fmt.Errorf("SID %d missing DAG field: %v", sid, sub.Fields)
				}
				if slices.Contains(sub.Fields, "close") != (sid == 1 || sid == 2) {
					return fmt.Errorf("SID %d wrong execution projection: %v", sid, sub.Fields)
				}
			}
			if seen[4] {
				return fmt.Errorf("evaluation-only SID 4 acquired a required live subscription")
			}
			if len(seen) != 3 || !seen[1] || !seen[2] || !seen[3] {
				return fmt.Errorf("incomplete data pools: %v", seen)
			}
			return nil
		}
	}
	configs := make([]runner.Config, strategyCount)
	for i := range configs {
		configs[i] = c
		if strategyCount > 1 {
			configs[i].StrategyID = fmt.Sprintf("live-%d", i)
		}
	}
	var emitted, mapped atomic.Int32
	joined := make(chan struct{})
	if err = data.RegisterDataSourceFactory(name, func() data.DataSource {
		return &liveEntrySource{name: name, cancel: cancel, emitted: &emitted, joined: joined, failure: failure, subscribed: subscribed, closed: scoped, mapped: &mapped, expectedMapped: expectedRows * int32(strategyCount)}
	}); err != nil {
		t.Fatal(err)
	}
	exchange := &liveEntryExchange{trades: make(chan *banexg.MyTrade)}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: c.AccountID, SettlementDomain: "USD"}
	if scoped {
		exchange.cash = 10000
		store, err := execution.OpenStoreWithLeaseDir(c.Execution.StorePath, key, c.Execution.SenderLeaseDir)
		if err != nil {
			t.Fatal(err)
		}
		postings := []execution.CashPosting{{Amount: decimal.NewFromInt(-10000)}}
		for _, cfg := range configs {
			postings = append(postings, execution.CashPosting{Strategy: execution.StrategyID(cfg.StrategyID), Amount: decimal.NewFromInt(10000).Div(decimal.NewFromInt(int64(strategyCount)))})
		}
		for _, event := range []execution.CashEvent{
			{ID: "deposit", Kind: execution.ExternalCashChange, AccountDelta: decimal.NewFromInt(10000), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(10000)}}, AtMS: 100},
			{ID: "allocate", Kind: execution.CapitalTransfer, Postings: postings, AtMS: 100},
		} {
			if _, err := store.ApplyCashEvent(ctx, event); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	transport := &liveEntryTransport{key: key}
	binding := FactorLiveBinding{Account: key, Transport: transport, Symbols: map[int32]*orm.ExSymbol{}, VerifyFunding: func(context.Context, string) (string, error) { return "verified-no-funding-fixture", nil }, Record: func(s *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		if _, ok := s.Values["nullable"]; !ok {
			return factor.VersionRecord{}, errors.New("arbitrary field lost")
		}
		mapped.Add(1)
		return factor.VersionRecord{Series: *s, EventTime: s.EndMS, Revision: 1, AvailableAt: s.EndMS, IngestedAt: received, SourceVersion: "v1"}, nil
	}}
	for sid, symbol := range c.Snapshot.SIDMap {
		binding.Symbols[sid] = &orm.ExSymbol{ID: sid, Exchange: "entrytest", Market: "linear", Symbol: symbol}
	}
	if err = RegisterFactorLiveBinding(name, func(context.Context, banexg.BanExchange, *config.Snapshot, runner.Config) (FactorLiveBinding, error) {
		return binding, nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { factorLiveBindings.Lock(); delete(factorLiveBindings.items, name); factorLiveBindings.Unlock() })
	factory, err := factorLiveFactory(name)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Exchange: &config.ExchangeConfig{Name: "entrytest"}, MarketType: "linear", SpiderAddr: listener.Addr().String()}
	snapshot := config.NewSnapshotWithDirs(cfg, dir, "", nil)
	binding, err = factory(ctx, exchange, snapshot, c)
	if err != nil {
		t.Fatal(err)
	}
	sessionctx, sessioncancel := context.WithCancel(context.Background())
	session := &explicitEntrySession{process: runtime.NewProcess(), ctx: sessionctx, cancel: sessioncancel, exchange: exchange}
	var output bytes.Buffer
	err = session.runFactorsLive(ctx, snapshot, configs, binding, &output)
	session.close()
	expected := failure
	if expected == nil {
		expected = context.Canceled
	}
	if !errors.Is(err, expected) {
		t.Fatalf("live result: %v (caller=%v emitted=%d mapped=%d)", err, ctx.Err(), emitted.Load(), mapped.Load())
	}
	if transport.verified.Load() != 1 || exchange.snapshots.Load() != 1 || emitted.Load() != expectedRows || mapped.Load() != expectedRows*int32(strategyCount) {
		t.Fatalf("incomplete assembly: proof=%d snapshots=%d emitted=%d mapped=%d", transport.verified.Load(), exchange.snapshots.Load(), emitted.Load(), mapped.Load())
	}
	if scoped && strings.Count(output.String(), `"Kind":"decision"`) != strategyCount {
		t.Fatalf("inference/execution emission did not complete one decision: %s", output.String())
	}
	if scoped {
		merged, err := factorLiveAccountConfig(configs)
		if err != nil || !slices.Equal(merged.Snapshot.Universe.Evaluation, []int32{4}) || merged.Snapshot.SIDMap[4] != "asset-4" {
			t.Fatal("live assembly lost evaluation research metadata", err)
		}
		for _, cfg := range configs {
			if !slices.Equal(cfg.Snapshot.Universe.Evaluation, []int32{4}) || cfg.Snapshot.SIDMap[4] != "asset-4" {
				t.Fatal("live assembly mutated strategy evaluation metadata")
			}
		}
	}
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("source was not joined")
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("provider socket was not joined")
	}
}
