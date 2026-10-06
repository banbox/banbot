package runner

import (
	"context"
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
	"path/filepath"
	"reflect"
	"testing"
)

type bookPolicySink struct {
	book    *backtest.Book
	targets []*factor.PortfolioTarget
}

func (s *bookPolicySink) ProcessSnapshot(context.Context, *factor.TargetPortfolio, map[int32]backtest.Quote, int64) error {
	return errors.New("unexpected legacy target")
}
func (s *bookPolicySink) ObserveQuote(_ context.Context, sid int32, q backtest.Quote, now int64) error {
	return s.book.Mark(sid, q, now)
}
func (s *bookPolicySink) StrategyNAV(context.Context, int64) (float64, error) {
	return s.book.State().NAV, nil
}
func (s *bookPolicySink) StrategyState(context.Context, int64) (backtest.State, error) {
	return s.book.State(), nil
}
func (s *bookPolicySink) PolicyEvidence(_ context.Context, now int64) (factor.PortfolioEvidence, error) {
	return s.book.PolicyEvidence(now)
}
func (s *bookPolicySink) AcceptProposal(_ context.Context, p factor.PortfolioProposal, v, cursor uint64, q map[int32]backtest.Quote, now int64) (execution.PolicyReceipt, error) {
	receipt, err := s.book.AcceptProposal(p, v, cursor, q, now, 0, 0)
	if receipt.Accepted && p.Target != nil {
		s.targets = append(s.targets, p.Target)
	}
	return receipt, err
}

func TestLivePolicyExpiryWithIncompleteScoresAndSameGridRetry(t *testing.T) {
	const hour = int64(3600000)
	c := archiveConfig(t, false)
	c.Manifest.Labels = nil
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Plan, _ = factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	c.Manifest.Portfolio.K = 1
	c.Manifest.Portfolio.Policy = "lifecycle-v1"
	c.Manifest.Portfolio.LongNotional = 1
	c.Manifest.Portfolio.ShortNotional = 0
	c.Manifest.Portfolio.Rebalance = &factor.RebalanceConfig{EveryBars: 8}
	c.Manifest.Portfolio.Holding = &factor.HoldingConfig{MaxBars: 1}
	c.Manifest.Portfolio.Transition = &factor.TransitionConfig{Mode: "linear-exit", Basis: "quantity", ExitSteps: 8}
	book, _ := backtest.NewBook(10000)
	sink := &bookPolicySink{book: book}
	now := hour
	live, err := NewLive(c, sink, func() int64 { return now }, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		live.Stop()
		if err := live.Join(context.Background()); err != nil {
			t.Error(err)
		}
	})
	observe := func(sid int32, source, tf, field string, at int64) {
		t.Helper()
		row := factor.VersionRecord{Series: orm.DataSeries{Source: source, TimeFrame: tf, Sid: sid, TimeMS: at, EndMS: at, Closed: true, Values: map[string]any{field: 100 + float64(sid)}}, EventTime: at, AvailableAt: now, IngestedAt: now, Revision: 1, SourceVersion: "v1"}
		if err := live.Observe(context.Background(), row); err != nil {
			t.Fatal(err)
		}
	}
	for _, sid := range c.Snapshot.Universe.Investable {
		observe(sid, "kline", "1h", "close", hour)
	}
	if err := live.Flush(context.Background(), hour); err != nil {
		t.Fatal(err)
	}
	now = hour + 1
	for _, sid := range c.Snapshot.Universe.Investable {
		observe(sid, "tick", "event", "price", now)
	}
	if len(sink.targets) != 1 {
		t.Fatalf("initial targets=%d", len(sink.targets))
	}
	// The first eligible base grid after one actual filled hour is hour 3.
	now = 3 * hour
	if err := live.Flush(context.Background(), 3*hour); err != nil {
		t.Fatal(err)
	}
	now++
	for _, sid := range c.Snapshot.Universe.Investable {
		observe(sid, "tick", "event", "price", now)
	}
	if len(sink.targets) != 2 {
		t.Fatalf("expiry held behind score barrier/schedule: %d", len(sink.targets))
	}
	if len(sink.targets[1].Allocations()) != 1 || sink.targets[1].Allocations()[24].Value != "0" {
		t.Fatalf("maximum duration did not hard close: %v", sink.targets[1].Allocations())
	}
	if book.State().Quantities[24] != 0 {
		t.Fatal("expiry was only a proposal")
	}
	// Missing scores do not consume the ordinary ranking decision on this grid.
	for _, sid := range c.Snapshot.Universe.Investable {
		observe(sid, "kline", "1h", "close", 3*hour)
	}
	if err := live.Flush(context.Background(), 3*hour); err != nil {
		t.Fatal(err)
	}
}

func TestPortfolioParameterScanBoundAndIsolation(t *testing.T) {
	c := archiveConfig(t, false)
	direct := c.Manifest.Portfolio
	direct.Policy = "lifecycle-v1"
	cohort := direct
	cohort.Transition = &factor.TransitionConfig{Mode: "cohort", PeriodBars: 4}
	trials := []PortfolioTrial{{Name: "direct", Portfolio: direct}, {Name: "cohort", Portfolio: cohort}}
	if _, err := ScanPortfolioTrials(context.Background(), c, trials, 1); err == nil {
		t.Fatal("unbounded scan accepted")
	}
	results, err := ScanPortfolioTrials(context.Background(), c, trials, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Result.StrategyHash == results[1].Result.StrategyHash || results[0].Result.TargetsAccepted == 0 || results[1].Result.TargetsAccepted == 0 {
		t.Fatalf("trial state/identity not isolated: %+v", results)
	}
}

func TestSharedAccountLifecycleStrategiesRemainIndependent(t *testing.T) {
	base := paperConfig(t, archiveConfig(t, false))
	base.Mode = Events
	base.Manifest.Portfolio.Policy = "lifecycle-v1"
	base.InitialNAV = 5000
	base.AccountInitialNAV = 10000
	one := base
	two := base
	one.StrategyID = "rotation-a"
	two.StrategyID = "rotation-b"
	one.Manifest.Portfolio.Transition = &factor.TransitionConfig{Mode: "linear-exit", ExitSteps: 4, Basis: "quantity"}
	two.Manifest.Portfolio.Transition = &factor.TransitionConfig{Mode: "cohort", PeriodBars: 4}
	configs := []Config{one, two}
	sinks, done, err := NewPaperSinks(context.Background(), configs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := done(); err != nil {
			t.Error(err)
		}
	})
	results, err := RunMany(context.Background(), configs, sinks, make([]Output, 2))
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range results {
		if result.TargetsAccepted == 0 || result.Account == nil || result.Book.NAV <= 0 {
			t.Fatalf("strategy %d not executed: %+v", i, result)
		}
		state, evidenceErr := sinks[i].(PolicySink).PolicyEvidence(context.Background(), 28*3600000+2)
		if evidenceErr != nil {
			t.Fatal(evidenceErr)
		}
		life, decodeErr := factor.DecodeLifecycleState(state.State)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if life.StrategyID != configs[i].StrategyID {
			t.Fatalf("policy state crossed strategy boundary: %+v", life)
		}
	}
}

type allocationCapture struct {
	capture
	allocations []map[int32]factor.Allocation
}

func (o *allocationCapture) DecisionAllocation(factor.Frame, *factor.PortfolioTarget, []factor.Diagnostic) error {
	return nil
}
func (o *allocationCapture) AllocationAccepted(target *factor.PortfolioTarget, _ backtest.State, _ int64) error {
	o.allocations = append(o.allocations, target.Allocations())
	return nil
}

func TestLifecycleReplayLiveParity(t *testing.T) {
	const hour = int64(3600000)
	c := archiveConfig(t, false)
	c.Manifest.Labels = nil
	c.Manifest.Costs = research.CostSpec{FundingPolicy: "explicit-zero"}
	c.FundingSource = ""
	c.Plan, _ = factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	c.Manifest.Portfolio.Policy = "lifecycle-v1"
	c.Manifest.Portfolio.LongNotional, c.Manifest.Portfolio.ShortNotional = 1, 0
	c.Manifest.Portfolio.K = 1
	c.Manifest.Portfolio.Holding = &factor.HoldingConfig{MinBars: 1, MaxBars: 3}
	c.Manifest.Portfolio.Transition = &factor.TransitionConfig{Mode: "linear-exit", Basis: "quantity", ExitSteps: 2}
	store, _ := factor.NewVersionStore(1000)
	var rounds [][]factor.VersionRecord
	for grid := int64(1); grid <= 8; grid++ {
		var records []factor.VersionRecord
		for _, source := range []string{"kline", "tick"} {
			for _, sid := range c.Snapshot.Universe.Investable {
				at, tf, field, value := grid*hour, "1h", "close", float64(sid)
				if grid%4 >= 2 {
					value = -value
				}
				if source == "tick" {
					at, tf, field, value = at+1, "event", "price", 100
				}
				record := factor.VersionRecord{Series: orm.DataSeries{Source: source, TimeFrame: tf, Sid: sid, TimeMS: at, EndMS: at, Closed: true, Values: map[string]any{field: value}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"}
				if err := store.Put(record); err != nil {
					t.Fatal(err)
				}
				records = append(records, record)
			}
		}
		rounds = append(rounds, records)
	}
	path := filepath.Join(t.TempDir(), "parity.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	c.Chunks = []Chunk{{Path: path, From: hour, To: 8*hour + 1}}
	replay := &allocationCapture{}
	if _, err := Run(context.Background(), c, nil, replay); err != nil {
		t.Fatal(err)
	}
	book, _ := backtest.NewBook(c.InitialNAV)
	sink := &bookPolicySink{book: book}
	now := hour
	live, err := NewLive(c, sink, func() int64 { return now }, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { live.Stop(); _ = live.Join(context.Background()) })
	for _, records := range rounds {
		for _, record := range records {
			if record.Series.Source == "tick" && now < record.EventTime {
				if err := live.Flush(context.Background(), now); err != nil {
					t.Fatal(err)
				}
			}
			now = record.EventTime
			if err := live.Observe(context.Background(), record); err != nil {
				t.Fatal(err)
			}
		}
	}
	var actual []map[int32]factor.Allocation
	for _, target := range sink.targets {
		actual = append(actual, target.Allocations())
	}
	if len(actual) < 4 || !reflect.DeepEqual(replay.allocations, actual) {
		t.Fatalf("replay/live allocation mismatch:\nreplay=%v\nlive=%v", replay.allocations, actual)
	}
}

func TestNewLiveOwnsLifecycleConfiguration(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Manifest.Portfolio.Policy = "lifecycle-v1"
	c.Manifest.Portfolio.Allocation = &factor.AllocationConfig{Method: "equal"}
	book, _ := backtest.NewBook(c.InitialNAV)
	live, err := NewLive(c, &bookPolicySink{book: book}, func() int64 { return 3600000 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { live.Stop(); _ = live.Join(context.Background()) })
	c.Manifest.Portfolio.Allocation.Method = "score"
	delete(c.Execution.Instruments, 1)
	if live.c.Manifest.Portfolio.Allocation.Method != "equal" || len(live.c.Execution.Instruments) != 24 {
		t.Fatal("live configuration aliases caller-owned containers")
	}
}
