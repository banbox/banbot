package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
	"github.com/shopspring/decimal"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegisteredDefinitionBypassesPreset(t *testing.T) {
	name := fmt.Sprintf("registered-%s-%d", t.Name(), time.Now().UnixNano())
	err := RegisterDefinition(name, func(c Config) (*factor.Plan, research.ComboSpec, error) {
		p, err := factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
		return p, research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}, err
	})
	if err != nil {
		t.Fatal(err)
	}
	c := Config{Definition: name}
	if p, combo, err := CompileDefinition(c); err != nil || p == nil || combo.Method != research.Fixed {
		t.Fatalf("registered definition: %v", err)
	}
	if err := RegisterDefinition(name, nil); err == nil {
		t.Fatal("nil duplicate accepted")
	}
	c.Definition = "absent"
	if _, _, err := CompileDefinition(c); err == nil {
		t.Fatal("unknown definition accepted")
	}
}

func TestRegisteredPortfolioBuilderUsesActualManifest(t *testing.T) {
	c := archiveConfig(t, false)
	c.Mode = Research
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	name := fmt.Sprintf("single-long-%d", time.Now().UnixNano())
	if err := RegisterPortfolioBuilder(name, func(_ factor.Frame, u factor.Universe, spec factor.PortfolioSpec, _ research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		p, err := factor.NewTargetPortfolio(spec, map[int32]float64{u.Investable[0]: .25})
		u.Investable[0] = 999
		return p, nil, err
	}); err != nil {
		t.Fatal(err)
	}
	c.Manifest.Portfolio = research.PortfolioDefinition{Builder: name, Mode: factor.Full}
	output := &capture{targets: map[int64]map[int32]float64{}}
	result, err := Run(context.Background(), c, nil, output)
	if err != nil {
		t.Fatal(err)
	}
	if result.Manifest.Portfolio.Builder != name || result.Manifest.Portfolio.K != 0 || c.Snapshot.Universe.Investable[0] != 1 {
		t.Fatal("custom builder identity or immutable universe changed")
	}
	for _, weights := range output.targets {
		if len(weights) != 1 || weights[1] != .25 {
			t.Fatalf("custom builder targets %v", weights)
		}
	}
}

func TestRunManySharesActualComputationAndIsolatesConsumers(t *testing.T) {
	c := archiveConfig(t, false)
	c.Mode = Research
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	var calls atomic.Int64
	node := factor.Custom("shared-run-v1", []*factor.Node{factor.Field("kline", "close", "1h")}, func(v []factor.Numeric) factor.Numeric { calls.Add(1); return v[0] })
	var err error
	c.Plan, err = factor.New().Add("shared", node).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"shared"}, Weights: map[string]float64{"shared": 1}}
	expected, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	singleCalls := calls.Load()
	calls.Store(0)
	configs := make([]Config, 10)
	outputs := make([]Output, 10)
	for i := range configs {
		configs[i] = c
		configs[i].StrategyID = fmt.Sprint("s", i)
		outputs[i] = &capture{targets: map[int64]map[int32]float64{}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := RunMany(ctx, configs, make([]Sink, 10), outputs)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != singleCalls {
		t.Fatalf("ten drivers repeated computation: %d vs %d", calls.Load(), singleCalls)
	}
	for _, result := range results {
		if !reflect.DeepEqual(result.NodeUpdates, expected.NodeUpdates) {
			t.Fatal("shared node updates differ")
		}
	}
	for i := 1; i < 10; i++ {
		if !reflect.DeepEqual(outputs[0].(*capture).targets, outputs[i].(*capture).targets) {
			t.Fatal("consumer portfolios differ")
		}
	}
}

func TestCanceledRunArtifactPreservesFailure(t *testing.T) {
	c := Config{ArtifactPath: filepath.Join(t.TempDir(), "run.json")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Run(ctx, c, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel lost: %v", err)
	}
	raw, err := os.ReadFile(c.ArtifactPath)
	if err != nil {
		t.Fatal(err)
	}
	var artifact RunArtifact
	if err = json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Version != 1 || artifact.Status != "incomplete" || len(artifact.Errors) != 1 {
		t.Fatalf("failure artifact: %+v", artifact)
	}
	if err = WriteRunArtifact(c.ArtifactPath, RunArtifact{Version: 1, Status: "complete"}); err != nil {
		t.Fatal(err)
	}
}

type blockingDecisionOutput struct {
	capture
	entered, release chan struct{}
}

func (o *blockingDecisionOutput) Decision(f factor.Frame, p *factor.TargetPortfolio, d []factor.Diagnostic) error {
	close(o.entered)
	<-o.release
	return o.capture.Decision(f, p, d)
}
func TestLiveSlowWorkDoesNotHoldIntakeAndStopJoin(t *testing.T) {
	for _, stage := range []string{"compute", "output"} {
		t.Run(stage, func(t *testing.T) {
			c := archiveConfig(t, false)
			c.Manifest.Costs.FundingPolicy = "explicit-zero"
			c.Manifest.Portfolio.K = 1
			c.Snapshot.Universe = factor.Universe{Version: "slow", Static: true, Investable: []int32{1, 2}, Reference: []int32{1, 2}, Tradable: []int32{1, 2}}
			entered, release := make(chan struct{}), make(chan struct{})
			var once atomic.Bool
			node := factor.Field("kline", "close", "1h")
			if stage == "compute" {
				node = factor.Custom("slow-compute-v1", []*factor.Node{node}, func(v []factor.Numeric) factor.Numeric {
					if once.CompareAndSwap(false, true) {
						close(entered)
						<-release
					}
					return v[0]
				})
			}
			var err error
			c.Plan, err = factor.New().Add("close", node).Compile()
			if err != nil {
				t.Fatal(err)
			}
			c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
			var out Output
			if stage == "output" {
				out = &blockingDecisionOutput{capture: capture{targets: map[int64]map[int32]float64{}}, entered: entered, release: release}
			}
			const grid int64 = 3600000
			live, err := NewLive(c, &liveSink{}, func() int64 { return grid + 1 }, out)
			if err != nil {
				t.Fatal(err)
			}
			row := func(sid int32, revision uint64) factor.VersionRecord {
				return factor.VersionRecord{Series: orm.DataSeries{Sid: sid, Source: "kline", TimeFrame: "1h", Closed: true, EndMS: grid, Values: map[string]any{"close": float64(sid), "large": int64(1<<53 + 1), "null": nil}}, EventTime: grid, AvailableAt: grid, IngestedAt: grid, Revision: revision, SourceVersion: "v1"}
			}
			for _, sid := range []int32{1, 2} {
				if err = live.Observe(context.Background(), row(sid, 1)); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() { done <- live.Flush(context.Background(), grid) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("slow work did not start")
			}
			intake := make(chan error, 1)
			go func() { intake <- live.Observe(context.Background(), row(1, 2)) }()
			select {
			case err = <-intake:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("slow work blocked intake")
			}
			stopped := make(chan struct{})
			go func() { live.Stop(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("Stop blocked behind work")
			}
			short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			if err = live.Join(short); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Join should observe blocked callback: %v", err)
			}
			cancel()
			close(release)
			select {
			case err = <-done:
				if !errors.Is(err, factor.ErrRoundStale) && !errors.Is(err, context.Canceled) {
					t.Fatalf("stopped work published: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("worker did not stop")
			}
			if err = live.Join(context.Background()); err != nil {
				t.Fatal(err)
			}
			if live.pending != nil || live.lastDecision != 0 {
				t.Fatal("stopped generation published target")
			}
			if stage == "compute" && len(live.engine.session.Updates()) != 0 {
				t.Fatal("stopped compute advanced shared owner")
			}
		})
	}
}

func TestLiveCompatibleGroupSharesActualNodes(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.Snapshot.Universe = factor.Universe{Version: "shared-live", Static: true, Investable: []int32{1, 2}, Reference: []int32{1, 2}, Tradable: []int32{1, 2}}
	c.ComputationGroup = NewComputationGroup()
	c.ComputationContext = ComputationContext{DataNamespace: "fixture", ClockDomain: "fixture-clock", SamplingIdentity: "closed-hour"}
	var calls atomic.Int64
	node := factor.Custom("actual-live-v1", []*factor.Node{factor.Field("kline", "close", "1h")}, func(v []factor.Numeric) factor.Numeric { calls.Add(1); return v[0] })
	var err error
	c.Plan, err = factor.New().Add("close", node).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	const grid int64 = 3600000
	lives := make([]*Live, 10)
	for i := range lives {
		c.StrategyID = fmt.Sprint("live", i)
		lives[i], err = NewLive(c, &liveSink{}, func() int64 { return grid + 1 }, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer lives[i].Stop()
	}
	for _, live := range lives {
		for _, sid := range []int32{1, 2} {
			r := factor.VersionRecord{Series: orm.DataSeries{Sid: sid, Source: "kline", TimeFrame: "1h", Closed: true, EndMS: grid, Values: map[string]any{"close": float64(sid)}}, EventTime: grid, AvailableAt: grid, IngestedAt: grid, Revision: 1, SourceVersion: "v1"}
			if err = live.Observe(context.Background(), r); err != nil {
				t.Fatal(err)
			}
		}
		if err = live.Flush(context.Background(), grid); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("ten live consumers repeated nodes: %d", calls.Load())
	}
	lives[0].pending.Targets()[1] = 999
	if lives[1].pending.Targets()[1] == 999 {
		t.Fatal("targets shared across accounts")
	}
}

func TestLiveNewGenerationDiscardsSlowOlderCompute(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.Snapshot.Universe = factor.Universe{Version: "generation", Static: true, Investable: []int32{1, 2}, Reference: []int32{1, 2}, Tradable: []int32{1, 2}}
	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	node := factor.Custom("generation-v1", []*factor.Node{factor.Field("kline", "close", "1h")}, func(v []factor.Numeric) factor.Numeric {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return v[0]
	})
	var err error
	c.Plan, err = factor.New().Add("close", node).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	const grid int64 = 3600000
	var clock atomic.Int64
	clock.Store(grid + 1)
	live, err := NewLive(c, &liveSink{}, clock.Load, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Stop()
	observe := func(at int64) {
		for _, sid := range []int32{1, 2} {
			r := factor.VersionRecord{Series: orm.DataSeries{Sid: sid, Source: "kline", TimeFrame: "1h", Closed: true, EndMS: at, Values: map[string]any{"close": float64(sid)}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"}
			if err := live.Observe(context.Background(), r); err != nil {
				t.Fatal(err)
			}
		}
	}
	observe(grid)
	older := make(chan error, 1)
	go func() { older <- live.Flush(context.Background(), grid) }()
	<-entered
	clock.Store(grid + 2)
	observe(grid + 1)
	newer := make(chan error, 1)
	go func() { newer <- live.Flush(context.Background(), grid+1) }()
	// prepareRound supersedes before waiting for serialized computation.
	deadline := time.Now().Add(time.Second)
	for {
		live.mu.Lock()
		generation := live.generation
		live.mu.Unlock()
		if generation >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new round did not supersede slow worker")
		}
		runtime.Gosched()
	}
	close(release)
	if err = <-older; !errors.Is(err, factor.ErrRoundStale) {
		t.Fatalf("old generation published: %v", err)
	}
	if err = <-newer; err != nil {
		t.Fatal(err)
	}
	if live.lastDecision != grid+1 || live.pending == nil {
		t.Fatal("newest generation not published")
	}
}

func TestExplicitPlanDoesNotRequirePreset(t *testing.T) {
	c := archiveConfig(t, false)
	c.Factor = research.MomentumVolConfig{}
	var err error
	c.Plan, err = factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	if _, _, err := compileDecision(c); err != nil {
		t.Fatal(err)
	}
}

func TestActualPortfolioAllocation(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Portfolio.LongNotional, c.Manifest.Portfolio.ShortNotional = .8, .2
	output := &capture{targets: map[int64]map[int32]float64{}}
	result, err := Run(context.Background(), c, nil, output)
	if err != nil {
		t.Fatal(err)
	}
	if result.Manifest.Portfolio.LongNotional != .8 {
		t.Fatal("manifest lost actual allocation")
	}
	for _, weights := range output.targets {
		var long, short float64
		for _, w := range weights {
			if w > 0 {
				long += w
			} else {
				short -= w
			}
		}
		if long < .799999 || long > .800001 || short < .199999 || short > .200001 {
			t.Fatalf("actual tails %v/%v", long, short)
		}
	}
}

func TestEventsDefaultMemoryMatchesExplicitLedger(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Mode = Events
	durable, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Execution.StorePath = ""
	c.Execution.SenderLeaseDir = ""
	c.ArtifactPath = filepath.Join(t.TempDir(), "run.json")
	memory, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if memory.TargetsAccepted == 0 || memory.TargetsAccepted != memory.Executions || memory.Fills == 0 {
		t.Fatal("accepted targets/fills not separated")
	}
	if !reflect.DeepEqual(memory.Book, durable.Book) || memory.Fills != durable.Fills || memory.TargetsAccepted != durable.TargetsAccepted {
		t.Fatalf("memory replay differs: %+v vs %+v", memory.Book, durable.Book)
	}
	sink, cleanup, err := NewPaperSink(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if sink.Account.Service().Store().Durability() != execution.MemoryOnly {
		t.Fatal("default paper sink created durable storage")
	}
}

func TestRunManyUnequalArchivesAndEarlyFailureDoNotHang(t *testing.T) {
	for _, kind := range []string{"shorter", "missing", "failed"} {
		t.Run(kind, func(t *testing.T) {
			c := archiveConfig(t, false)
			c.Mode = Research
			c.Manifest.Costs.FundingPolicy = "explicit-zero"
			other := c
			other.StrategyID = "other"
			switch kind {
			case "shorter":
				other.Chunks = append([]Chunk(nil), c.Chunks...)
				other.Chunks[0].To = 10 * c.DecisionInterval
			case "missing":
				store, err := factor.OpenVersionStore(c.Chunks[0].Path, c.MaxRecords)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := store.Records()
				if err != nil {
					t.Fatal(err)
				}
				missing, _ := factor.NewVersionStore(c.MaxRecords)
				for _, row := range rows {
					if row.Series.Sid == 1 && row.Series.Source == "kline" {
						continue
					}
					if err = missing.Put(row); err != nil {
						t.Fatal(err)
					}
				}
				path := filepath.Join(t.TempDir(), "missing.gob")
				if _, err = missing.Export(path); err != nil {
					t.Fatal(err)
				}
				other.Chunks = append([]Chunk(nil), c.Chunks...)
				other.Chunks[0].Path = path
			case "failed":
				other.MaxRecords = 1
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			results, err := RunMany(ctx, []Config{c, other}, make([]Sink, 2), make([]Output, 2))
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("consumer rendezvous hung: %v", err)
			}
			if kind == "failed" {
				if err == nil {
					t.Fatal("early driver failure lost")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(results) != 2 {
				t.Fatal("lost driver")
			}
			if kind == "shorter" && results[1].Decisions != 10 {
				t.Fatalf("shorter driver decisions %d", results[1].Decisions)
			}
			if kind == "missing" && (results[0].Decisions == 0 || results[1].Incomplete == 0) {
				t.Fatal("missing archive silently shared the complete frame")
			}
		})
	}
}

func TestPaperGroupSharesOwnerCapitalAndReplayTimeline(t *testing.T) {
	first := paperConfig(t, archiveConfig(t, false))
	first.Mode = Events
	first.Manifest.Costs.FundingPolicy = "explicit-zero"
	first.Execution.StorePath = ""
	first.Execution.SenderLeaseDir = ""
	first.InitialNAV = 4000
	first.AccountInitialNAV = 10000
	second := first
	second.StrategyID = "second"
	second.InitialNAV = 4000
	configs := []Config{first, second}
	sinks, cleanup, err := NewPaperSinks(context.Background(), configs)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	a, b := sinks[0].(*AccountSink), sinks[1].(*AccountSink)
	if a.Account.Service() != b.Account.Service() || a.Paper != b.Paper {
		t.Fatal("strategies did not share one simulated account owner")
	}
	snapshot, err := a.Account.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.UnassignedCash.Equal(decimal.NewFromInt(2000)) {
		t.Fatalf("unallocated cash changed %s", snapshot.UnassignedCash)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := RunMany(ctx, configs, sinks, make([]Output, 2))
	if err != nil {
		t.Fatal(err)
	}
	if results[0].TargetsAccepted == 0 || results[1].TargetsAccepted == 0 {
		t.Fatal("shared account runners did not admit targets")
	}
}
