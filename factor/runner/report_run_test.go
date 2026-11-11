package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
	"github.com/banbox/banbot/orm"
)

// pairsRunConfig is a weights-mode run of a RegisterPairs strategy on a synthetic cointegrated pair
// (ETH = 1.1 x BTC + mean-reverting noise), with a label so the evaluation Summary exists.
var pairsRunRegistered sync.Map

func pairsRunConfig(t *testing.T, name string, bars int64) Config {
	t.Helper()
	if _, loaded := pairsRunRegistered.LoadOrStore(name, true); !loaded {
		err := RegisterPairs(name, func(p *Params) (PairsSpec, error) {
			return PairsSpec{Pairs: []Pair{{Y: "ETH", X: "BTC"}}, Window: 20, Band: statistics.BandConfig{Entry: 1.5, Exit: .3, Stop: 0, MaxHold: 40}}, p.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	c := pitConfig(t, map[int32]int64{1: 1, 2: 1}, 1, 0)
	c.Plan, c.Combo, c.Definition = nil, research.ComboSpec{}, name
	c.Factor = research.DefaultMomentumVolConfig()
	c.Factor.TimeFrame = "1h"
	c.Snapshot.Universe = factor.Universe{Version: "two", Static: true, Investable: []int32{1, 2}, Tradable: []int32{1, 2}, Evaluation: []int32{1, 2}, Tracked: []int32{1, 2}}
	c.Snapshot.SIDMap = map[int32]string{1: "ETH", 2: "BTC"}
	c.MaxRecords, c.MaxPending, c.DecisionInterval, c.ExpiryMS = 3000, 10, 3600000, 60000
	const hour int64 = 3600000
	c.Manifest.Labels = []research.LabelSpec{{Name: "1h", Kind: research.ExecutableReturn, Horizon: hour, PeriodsPerYear: 8760, Overlapping: true}}
	store, err := factor.NewVersionStore(3000)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(11))
	lx, u := math.Log(100), 0.0
	for bar := int64(1); bar <= bars; bar++ {
		lx += rng.NormFloat64() * 0.01
		u = 0.8*u + rng.NormFloat64()*0.02
		for sid, p := range map[int32]float64{1: math.Exp(1.1*lx + u), 2: math.Exp(lx)} {
			at := bar * hour
			must := func(err error) {
				if err != nil {
					t.Fatal(err)
				}
			}
			must(store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{"close": p}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"}))
			must(store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: at + 1, EndMS: at + 1, Closed: true, Values: map[string]any{"price": p}}, EventTime: at + 1, AvailableAt: at + 1, IngestedAt: at + 1, Revision: 1, SourceVersion: "v1"}))
		}
	}
	path := filepath.Join(t.TempDir(), "pairs.gob")
	if _, err = store.Export(path); err != nil {
		t.Fatal(err)
	}
	c.Chunks = []Chunk{{Path: path, From: hour, To: bars*hour + 2}}
	return c
}

func TestRunReportsPairsAndAttribution(t *testing.T) {
	c := pairsRunConfig(t, "test-pairs-report", 300)
	res, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pairs) != 1 || res.Pairs[0].Y != "ETH" || res.Pairs[0].X != "BTC" || res.Pairs[0].Trades == 0 || res.Pairs[0].Bars == 0 {
		t.Fatalf("pairs: %+v", res.Pairs)
	}
	a := res.Attribution
	if a == nil || a.ExposureSamples == 0 || len(a.TopSymbols) != 2 || a.Fees <= 0 {
		t.Fatalf("attribution: %+v", a)
	}
	if math.Abs(a.Residual) > 1e-9*a.InitialNAV {
		t.Fatalf("attribution residual %v", a.Residual)
	}
	if math.Abs(a.FinalNAV-res.Book.NAV) > 1e-9 || a.LongPnL == 0 || a.ShortPnL == 0 || a.AvgLongExposure <= 0 || a.AvgShortExposure <= 0 {
		t.Fatalf("legs: %+v book NAV %v", a, res.Book.NAV)
	}
	// Pairs reconcile: NAV return = gross pair PnL + residual (costs and execution approximation).
	if math.Abs(res.PairsNAVReturn-res.PairsGrossPnL-res.PairsResidual) > 1e-12 {
		t.Fatalf("pairs reconcile: %+v", res)
	}
	costs := (a.Fees + a.Slippage + a.Funding + a.PITCost) / a.InitialNAV
	t.Logf("pairs gross=%.5f nav=%.5f residual=%.5f costs=%.5f episodes=%d", res.PairsGrossPnL, res.PairsNAVReturn, res.PairsResidual, costs, res.Pairs[0].EpisodeCount)
	if res.PairsResidual > 0.01 || res.PairsResidual < -costs-0.05 {
		t.Fatalf("residual implausible vs costs: residual=%v costs=%v", res.PairsResidual, costs)
	}
	// round trip through the run.json encoding
	body, err := json.Marshal(RunArtifact{Result: res})
	if err != nil {
		t.Fatal(err)
	}
	var back struct{ Result Result }
	if err = json.Unmarshal(body, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Result.Pairs) != 1 || back.Result.Attribution == nil || back.Result.Pairs[0].EpisodeCount != res.Pairs[0].EpisodeCount {
		t.Fatal("round trip lost fields")
	}
}

// Research panel rows can be switched off without touching events, Summary or any ledger number.
func TestPanelNoneKeepsEventsSummaryAndLedger(t *testing.T) {
	run := func(noPanel bool) (Result, []string) {
		c := pairsRunConfig(t, "test-pairs-panel", 120)
		var buf bytes.Buffer
		res, err := Run(context.Background(), c, nil, &JSONOutput{Writer: &buf, SIDs: c.Snapshot.Universe.Evaluation, NoPanel: noPanel})
		if err != nil {
			t.Fatal(err)
		}
		var events []string
		panel := 0
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			switch {
			case strings.HasPrefix(line, `{"Kind"`):
				events = append(events, line)
			case strings.HasPrefix(line, `{"SnapshotID"`):
				panel++
			default:
				t.Fatalf("unexpected line %.60s", line)
			}
		}
		if noPanel && panel != 0 || !noPanel && panel == 0 {
			t.Fatalf("noPanel=%v panel rows=%d", noPanel, panel)
		}
		return res, events
	}
	full, fullEvents := run(false)
	none, noneEvents := run(true)
	if len(full.Summary) == 0 {
		t.Fatal("no Summary to compare")
	}
	a, _ := json.Marshal(full.Summary)
	b, _ := json.Marshal(none.Summary)
	if !bytes.Equal(a, b) {
		t.Fatal("Summary changed with panel=none")
	}
	if math.Float64bits(full.Book.NAV) != math.Float64bits(none.Book.NAV) || full.Decisions != none.Decisions {
		t.Fatal("ledger changed")
	}
	if len(fullEvents) != len(noneEvents) {
		t.Fatalf("event lines %d vs %d", len(fullEvents), len(noneEvents))
	}
}

// Production entry points call PreparePortfolioConfig before Run: Pairs must still be reported, and running
// the same prepared Config again must start the result bookkeeping from zero.
func TestPreparedConfigReportsPairsAndRerunIsNotCumulative(t *testing.T) {
	c := pairsRunConfig(t, "test-pairs-prepared", 200)
	prepared, err := PreparePortfolioConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Run(context.Background(), prepared, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Pairs) != 1 || first.Pairs[0].Bars == 0 || first.Attribution == nil {
		t.Fatalf("prepared config lost pairs: %+v", first.Pairs)
	}
	second, err := Run(context.Background(), prepared, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// the builder keeps its trading state across runs (a prepared Config is meant for one run), but the
	// bookkeeping is per run: bars never accumulate across runs
	if len(second.Pairs) != 1 || second.Pairs[0].Bars > second.Decisions || second.Pairs[0].InMarketBars > second.Pairs[0].Bars {
		t.Fatalf("second run not clean: bars=%d decisions=%d", second.Pairs[0].Bars, second.Decisions)
	}
	// two independently prepared runs are identical
	other, err := PreparePortfolioConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Run(context.Background(), other, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(first.Pairs)
	b, _ := json.Marshal(again.Pairs)
	if !bytes.Equal(a, b) || first.PairsGrossPnL != again.PairsGrossPnL {
		t.Fatal("identically prepared runs differ")
	}
}
