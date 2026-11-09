package runner

import (
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

func TestParamsDefaultsValidationAndUnused(t *testing.T) {
	c := Config{Manifest: research.ManifestSpec{Parameters: map[string]float64{"len": 20, "frac": 1.5, "flag": 1, "windw": 9, "window": 5}}}
	p := c.Params()
	if p.Int("len", 1) != 20 || p.Int("absent", 7) != 7 || !p.Bool("flag", false) || p.Float("frac", 0) != 1.5 {
		t.Fatal("wrong values")
	}
	if p.Err() != nil {
		t.Fatalf("unexpected error %v", p.Err())
	}
	if got := p.Unused(); len(got) != 1 || got[0] != "windw" {
		t.Fatalf("typo not reported, unused=%v", got)
	}
	if p.Int("frac", 3) != 3 || p.Err() == nil {
		t.Fatal("fractional integer accepted")
	}
	if c.Manifest.Parameters["len"] != 20 || len(c.Manifest.Parameters) != 5 {
		t.Fatal("config changed by reading")
	}
	bad := (Config{Manifest: research.ManifestSpec{Parameters: map[string]float64{"b": 2, "n": math.NaN()}}}).Params()
	bad.Bool("b", false)
	if bad.Err() == nil {
		t.Fatal("non 0/1 bool accepted")
	}
	if (Config{}).Params().Int("x", 4) != 4 {
		t.Fatal("nil parameters must use defaults")
	}
}

func TestRegisterFactorBuildsEqualComboByDefault(t *testing.T) {
	err := RegisterFactor("test-simple-factor", func(c FactorContext) (FactorSpec, error) {
		w := c.Params.Int("window", 12)
		return FactorSpec{Outputs: map[string]*factor.Node{
			"b": factor.ZScore(factor.Return(c.Close(), w)),
			"a": factor.Rank(factor.Return(c.Close(), w)),
		}}, c.Params.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if RegisterFactor("test-simple-factor", func(FactorContext) (FactorSpec, error) { return FactorSpec{}, nil }) == nil {
		t.Fatal("duplicate accepted")
	}
	if RegisterFactor("", nil) == nil {
		t.Fatal("empty registration accepted")
	}
	c := Config{Definition: "test-simple-factor", Factor: research.MomentumVolConfig{TimeFrame: "1h"}}
	plan, combo, err := CompileDefinition(c)
	if err != nil {
		t.Fatal(err)
	}
	if plan.TimeFrame() != "1h" || combo.Method != research.Equal || strings.Join(combo.Columns, ",") != "a,b" {
		t.Fatalf("combo %+v tf %s", combo, plan.TimeFrame())
	}
	c.Manifest.Parameters = map[string]float64{"window": 1.5}
	if _, _, err = CompileDefinition(c); err == nil {
		t.Fatal("bad param error lost")
	}
	if err = RegisterFactor("test-simple-empty", func(FactorContext) (FactorSpec, error) { return FactorSpec{}, nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, err = CompileDefinition(Config{Definition: "test-simple-empty"}); err == nil {
		t.Fatal("empty outputs accepted")
	}
}

func pairsTestConfig(name string, params map[string]float64) Config {
	return Config{
		Definition: name,
		Factor:     research.MomentumVolConfig{TimeFrame: "1h"},
		Snapshot:   factor.SnapshotSpec{SIDMap: map[int32]string{1: "ETH", 2: "BTC", 3: "SOL"}},
		Manifest:   research.ManifestSpec{Parameters: params},
	}
}

func pairsTestSpec(seq uint64) factor.PortfolioSpec {
	return factor.PortfolioSpec{
		StrategyID: "s", AccountID: "a", DecisionTime: int64(seq) * 1000, ExecutableAt: int64(seq)*1000 + 1, ExpireAt: int64(seq)*1000 + 2,
		PlanSequence: seq, SnapshotID: "snap", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u",
		Budget: factor.FrozenBudget{Version: "b", Currency: "USDT", NAV: 1000},
	}
}

func logFrame(y, x float64) factor.Frame {
	return factor.Frame{Values: map[string]map[int32]factor.Numeric{pairsLogOutput: {
		1: {Value: math.Log(y), Validity: factor.Valid},
		2: {Value: math.Log(x), Validity: factor.Valid},
	}}}
}

func registerTestPairs(t *testing.T, name string) {
	t.Helper()
	err := RegisterPairs(name, func(p *Params) (PairsSpec, error) {
		return PairsSpec{
			Pairs:  []Pair{{Y: "ETH", X: "BTC"}},
			Window: p.Int("window", 30),
			Band:   statistics.BandConfig{Entry: p.Float("entry", 2), Exit: p.Float("exit", 0.5), Stop: p.Float("stop", 0)},
		}, p.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPairsBindsBuilderAndTradesSpreadReversion(t *testing.T) {
	registerTestPairs(t, "test-pairs")
	c := pairsTestConfig("test-pairs", nil)
	resolved, err := resolveDecisionPortfolio(c)
	if err != nil {
		t.Fatalf("one name in config must be enough: %v", err)
	}
	if resolved.Manifest.Portfolio.Builder != "test-pairs-pairs-v1" {
		t.Fatalf("builder not bound: %q", resolved.Manifest.Portfolio.Builder)
	}
	// An explicit builder name wins over the binding (it is unregistered here, so it must be reported).
	explicit := c
	explicit.Manifest.Portfolio.Builder = "other-builder-v1"
	if _, err := resolveDecisionPortfolio(explicit); err == nil || !strings.Contains(err.Error(), "other-builder-v1") {
		t.Fatalf("explicit builder not honoured: %v", err)
	}

	prepared, err := PreparePortfolioConfig(c)
	if err != nil || prepared.PortfolioBuilder == nil {
		t.Fatalf("prepare: %v", err)
	}
	build := prepared.PortfolioBuilder
	rng := rand.New(rand.NewSource(3))
	x, seq := 4.0, uint64(0)
	step := func(dislocation float64) (map[int32]float64, bool) {
		seq++
		x += rng.NormFloat64() * 0.01
		y := 1.1*x + 0.5 + rng.NormFloat64()*0.003 + dislocation
		p, _, err := build(logFrame(math.Exp(y), math.Exp(x)), factor.Universe{Version: "u", Tradable: []int32{1, 2, 3}}, pairsTestSpec(seq), research.PortfolioDefinition{})
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			return nil, false
		}
		return p.Targets(), true
	}
	for i := 0; i < 29; i++ {
		if _, ok := step(0); ok {
			t.Fatalf("emitted target during warmup at %d", i)
		}
	}
	for i := 0; i < 40; i++ {
		got, ok := step(0)
		if !ok {
			t.Fatal("no target after warmup")
		}
		if len(got) != 0 && i < 10 {
			t.Fatalf("traded without dislocation: %v", got)
		}
	}
	// Y rich versus the relation: short Y, long X, gross == 1 for the single pair.
	var entered map[int32]float64
	for i := 0; i < 3 && len(entered) == 0; i++ {
		entered, _ = step(0.2)
	}
	if entered[1] >= 0 || entered[2] <= 0 {
		t.Fatalf("expected short ETH / long BTC, got %v", entered)
	}
	if gross := math.Abs(entered[1]) + math.Abs(entered[2]); math.Abs(gross-1) > 1e-9 {
		t.Fatalf("gross %v", gross)
	}
	if hedge := -entered[2] / entered[1]; hedge < 0.5 || hedge > 2 {
		t.Fatalf("implausible beta-hedge ratio %v", hedge)
	}
	// Dislocation vanishes: the pair must close.
	var final map[int32]float64
	for i := 0; i < 40; i++ {
		final, _ = step(0)
		if len(final) == 0 {
			break
		}
	}
	if len(final) != 0 {
		t.Fatalf("position not closed after reversion: %v", final)
	}
}

func TestPairsRunsHavePrivateStateAndValidateInput(t *testing.T) {
	registerTestPairs(t, "test-pairs-state")
	c := pairsTestConfig("test-pairs-state", map[string]float64{"window": 5})
	a, ok, err := newPortfolioBuilder("test-pairs-state-pairs-v1", c)
	if err != nil || !ok {
		t.Fatal(err)
	}
	b, _, _ := newPortfolioBuilder("test-pairs-state-pairs-v1", c)
	uni := factor.Universe{Version: "u", Tradable: []int32{1, 2}}
	feed := func(build PortfolioBuilder, n int) []factor.Diagnostic {
		var d []factor.Diagnostic
		for i := 0; i < n; i++ {
			_, d, _ = build(logFrame(10+float64(i)*0.1, 20+float64(i)*0.3), uni, pairsTestSpec(uint64(i+1)), research.PortfolioDefinition{})
		}
		return d
	}
	if d := feed(a, 5); len(d) != 0 {
		t.Fatalf("a full window must clear warmup diagnostics: %v", d)
	}
	// b has seen nothing: still warming even though a has a full window.
	if d := feed(b, 3); len(d) == 0 || d[0].Code != "pair-warmup" {
		t.Fatalf("state shared across runs: %v", d)
	}
	// Unknown symbol is rejected when the run state is built, before any decision.
	missing := pairsTestConfig("test-pairs-state", map[string]float64{"window": 5})
	missing.Snapshot.SIDMap = map[int32]string{1: "ETH"}
	if _, _, err = newPortfolioBuilder("test-pairs-state-pairs-v1", missing); err == nil {
		t.Fatal("missing symbol accepted")
	}
	// Invalid thresholds are caught by the pure validator.
	if err = validatePairsForTest(pairsTestConfig("test-pairs-state", map[string]float64{"entry": 0.1, "exit": 1})); err == nil {
		t.Fatal("invalid band accepted")
	}
	if RegisterPairs("test-pairs-state", func(*Params) (PairsSpec, error) { return PairsSpec{}, nil }) == nil {
		t.Fatal("duplicate accepted")
	}
}

// validatePairsForTest runs only the pure builder validator.
func validatePairsForTest(c Config) error {
	_, _, err := validatePortfolioBuilder(boundDefinitionBuilder(c.Definition), c)
	return err
}

func TestRegisterFactorChecksParamErrorsItself(t *testing.T) {
	err := RegisterFactor("test-simple-unchecked-param", func(c FactorContext) (FactorSpec, error) {
		w := c.Params.Int("window", 12)
		return FactorSpec{Outputs: map[string]*factor.Node{"a": factor.Rank(factor.Return(c.Close(), w))}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	c := Config{Definition: "test-simple-unchecked-param", Factor: research.MomentumVolConfig{TimeFrame: "1h"}}
	c.Manifest.Parameters = map[string]float64{"window": 1.5}
	if _, _, err = CompileDefinition(c); err == nil {
		t.Fatal("unchecked param error silently ignored")
	}
}
