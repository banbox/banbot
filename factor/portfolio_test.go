package factor

import (
	"math"
	"reflect"
	"testing"
)

func portfolioFixture() (Frame, Universe, PortfolioSpec) {
	u := Universe{Version: "u1", Static: true}
	scores := make(map[int32]Numeric)
	for sid := int32(1); sid <= 24; sid++ {
		u.Investable = append(u.Investable, sid)
		u.Tradable = append(u.Tradable, sid)
		scores[sid] = Numeric{float64(sid), Valid}
	}
	f := Frame{SnapshotID: "snapshot", PlanHash: "factor-plan", DecisionTime: 1000, Values: map[string]map[int32]Numeric{"score": scores}}
	s := PortfolioSpec{StrategyID: "strategy", AccountID: "account", DecisionTime: 1000, ExecutableAt: 1001, ExpireAt: 2000, PlanSequence: 1, SnapshotID: f.SnapshotID, PlanHash: "strategy-plan", FactorPlanHash: f.PlanHash, UniverseVersion: u.Version, Budget: FrozenBudget{Version: "budget1", Currency: "USDT", NAV: 1000}, Mode: Full}
	return f, u, s
}
func TestTopBottomPortfolio24AssetsFrozenNAVAndTies(t *testing.T) {
	f, u, s := portfolioFixture()
	f.Values["score"][2] = Numeric{1, Valid}
	f.Values["score"][12] = Numeric{0, Null}
	p, d, err := TopBottomK(f, "score", u, s, 10)
	if err != nil || p == nil || len(d) != 0 {
		t.Fatalf("build: %v %v", d, err)
	}
	long, short := 0.0, 0.0
	for sid, w := range p.Targets() {
		if w > 0 {
			long += w
			if sid < 15 {
				t.Fatalf("unexpected long %d", sid)
			}
		} else {
			short += w
			if sid > 10 {
				t.Fatalf("unexpected short %d", sid)
			}
		}
	}
	if math.Abs(long-.5) > 1e-12 || math.Abs(short+.5) > 1e-12 || p.Notional(24) != 50 {
		t.Fatalf("NAV denominator/exposure: %g %g %g", long, short, p.Notional(24))
	}
	again, _, err := TopBottomK(f, "score", u, s, 10)
	if err != nil || again.ID() != p.ID() {
		t.Fatal("unstable deterministic plan")
	}
	u.Tradable = u.Tradable[:19]
	if p, d, err := TopBottomK(f, "score", u, s, 10); err != nil || p != nil || len(d) != 1 {
		t.Fatalf("untradable asset used: %v %v", d, err)
	}
}
func TestPortfolioSkipsConstantInsufficientAndRejectsIdentity(t *testing.T) {
	f, u, s := portfolioFixture()
	for sid := range f.Values["score"] {
		f.Values["score"][sid] = Numeric{2, Valid}
	}
	if p, d, err := TopBottomK(f, "score", u, s, 10); p != nil || err != nil || d[0].Code != "constant-scores" {
		t.Fatalf("constant decision: %+v %v", d, err)
	}
	f.Values["score"][1] = Numeric{math.NaN(), Valid}
	u.Investable = u.Investable[:20]
	if p, d, err := TopBottomK(f, "score", u, s, 10); p != nil || err != nil || d[0].Code != "insufficient-scores" {
		t.Fatalf("invalid scores admitted: %+v %v", d, err)
	}
	s.FactorPlanHash = "wrong"
	if _, _, err := TopBottomK(f, "score", u, s, 10); err == nil {
		t.Fatal("mismatched factor identity accepted")
	}
}
func TestImmutablePortfolioFullPatchOwnScopeAndCurrency(t *testing.T) {
	_, _, s := portfolioFixture()
	s.Diagnostics = []Diagnostic{{"test", "original"}}
	weights := map[int32]float64{1: 0.5, 2: -0.5}
	previous, err := NewTargetPortfolio(s, weights)
	if err != nil {
		t.Fatal(err)
	}
	weights[1] = 99
	s.Diagnostics[0].Detail = "mutated"
	got := previous.Targets()
	got[1] = 88
	copy := previous.Spec()
	copy.Diagnostics[0].Detail = "changed"
	if previous.Targets()[1] != .5 || previous.Spec().Diagnostics[0].Detail != "original" {
		t.Fatal("mutable portfolio leaked")
	}
	s = previous.Spec()
	s.PlanSequence = 2
	s.Mode = Patch
	patch, err := NewTargetPortfolio(s, map[int32]float64{3: .1})
	if err != nil {
		t.Fatal(err)
	}
	effective, err := patch.EffectiveTargets(previous)
	if err != nil || !reflect.DeepEqual(effective, map[int32]float64{1: .5, 2: -.5, 3: .1}) {
		t.Fatalf("patch own scope: %v %v", effective, err)
	}
	priorEffective, _ := NewTargetPortfolio(patch.Spec(), effective)
	s.PlanSequence = 3
	s.Mode = Full
	next, _ := NewTargetPortfolio(s, map[int32]float64{4: .5})
	effective, err = next.EffectiveTargets(priorEffective)
	if err != nil || !reflect.DeepEqual(effective, map[int32]float64{1: 0, 2: 0, 3: 0, 4: .5}) {
		t.Fatalf("full omitted patch positions: %v %v", effective, err)
	}
	s.StrategyID = "other"
	foreign, _ := NewTargetPortfolio(s, map[int32]float64{4: .5})
	if _, err := foreign.EffectiveTargets(previous); err == nil {
		t.Fatal("full could clear another strategy")
	}
	s.StrategyID = "strategy"
	s.Budget.Currency = "BTC"
	foreign, _ = NewTargetPortfolio(s, map[int32]float64{4: .5})
	if _, err := foreign.EffectiveTargets(previous); err == nil {
		t.Fatal("budget currency silently changed")
	}
	s.PlanSequence = math.MaxUint64
	if _, err := NewTargetPortfolio(s, nil); err == nil {
		t.Fatal("sequence could overflow transaction store")
	}
}
