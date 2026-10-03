package runner

import (
	"fmt"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
)

// decisionEngine owns the shared CS definition and incremental computation.
// Drivers retain observation, publication, clocks, research maturity and sinks:
// replay evaluates frozen snapshots directly; live commits through RoundBarrier.
type decisionEngine struct {
	plan                  *factor.Plan
	combo                 research.ComboSpec
	manifest              *research.Manifest
	session               *factor.Session
	strategyID, accountID string
	currency              string
	portfolio             research.PortfolioDefinition
	builder               PortfolioBuilder
	shared                *sharedComputation
}

func compileDecision(c Config) (*factor.Plan, research.ComboSpec, error) {
	var plan *factor.Plan
	var combo research.ComboSpec
	var err error
	if c.Plan != nil {
		plan = c.Plan
	} else {
		builder, ok := definitionBuilder(c.Definition)
		if !ok {
			return nil, combo, fmt.Errorf("runner: unregistered definition %q", c.Definition)
		}
		plan, combo, err = builder(c)
		if err != nil {
			return nil, combo, err
		}
	}
	if c.Combo.Method != "" {
		combo = c.Combo
	}
	if plan == nil || combo.Method == "" {
		return nil, combo, fmt.Errorf("runner: definition requires plan and combiner")
	}
	return plan, combo, nil
}

// Run-specific lineage and latency assumptions are supplied by each driver.
func newDecisionEngine(c Config, plan *factor.Plan, combo research.ComboSpec) (*decisionEngine, error) {
	var err error
	c, err = resolveDecisionPortfolio(c)
	if err != nil {
		return nil, err
	}
	c.Manifest.Combo = combo
	c.Manifest.FactorPlanHash = plan.Hash()
	c.Manifest.UniverseVersion = c.Snapshot.Universe.Version
	c.Manifest.VisibilityPolicy = c.Snapshot.VisibilityPolicy
	c.Manifest.StaticUniverse = c.Snapshot.Universe.Static
	manifest, err := research.BuildManifest(c.Manifest)
	if err != nil {
		return nil, err
	}
	session, err := factor.NewSession(plan)
	if err != nil {
		return nil, err
	}
	definition := manifest.Spec()
	engine := &decisionEngine{plan: plan, combo: definition.Combo, manifest: manifest, session: session, strategyID: c.StrategyID, accountID: c.AccountID, currency: definition.Currency, portfolio: definition.Portfolio, builder: c.PortfolioBuilder}
	if c.ComputationGroup != nil {
		engine.shared, err = c.ComputationGroup.acquire(c, plan)
		if err != nil {
			return nil, err
		}
		engine.session = engine.shared.session
	}
	return engine, nil
}

// combine accepts only already matured, visible IC history. Labels themselves
// never enter inference; the archival driver owns their maturity queue.
func (e *decisionEngine) combine(frame factor.Frame, universe factor.Universe, history *research.ICHistory) (factor.Frame, []factor.Diagnostic, error) {
	score, diagnostics, err := research.Combine(frame, universe, e.combo, history)
	if err != nil {
		return factor.Frame{}, nil, err
	}
	frame.Values["score"] = score
	return frame, diagnostics, nil
}

func (e *decisionEngine) buildPortfolio(frame factor.Frame, universe factor.Universe, sequence uint64, nav float64, executableAt, expireAt int64) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
	spec := factor.PortfolioSpec{
		StrategyID: e.strategyID, AccountID: e.accountID,
		DecisionTime: frame.DecisionTime, ExecutableAt: executableAt, ExpireAt: expireAt,
		PlanSequence: sequence, SnapshotID: frame.SnapshotID,
		PlanHash: e.manifest.StrategyHash(), FactorPlanHash: e.plan.Hash(), UniverseVersion: universe.Version,
		Budget: factor.FrozenBudget{Version: fmt.Sprint(sequence), Currency: e.currency, NAV: nav}, Mode: e.portfolio.Mode,
	}
	if e.builder != nil {
		return e.builder(factor.CloneFrame(frame), factor.CloneUniverse(universe), spec, e.portfolio)
	}
	return factor.TopBottomKNotional(frame, "score", universe, spec, e.portfolio.K, e.portfolio.LongNotional, e.portfolio.ShortNotional)
}
