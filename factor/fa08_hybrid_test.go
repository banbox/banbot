package factor_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/orm"
)

func fa08PortfolioSpec(seq uint64, decisionMS int64, nav float64, mode factor.PortfolioMode) factor.PortfolioSpec {
	return factor.PortfolioSpec{
		StrategyID:      "strat-fa08",
		AccountID:       "acct-fa08",
		PlanSequence:    seq,
		DecisionTime:    decisionMS,
		ExecutableAt:    decisionMS + 1,
		ExpireAt:        decisionMS + 60_000,
		SnapshotID:      "snap-fa08",
		PlanHash:        "plan-fa08",
		FactorPlanHash:  "factor-plan-fa08",
		UniverseVersion: "u1",
		Mode:            mode,
		Budget:          factor.FrozenBudget{Version: "v1", Currency: "USDT", NAV: nav},
	}
}

func TestBookDriftThresholdExecuteAndExecuteAllocation(t *testing.T) {
	const (
		fee  = 0.001
		slip = 0.0005
	)

	t.Run("TargetPortfolio_Execute", func(t *testing.T) {
		book, err := backtest.NewBook(10_000)
		if err != nil {
			t.Fatal(err)
		}
		book.SetDriftThreshold(0.02)
		if got := book.DriftThreshold(); got != 0.02 {
			t.Fatalf("DriftThreshold = %v, want 0.02", got)
		}

		// 1. Fresh entry (w_cur == 0 -> non-zero) must execute normally.
		t1 := int64(1_000)
		p1, err := factor.NewTargetPortfolio(fa08PortfolioSpec(1, t1, 10_000, factor.Full), map[int32]float64{
			1: 0.50,
			2: -0.30,
		})
		if err != nil {
			t.Fatal(err)
		}
		q1 := map[int32]backtest.Quote{
			1: {Price: 100, AtMS: t1 + 10, AvailableAt: t1 + 10},
			2: {Price: 50, AtMS: t1 + 10, AvailableAt: t1 + 10},
		}
		if err := book.Execute(p1, q1, t1+10, fee, slip); err != nil {
			t.Fatal(err)
		}
		s1 := book.State()
		if s1.Quantities[1] != 50 || s1.Quantities[2] != -60 || s1.Turnover != 8000 || s1.Fees <= 0 {
			t.Fatalf("unexpected initial state: %+v", s1)
		}

		// 2. Small drift (< 0.02): Asset 1 rises +2% to 102, Asset 2 rises +2% to 51.
		// Current weights are ~ +0.508 and -0.305, within 0.02 of +0.50 and -0.30.
		// Re-executing same target weights must incur ZERO turnover and ZERO fees!
		t2 := int64(2_000)
		q2 := map[int32]backtest.Quote{
			1: {Price: 102, AtMS: t2 + 10, AvailableAt: t2 + 10},
			2: {Price: 51, AtMS: t2 + 10, AvailableAt: t2 + 10},
		}
		if err := book.Mark(1, q2[1], t2+10); err != nil {
			t.Fatal(err)
		}
		if err := book.Mark(2, q2[2], t2+10); err != nil {
			t.Fatal(err)
		}
		navBefore := book.State().NAV
		p2, err := factor.NewTargetPortfolio(fa08PortfolioSpec(2, t2, navBefore, factor.Full), map[int32]float64{
			1: 0.50,
			2: -0.30,
		})
		if err != nil {
			t.Fatal(err)
		}
		q2Exec := map[int32]backtest.Quote{
			1: {Price: 102, AtMS: t2 + 20, AvailableAt: t2 + 20},
			2: {Price: 51, AtMS: t2 + 20, AvailableAt: t2 + 20},
		}
		if err := book.Execute(p2, q2Exec, t2+20, fee, slip); err != nil {
			t.Fatal(err)
		}
		s2 := book.State()
		if s2.Turnover != s1.Turnover || s2.Fees != s1.Fees {
			t.Fatalf("small drift < 0.02 incurred turnover/fees: before=(%v,%v) after=(%v,%v)", s1.Turnover, s1.Fees, s2.Turnover, s2.Fees)
		}
		if s2.Quantities[1] != 50 || s2.Quantities[2] != -60 {
			t.Fatalf("held quantities were modified inside drift band: %+v", s2.Quantities)
		}

		// 3. Drift >= 0.02 on Asset 1 (target 0.50 -> 0.55), full exit to 0 on Asset 2,
		// and fresh entry on Asset 3 must all execute normally.
		t3 := int64(3_000)
		p3, err := factor.NewTargetPortfolio(fa08PortfolioSpec(3, t3, s2.NAV, factor.Full), map[int32]float64{
			1: 0.55,
			3: 0.01,
		})
		if err != nil {
			t.Fatal(err)
		}
		q3 := map[int32]backtest.Quote{
			1: {Price: 102, AtMS: t3 + 10, AvailableAt: t3 + 10},
			2: {Price: 51, AtMS: t3 + 10, AvailableAt: t3 + 10},
			3: {Price: 20, AtMS: t3 + 10, AvailableAt: t3 + 10},
		}
		if err := book.Execute(p3, q3, t3+10, fee, slip); err != nil {
			t.Fatal(err)
		}
		s3 := book.State()
		if s3.Quantities[2] != 0 {
			t.Fatalf("full exit to 0 failed under drift threshold: qty[2]=%v", s3.Quantities[2])
		}
		if s3.Quantities[1] <= 50 || s3.Quantities[3] <= 0 {
			t.Fatalf("drift >= 0.02 or fresh entry did not trade: %+v", s3.Quantities)
		}

		// 4. Sign flip with small |w_target - w_cur| < 0.02 (+0.008 -> -0.008, diff = 0.016 < 0.02)
		// MUST still execute because w_cur * w_target < 0.
		t4 := int64(4_000)
		p4, err := factor.NewTargetPortfolio(fa08PortfolioSpec(4, t4, s3.NAV, factor.Patch), map[int32]float64{
			3: -0.008,
		})
		if err != nil {
			t.Fatal(err)
		}
		q4 := map[int32]backtest.Quote{
			3: {Price: 20, AtMS: t4 + 10, AvailableAt: t4 + 10},
		}
		if err := book.Execute(p4, q4, t4+10, fee, slip); err != nil {
			t.Fatal(err)
		}
		if got := book.State().Quantities[3]; got >= 0 {
			t.Fatalf("sign flip within |diff| < 0.02 must execute, got qty[3]=%v", got)
		}
	})

	t.Run("PortfolioTarget_ExecuteAllocation_And_ZeroDefault", func(t *testing.T) {
		bookBuf, _ := backtest.NewBook(10_000)
		bookBuf.SetDriftThreshold(0.02)
		bookDef, _ := backtest.NewBook(10_000) // default 0

		t1 := int64(1_000)
		tgt1, err := factor.NewPortfolioTarget(fa08PortfolioSpec(1, t1, 10_000, factor.Full), map[int32]factor.Allocation{
			1: {Basis: factor.NAVFraction, Value: "0.4"},
		})
		if err != nil {
			t.Fatal(err)
		}
		q1 := map[int32]backtest.Quote{1: {Price: 100, AtMS: t1 + 5, AvailableAt: t1 + 5}}
		if err := bookBuf.ExecuteAllocation(tgt1, q1, t1+5, fee, slip); err != nil {
			t.Fatal(err)
		}
		if err := bookDef.ExecuteAllocation(tgt1, q1, t1+5, fee, slip); err != nil {
			t.Fatal(err)
		}
		turn1 := bookBuf.State().Turnover

		// Price moves +3% -> w_cur = 40*103 / (6000 + 4120 - cost) ~ 0.4071 (|0.4 - 0.4071| < 0.02).
		t2 := int64(2_000)
		q2 := map[int32]backtest.Quote{1: {Price: 103, AtMS: t2 + 5, AvailableAt: t2 + 5}}
		_ = bookBuf.Mark(1, q2[1], t2+5)
		_ = bookDef.Mark(1, q2[1], t2+5)

		tgt2Buf, _ := factor.NewPortfolioTarget(fa08PortfolioSpec(2, t2, bookBuf.State().NAV, factor.Full), map[int32]factor.Allocation{
			1: {Basis: factor.NAVFraction, Value: "0.4"},
		})
		tgt2Def, _ := factor.NewPortfolioTarget(fa08PortfolioSpec(2, t2, bookDef.State().NAV, factor.Full), map[int32]factor.Allocation{
			1: {Basis: factor.NAVFraction, Value: "0.4"},
		})
		q2Exec := map[int32]backtest.Quote{1: {Price: 103, AtMS: t2 + 10, AvailableAt: t2 + 10}}
		if err := bookBuf.ExecuteAllocation(tgt2Buf, q2Exec, t2+10, fee, slip); err != nil {
			t.Fatal(err)
		}
		if err := bookDef.ExecuteAllocation(tgt2Def, q2Exec, t2+10, fee, slip); err != nil {
			t.Fatal(err)
		}
		if bookBuf.State().Turnover != turn1 {
			t.Fatalf("buffered ExecuteAllocation traded inside 2%% band: %v vs %v", bookBuf.State().Turnover, turn1)
		}
		if bookDef.State().Turnover <= turn1 {
			t.Fatalf("default drift_threshold=0 expected exact weight rebalance, got turnover %v", bookDef.State().Turnover)
		}
	})
}

func TestLifecyclePolicyDriftThresholdAndMidCyclePatchIsolation(t *testing.T) {
	cfg := factor.PortfolioPolicyConfig{
		Policy:        "lifecycle-v1",
		LongNotional:  1.0,
		ShortNotional: 0,
		Rebalance:     factor.RebalanceConfig{EveryBars: 4, DriftThreshold: 0.02},
		Selection:     factor.SelectionConfig{LongK: 2},
		Holding:       factor.HoldingConfig{StopLossPct: 0.08, CooldownBars: 1},
		Transition:    factor.TransitionConfig{Mode: "direct"},
		Allocation:    factor.AllocationConfig{Method: "equal"},
	}
	pol, err := factor.NewLifecyclePolicy(cfg)
	if err != nil {
		t.Fatal(err)
	}

	u := factor.Universe{
		Version:    "u1",
		Investable: []int32{1, 2, 3},
		Reference:  []int32{1, 2, 3},
		Tradable:   []int32{1, 2, 3},
	}
	names := map[int32]string{1: "BTC/USDT:USDT", 2: "ETH/USDT:USDT", 3: "SOL/USDT:USDT"}
	mkCtx := func(seq uint64, grid int64, nav float64, scores, closes map[int32]float64, pos map[int32]factor.PositionEvidence) factor.PortfolioContext {
		vals := map[int32]factor.Numeric{}
		for sid, sc := range scores {
			vals[sid] = factor.Numeric{Value: sc, Validity: factor.Valid}
		}
		return factor.PortfolioContext{
			Frame:      factor.Frame{GridTime: grid, DecisionTime: grid, SnapshotID: "snap-fa08", PlanHash: "factor-plan-fa08", Values: map[string]map[int32]factor.Numeric{"score": vals}},
			Universe:   u,
			Spec:       fa08PortfolioSpec(seq, grid, nav, factor.Full),
			GridTime:   grid,
			BarMillis:  1_000,
			ScoreName:  "score",
			Positions:  pos,
			Marks:      closes,
			BarClose:   closes,
			BarHigh:    closes,
			BarLow:     closes,
			AssetNames: names,
		}
	}

	// Bar 1 (1000, rebalance due): selects SID 1 and SID 2 at 0.5 each.
	p1, err := pol.Propose(mkCtx(1, 1000, 10_000,
		map[int32]float64{1: 3.0, 2: 2.0, 3: 1.0},
		map[int32]float64{1: 100, 2: 100, 3: 100},
		map[int32]factor.PositionEvidence{},
	), nil)
	if err != nil || p1.Target == nil {
		t.Fatalf("bar 1 failed: err=%v target=%v", err, p1.Target)
	}

	// Bar 2 (2000, non-rebalance bar): normal drift -> no target emitted (zero fee churn!).
	posHeld := map[int32]factor.PositionEvidence{
		1: {Quantity: "50", FirstFillTime: 1010, EntryPrice: "100"},
		2: {Quantity: "50", FirstFillTime: 1010, EntryPrice: "100"},
	}
	p2, err := pol.Propose(mkCtx(2, 2000, 10_100,
		map[int32]float64{1: 3.0, 2: 2.0, 3: 1.0},
		map[int32]float64{1: 103, 2: 99, 3: 100},
		posHeld,
	), p1.NextState)
	if err != nil || p2.Target != nil {
		t.Fatalf("non-rebalance bar without stop breach must emit nil target, got target=%v err=%v", p2.Target, err)
	}

	// Bar 3 (3000, non-rebalance bar): SID 2 drops -10% (90 <= 92) -> mid-cycle stop-loss!
	// Must emit a Patch target ONLY closing SID 2 to "0" so SID 1 is NOT rebalanced!
	p3, err := pol.Propose(mkCtx(3, 3000, 9_650,
		map[int32]float64{1: 3.0, 2: 2.0, 3: 1.0},
		map[int32]float64{1: 103, 2: 90, 3: 100},
		posHeld,
	), p2.NextState)
	if err != nil || p3.Target == nil {
		t.Fatalf("mid-cycle stop-loss must emit target, got err=%v target=%v", err, p3.Target)
	}
	if p3.Target.Spec().Mode != factor.Patch {
		t.Fatalf("mid-cycle off-schedule stop exit must use Patch mode to avoid churning other holdings, got %v", p3.Target.Spec().Mode)
	}
	alloc3 := p3.Target.Allocations()
	if len(alloc3) != 1 || alloc3[2] != (factor.Allocation{Basis: factor.AbsoluteQuantity, Value: "0"}) {
		t.Fatalf("expected Patch target with only SID 2 -> 0, got %+v", alloc3)
	}

	// Bar 5 (5000, next rebalance round = floor(5000/4000) == 1):
	// SID 1 has quantity 50 at price 101, NAV = 10_050 -> w_cur = 5050/10050 = 0.50248 (< 0.02 from 0.5).
	// SID 1 must be preserved as AbsoluteQuantity "50" via drift-buffer, while SID 3 enters at 0.5!
	posBar5 := map[int32]factor.PositionEvidence{
		1: {Quantity: "50", FirstFillTime: 1010, EntryPrice: "100"},
		2: {Quantity: "0"},
	}
	p5, err := pol.Propose(mkCtx(5, 5000, 10_050,
		map[int32]float64{1: 3.0, 3: 2.5, 2: 1.0},
		map[int32]float64{1: 101, 2: 90, 3: 50},
		posBar5,
	), p3.NextState)
	if err != nil || p5.Target == nil {
		t.Fatalf("bar 5 rebalance failed: err=%v target=%v", err, p5.Target)
	}
	alloc5 := p5.Target.Allocations()
	if alloc5[1] != (factor.Allocation{Basis: factor.AbsoluteQuantity, Value: "50"}) {
		t.Fatalf("expected SID 1 preserved at exact 50 units inside drift band, got %+v", alloc5[1])
	}
	if alloc5[3] != (factor.Allocation{Basis: factor.NAVFraction, Value: "0.5"}) {
		t.Fatalf("expected fresh entry SID 3 at 0.5 NAVFraction, got %+v", alloc5[3])
	}
}

func TestLifecyclePolicyStopLossTrailingStopCooldownAndZeroLookahead(t *testing.T) {
	u := factor.Universe{
		Version:    "u1",
		Investable: []int32{1, 2, 3, 4},
		Reference:  []int32{1, 2, 3, 4},
		Tradable:   []int32{1, 2, 3, 4},
	}
	names := map[int32]string{1: "BTC/USDT:USDT", 2: "ETH/USDT:USDT", 3: "SOL/USDT:USDT", 4: "DOGE/USDT:USDT"}

	t.Run("LongAndShort_StopLoss_And_CooldownReentry", func(t *testing.T) {
		// Also verify auto-promotion to lifecycle-v1 when Policy == "" and Holding has stop fields!
		rawCfg := factor.PortfolioPolicyConfig{
			LongNotional:  0.5,
			ShortNotional: 0.5,
			Rebalance:     factor.RebalanceConfig{EveryBars: 1},
			Selection:     factor.SelectionConfig{LongK: 1, ShortK: 1},
			Holding:       factor.HoldingConfig{StopLossPct: 0.08, CooldownBars: 2},
			Transition:    factor.TransitionConfig{Mode: "direct"},
			Allocation:    factor.AllocationConfig{Method: "equal"},
		}
		norm, err := factor.NormalizePortfolioPolicyConfig(rawCfg)
		if err != nil || norm.Policy != "lifecycle-v1" {
			t.Fatalf("expected auto-promotion to lifecycle-v1, got policy=%q err=%v", norm.Policy, err)
		}
		pol, err := factor.NewLifecyclePolicy(rawCfg)
		if err != nil {
			t.Fatal(err)
		}

		// Scores remain constant: SID 1 best long (+10), SID 2 runner-up long (+5),
		// SID 3 runner-up short (-5), SID 4 best short (-10).
		scores := map[int32]float64{1: 10, 2: 5, 3: -5, 4: -10}
		step := func(seq uint64, grid int64, closes map[int32]float64, pos map[int32]factor.PositionEvidence, state json.RawMessage) factor.PortfolioProposal {
			t.Helper()
			vals := map[int32]factor.Numeric{}
			for sid, sc := range scores {
				vals[sid] = factor.Numeric{Value: sc, Validity: factor.Valid}
			}
			prop, err := pol.Propose(factor.PortfolioContext{
				Frame:      factor.Frame{GridTime: grid, DecisionTime: grid, SnapshotID: "snap-fa08", PlanHash: "factor-plan-fa08", Values: map[string]map[int32]factor.Numeric{"score": vals}},
				Universe:   u,
				Spec:       fa08PortfolioSpec(seq, grid, 10_000, factor.Full),
				GridTime:   grid,
				BarMillis:  1_000,
				ScoreName:  "score",
				Positions:  pos,
				Marks:      closes,
				BarClose:   closes,
				BarHigh:    closes,
				BarLow:     closes,
				AssetNames: names,
			}, state)
			if err != nil {
				t.Fatalf("step %d failed: %v", seq, err)
			}
			return prop
		}

		// Bar 1 (1000): enters Long SID 1 @ 100, Short SID 4 @ 100.
		p1 := step(1, 1000, map[int32]float64{1: 100, 2: 100, 3: 100, 4: 100}, nil, nil)
		if p1.Target.Allocations()[1].Value != "0.5" || p1.Target.Allocations()[4].Value != "-0.5" {
			t.Fatalf("bar 1 unexpected initial selection: %+v", p1.Target.Allocations())
		}

		// Bar 2 (2000): SID 1 drops -9% to 91 (long stop-loss >= 8%), SID 4 rallies +9% to 109 (short stop-loss >= 8%)!
		// Both must exit to "0", and runner-ups SID 2 (+0.5) and SID 3 (-0.5) are selected immediately!
		pos2 := map[int32]factor.PositionEvidence{
			1: {Quantity: "50", FirstFillTime: 1005, EntryPrice: "100"},
			4: {Quantity: "-50", FirstFillTime: 1005, EntryPrice: "100"},
		}
		p2 := step(2, 2000, map[int32]float64{1: 91, 2: 100, 3: 100, 4: 109}, pos2, p1.NextState)
		a2 := p2.Target.Allocations()
		if a2[1].Value != "0" || a2[4].Value != "0" || a2[2].Value != "0.5" || a2[3].Value != "-0.5" {
			t.Fatalf("bar 2 expected stop-loss exit on 1 & 4 and rotation into 2 & 3, got %+v (reasons=%+v)", a2, p2.Reasons)
		}

		// Bar 3 (3000, cooldown bar 1 of 2) & Bar 4 (4000, cooldown bar 2 of 2):
		// Even though SID 1 & SID 4 still have top scores, cooldown_bars=2 blocks re-entry!
		pos3 := map[int32]factor.PositionEvidence{
			1: {Quantity: "0"},
			2: {Quantity: "50", FirstFillTime: 2005, EntryPrice: "100"},
			3: {Quantity: "-50", FirstFillTime: 2005, EntryPrice: "100"},
			4: {Quantity: "0"},
		}
		p3 := step(3, 3000, map[int32]float64{1: 95, 2: 100, 3: 100, 4: 105}, pos3, p2.NextState)
		if p3.Target.Allocations()[1].Value != "0" || p3.Target.Allocations()[4].Value != "0" ||
			p3.Target.Allocations()[2].Value != "0.5" || p3.Target.Allocations()[3].Value != "-0.5" {
			t.Fatalf("bar 3 cooldown bar 1 failed to block re-entry: %+v", p3.Target.Allocations())
		}
		p4 := step(4, 4000, map[int32]float64{1: 95, 2: 100, 3: 100, 4: 105}, pos3, p3.NextState)
		if p4.Target.Allocations()[1].Value != "0" || p4.Target.Allocations()[4].Value != "0" ||
			p4.Target.Allocations()[2].Value != "0.5" || p4.Target.Allocations()[3].Value != "-0.5" {
			t.Fatalf("bar 4 cooldown bar 2 failed to block re-entry: %+v", p4.Target.Allocations())
		}

		// Bar 5 (5000, after 2 cooldown bars): cooldown expired -> SID 1 and SID 4 cleanly re-enter!
		p5 := step(5, 5000, map[int32]float64{1: 95, 2: 100, 3: 100, 4: 105}, pos3, p4.NextState)
		if p5.Target.Allocations()[1].Value != "0.5" || p5.Target.Allocations()[4].Value != "-0.5" {
			t.Fatalf("bar 5 expected clean re-entry after 2 cooldown bars, got %+v", p5.Target.Allocations())
		}
	})

	t.Run("TrailingStop_PeakTroughRatchet_And_ZeroEntryBarLookahead", func(t *testing.T) {
		zero := 0.0
		cfg := factor.PortfolioPolicyConfig{
			Policy:        "lifecycle-v1",
			LongNotional:  0.5,
			ShortNotional: 0.5,
			Rebalance:     factor.RebalanceConfig{EveryBars: 5},
			Selection:     factor.SelectionConfig{LongK: 1, ShortK: 1},
			Holding: factor.HoldingConfig{
				TrailingStopPct: 0.12,
				CooldownBars:    1,
				ByAsset: map[string]factor.HoldingOverride{
					"ETH/USDT:USDT": {TrailingStopPct: &zero},
				},
			},
			Transition: factor.TransitionConfig{Mode: "direct"},
			Allocation: factor.AllocationConfig{Method: "equal"},
		}
		pol, err := factor.NewLifecyclePolicy(cfg)
		if err != nil {
			t.Fatal(err)
		}
		scores := map[int32]float64{1: 10, 2: 5, 3: -5, 4: -10}
		mkStep := func(seq uint64, grid int64, closeMap, highMap, lowMap map[int32]float64, pos map[int32]factor.PositionEvidence, state json.RawMessage) factor.PortfolioProposal {
			t.Helper()
			vals := map[int32]factor.Numeric{}
			for sid, sc := range scores {
				vals[sid] = factor.Numeric{Value: sc, Validity: factor.Valid}
			}
			prop, err := pol.Propose(factor.PortfolioContext{
				Frame:      factor.Frame{GridTime: grid, DecisionTime: grid, SnapshotID: "snap-fa08", PlanHash: "factor-plan-fa08", Values: map[string]map[int32]factor.Numeric{"score": vals}},
				Universe:   u,
				Spec:       fa08PortfolioSpec(seq, grid, 10_000, factor.Full),
				GridTime:   grid,
				BarMillis:  1_000,
				ScoreName:  "score",
				Positions:  pos,
				Marks:      closeMap,
				BarClose:   closeMap,
				BarHigh:    highMap,
				BarLow:     lowMap,
				AssetNames: names,
			}, state)
			if err != nil {
				t.Fatalf("mkStep %d failed: %v", seq, err)
			}
			return prop
		}

		// Bar 1 (1000, entry decision): Entry bar has huge pre-entry wick (High=200 on SID 1, Low=40 on SID 4)
		// before close=100. Entry fill happens AFTER bar 1 at 1005 @ 100.
		// Entry-bar High/Low MUST NOT leak into PeakPrice!
		p1 := mkStep(1, 1000,
			map[int32]float64{1: 100, 2: 100, 3: 100, 4: 100},
			map[int32]float64{1: 200, 2: 100, 3: 100, 4: 100},
			map[int32]float64{1: 100, 2: 100, 3: 100, 4: 40},
			nil, nil,
		)

		// Bar 2 (2000, non-rebalance; fill was at 1005 inside (1000, 2000]):
		// Because fill happened at 1005 > 1000, bar 2's High=180 / Low=50 could have occurred at 1001 BEFORE fill 1005!
		// So bar 2 uses curPrice=102 / 98 (not intrabar 180 / 50), and neither triggers a false trailing stop!
		pos := map[int32]factor.PositionEvidence{
			1: {Quantity: "50", FirstFillTime: 1005, EntryPrice: "100"},
			4: {Quantity: "-50", FirstFillTime: 1005, EntryPrice: "100"},
		}
		p2 := mkStep(2, 2000,
			map[int32]float64{1: 102, 2: 100, 3: 100, 4: 98},
			map[int32]float64{1: 180, 2: 100, 3: 100, 4: 99},
			map[int32]float64{1: 101, 2: 100, 3: 100, 4: 50},
			pos, p1.NextState,
		)
		if p2.Target != nil {
			t.Fatalf("expected zero pre-fill intrabar lookahead at bar 2, but got premature stop target: %+v (%+v)", p2.Target.Allocations(), p2.Reasons)
		}

		// Bar 3 (3000, non-rebalance; full bar [2000, 3000] is strictly post-fill 1005 <= 2000):
		// SID 1 reaches High=125, Close=120 -> PeakPrice ratchets to 125.
		// SID 4 reaches Low=80, Close=84 -> Trough ratchets to 80.
		p3 := mkStep(3, 3000,
			map[int32]float64{1: 120, 2: 100, 3: 100, 4: 84},
			map[int32]float64{1: 125, 2: 100, 3: 100, 4: 86},
			map[int32]float64{1: 118, 2: 100, 3: 100, 4: 80},
			pos, p2.NextState,
		)
		if p3.Target != nil {
			t.Fatalf("bar 3 favorable move < 12%% pullback must not exit, got %+v", p3.Target)
		}

		// Bar 4 (4000, non-rebalance):
		// SID 1 pulls back to Close=109 ((125-109)/125 = 12.8% >= 12%) -> trailing-stop-exit!
		// SID 4 bounces to Close=90 ((90-80)/80 = 12.5% >= 12%) -> trailing-stop-exit!
		p4 := mkStep(4, 4000,
			map[int32]float64{1: 109, 2: 100, 3: 100, 4: 90},
			map[int32]float64{1: 121, 2: 100, 3: 100, 4: 91},
			map[int32]float64{1: 108, 2: 100, 3: 100, 4: 83},
			pos, p3.NextState,
		)
		if p4.Target == nil || p4.Target.Spec().Mode != factor.Patch {
			t.Fatalf("bar 4 expected mid-cycle Patch trailing-stop exit, got %+v", p4.Target)
		}
		if p4.Target.Allocations()[1].Value != "0" || p4.Target.Allocations()[4].Value != "0" {
			t.Fatalf("bar 4 expected both SID 1 and SID 4 closed to 0, got %+v", p4.Target.Allocations())
		}
	})

	t.Run("ValidationRejectsInvalidDriftAndStops", func(t *testing.T) {
		bad := []factor.PortfolioPolicyConfig{
			{Policy: "lifecycle-v1", LongNotional: 1, Rebalance: factor.RebalanceConfig{DriftThreshold: -0.01}},
			{Policy: "lifecycle-v1", LongNotional: 1, Rebalance: factor.RebalanceConfig{DriftThreshold: 1.0}},
			{Policy: "lifecycle-v1", LongNotional: 1, Allocation: factor.AllocationConfig{DriftThreshold: 1.5}},
			{Policy: "lifecycle-v1", LongNotional: 1, Holding: factor.HoldingConfig{StopLossPct: -0.05}},
			{Policy: "lifecycle-v1", LongNotional: 1, Holding: factor.HoldingConfig{StopLossPct: 1.0}},
			{Policy: "lifecycle-v1", LongNotional: 1, Holding: factor.HoldingConfig{TrailingStopPct: 1.2}},
		}
		for i, c := range bad {
			if _, err := factor.NormalizePortfolioPolicyConfig(c); err == nil {
				t.Fatalf("case %d expected validation error for %+v", i, c)
			}
		}
	})
}

func TestCryptoSectorDictionaryAndNeutralizationSessionVsBatch(t *testing.T) {
	cases := map[string]string{
		"BTC/USDT:USDT":      factor.SectorNameL1,
		"ETH/USDT":           factor.SectorNameL1,
		"NEAR/USDT:USDT":     factor.SectorNameL1,
		"ARB/USDT:USDT":      factor.SectorNameL2,
		"MATIC/USDT":         factor.SectorNameL2,
		"POL/USDT:USDT":      factor.SectorNameL2,
		"UNI":                factor.SectorNameDeFi,
		"AAVE/USDT:USDT":     factor.SectorNameDeFi,
		"MKR/USDT":           factor.SectorNameDeFi,
		"SKY/USDT:USDT":      factor.SectorNameDeFi,
		"FET/USDT:USDT":      factor.SectorNameAIDePin,
		"RNDR/USDT":          factor.SectorNameAIDePin,
		"RENDER/USDT:USDT":   factor.SectorNameAIDePin,
		"1000PEPE/USDT:USDT": factor.SectorNameMeme,
		"DOGE/USDT:USDT":     factor.SectorNameMeme,
		"1000SATS/USDT:USDT": factor.SectorNameMeme,
		"1MBABYDOGE/USDT":    factor.SectorNameOther,
		"AXS/USDT:USDT":      factor.SectorNameGaming,
		"BEAMX/USDT:USDT":    factor.SectorNameGaming,
		"HMSTR/USDT:USDT":    factor.SectorNameGaming,
		"LINK/USDT:USDT":     factor.SectorNameInfra,
		"EIGEN/USDT:USDT":    factor.SectorNameInfra,
		"FOOBAR/USDT:USDT":   factor.SectorNameOther,
	}
	for sym, want := range cases {
		if got := factor.CryptoSector(sym); got != want {
			t.Fatalf("CryptoSector(%q) = %q, want %q (base=%q)", sym, got, want, factor.NormalizeCryptoBaseSymbol(sym))
		}
	}

	closeNode := factor.Field("kline", "close", "1h")
	secCode := factor.SectorCode("1h")
	demean := factor.SectorDemean(closeNode)
	demeanViaGroup := factor.GroupDemeanByField(closeNode, secCode)
	zscore := factor.SectorZScore(closeNode)
	smooth := factor.SMA(demean, 2)

	plan, err := factor.New().
		Add("code", secCode).
		Add("demean", demean).
		Add("demean_group", demeanViaGroup).
		Add("zscore", zscore).
		Add("smooth", smooth).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	// Verify zero external DB source is required: only kline is in plan.Inputs()!
	if inputs := plan.Inputs(); len(inputs) != 1 || inputs[0].Source != "kline" {
		t.Fatalf("expected only kline input with zero external sector source, got %+v", inputs)
	}

	sids := []int32{1, 2, 3, 4, 5, 6}
	sidMap := map[int32]string{
		1: "1000PEPE/USDT:USDT", // Meme (code 5)
		2: "DOGE/USDT:USDT",     // Meme (code 5)
		3: "WIF/USDT:USDT",      // Meme (code 5)
		4: "UNI",                // DeFi (code 3)
		5: "AAVE/USDT:USDT",     // DeFi (code 3)
		6: "FOOBAR/USDT:USDT",   // Other (code 0, singleton -> demean 0, zscore 0)
	}
	u := factor.Universe{Version: "u-sec", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true}
	const hour = int64(3_600_000)

	priceTable := [][]float64{
		{10, 20, 30, 100, 140, 55},
		{12, 18, 36, 110, 130, 60},
		{15, 25, 50, 90, 150, 75},
	}
	snapshots := make([]*factor.Snapshot, len(priceTable))
	for stepIdx, row := range priceTable {
		at := int64(stepIdx+1) * hour
		recs := make([]factor.VersionRecord, 0, len(sids))
		reqs := make([]factor.Requirement, 0, len(sids))
		for i, sid := range sids {
			reqs = append(reqs, factor.Requirement{SID: sid, Source: "kline", TimeFrame: "1h", EventTime: at})
			recs = append(recs, factor.VersionRecord{
				Series:        orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at, EndMS: at, Closed: true, Values: map[string]any{"close": row[i]}},
				EventTime:     at,
				AvailableAt:   at,
				IngestedAt:    at,
				Revision:      1,
				SourceVersion: "v1",
			})
		}
		snap, err := factor.Freeze(factor.SnapshotSpec{
			GridTime:          at,
			DecisionTime:      at,
			Universe:          u,
			SIDMap:            sidMap,
			Schemas:           map[string]string{"kline": "s1"},
			SourceVersions:    map[string]string{"kline": "v1"},
			AdjustmentVersion: "raw",
			VisibilityPolicy:  "available-at",
		}, recs, reqs)
		if err != nil {
			t.Fatal(err)
		}
		snapshots[stepIdx] = snap
	}

	batchFrames, err := plan.Batch(snapshots, 1000)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := factor.NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	for tIdx, snap := range snapshots {
		sf, err := sess.Evaluate(snap)
		if err != nil {
			t.Fatal(err)
		}
		bf := batchFrames[tIdx]

		// Meme sector {1, 2, 3} mean and DeFi sector {4, 5} mean must both be 0!
		memeSum := sf.Values["demean"][1].Value + sf.Values["demean"][2].Value + sf.Values["demean"][3].Value
		defiSum := sf.Values["demean"][4].Value + sf.Values["demean"][5].Value
		if math.Abs(memeSum) > 1e-12 || math.Abs(defiSum) > 1e-12 || math.Abs(sf.Values["demean"][6].Value) > 1e-12 {
			t.Fatalf("t=%d sector mean not removed to 0: memeSum=%v defiSum=%v other=%v", tIdx, memeSum, defiSum, sf.Values["demean"][6].Value)
		}
		if sf.Values["code"][1].Value != float64(factor.SectorCodeMeme) || sf.Values["code"][4].Value != float64(factor.SectorCodeDeFi) {
			t.Fatalf("unexpected sector codes: %+v", sf.Values["code"])
		}
		for _, outName := range plan.Outputs() {
			for _, sid := range sids {
				sv := sf.Values[outName][sid]
				bv := bf.Values[outName][sid]
				if sv.Validity != bv.Validity || (sv.Validity == factor.Valid && math.Abs(sv.Value-bv.Value) > 1e-12) {
					t.Fatalf("Session vs Batch mismatch at t=%d out=%s sid=%d: sess=%+v batch=%+v", tIdx, outName, sid, sv, bv)
				}
				if outName == "demean" {
					gv := sf.Values["demean_group"][sid]
					if math.Abs(sv.Value-gv.Value) > 1e-12 {
						t.Fatalf("SectorDemean vs GroupDemeanByField(SectorCode) mismatch sid=%d: %v vs %v", sid, sv.Value, gv.Value)
					}
				}
			}
		}
	}
}

func TestQuantileGroupNeutralizationSessionVsBatch(t *testing.T) {
	ret := factor.Field("kline", "close", "1h")
	quote := factor.Field("kline", "quote", "1h")
	liq := factor.SMA(quote, 2)

	qDemean := factor.QuantileGroupDemean(ret, liq, 3)
	qZScore := factor.QuantileGroupZScore(ret, liq, 3)
	qRank := factor.QuantileGroupRank(ret, liq, 3)
	qPctRank := factor.QuantileGroupPctRank(ret, liq, 3)
	postTS := factor.EMA(qDemean, 2)

	plan, err := factor.New().
		Add("q_demean", qDemean).
		Add("q_zscore", qZScore).
		Add("q_rank", qRank).
		Add("q_pct_rank", qPctRank).
		Add("post_ts", postTS).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := factor.New().Add("bad", factor.QuantileGroupDemean(ret, quote, 1)).Compile(); err == nil {
		t.Fatal("expected nBuckets=1 to fail validation")
	}

	sids := []int32{1, 2, 3, 4, 5, 6}
	sidMap := map[int32]string{1: "A", 2: "B", 3: "C", 4: "D", 5: "E", 6: "F"}
	u := factor.Universe{Version: "u-q", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true}
	const hour = int64(3_600_000)

	// 6 assets partitioned into 3 liquidity buckets of 2 assets each:
	// Bucket 0 (small liq): SIDs {1, 2}
	// Bucket 1 (mid liq):   SIDs {3, 4}
	// Bucket 2 (large liq): SIDs {5, 6}
	quotesByBar := [][]float64{
		{100, 120, 500, 550, 2000, 2500},
		{110, 130, 520, 570, 2100, 2600},
		{105, 125, 510, 560, 2050, 2550},
		{115, 135, 530, 580, 2200, 2700},
	}
	closesByBar := [][]float64{
		{1, 3, 10, 14, 100, 110},
		{2, 6, 20, 28, 120, 100},
		{4, 8, 15, 25, 90, 130},
		{5, 9, 30, 18, 140, 100},
	}

	snapshots := make([]*factor.Snapshot, len(quotesByBar))
	for stepIdx := range quotesByBar {
		at := int64(stepIdx+1) * hour
		recs := make([]factor.VersionRecord, 0, len(sids))
		reqs := make([]factor.Requirement, 0, len(sids))
		for i, sid := range sids {
			reqs = append(reqs, factor.Requirement{SID: sid, Source: "kline", TimeFrame: "1h", EventTime: at})
			recs = append(recs, factor.VersionRecord{
				Series: orm.DataSeries{
					Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at, EndMS: at, Closed: true,
					Values: map[string]any{"close": closesByBar[stepIdx][i], "quote": quotesByBar[stepIdx][i]},
				},
				EventTime:     at,
				AvailableAt:   at,
				IngestedAt:    at,
				Revision:      1,
				SourceVersion: "v1",
			})
		}
		snap, err := factor.Freeze(factor.SnapshotSpec{
			GridTime:          at,
			DecisionTime:      at,
			Universe:          u,
			SIDMap:            sidMap,
			Schemas:           map[string]string{"kline": "s1"},
			SourceVersions:    map[string]string{"kline": "v1"},
			AdjustmentVersion: "raw",
			VisibilityPolicy:  "available-at",
		}, recs, reqs)
		if err != nil {
			t.Fatal(err)
		}
		snapshots[stepIdx] = snap
	}

	batchFrames, err := plan.Batch(snapshots, 1000)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := factor.NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	for tIdx, snap := range snapshots {
		sf, err := sess.Evaluate(snap)
		if err != nil {
			t.Fatal(err)
		}
		bf := batchFrames[tIdx]
		if tIdx == 0 {
			// Bar 0: SMA(quote, 2) is Warmup, so q_demean must propagate Warmup.
			if sf.Values["q_demean"][1].Validity != factor.Warmup {
				t.Fatalf("expected Warmup at bar 0, got %+v", sf.Values["q_demean"][1])
			}
		} else {
			// Bars >= 1: each 2-asset liquidity tier has exact demean sum == 0 and zscore +/-1!
			for _, pair := range [][2]int32{{1, 2}, {3, 4}, {5, 6}} {
				dSum := sf.Values["q_demean"][pair[0]].Value + sf.Values["q_demean"][pair[1]].Value
				zSum := sf.Values["q_zscore"][pair[0]].Value + sf.Values["q_zscore"][pair[1]].Value
				if math.Abs(dSum) > 1e-12 || math.Abs(zSum) > 1e-12 {
					t.Fatalf("t=%d bucket %v not neutralized: dSum=%v zSum=%v", tIdx, pair, dSum, zSum)
				}
				if math.Abs(math.Abs(sf.Values["q_zscore"][pair[0]].Value)-1.0) > 1e-12 {
					t.Fatalf("t=%d 2-element bucket zscore magnitude != 1: %v", tIdx, sf.Values["q_zscore"][pair[0]].Value)
				}
			}
		}
		for _, outName := range plan.Outputs() {
			for _, sid := range sids {
				sv := sf.Values[outName][sid]
				bv := bf.Values[outName][sid]
				if sv.Validity != bv.Validity || (sv.Validity == factor.Valid && math.Abs(sv.Value-bv.Value) > 1e-12) {
					t.Fatalf("QuantileGroup Session vs Batch mismatch t=%d out=%s sid=%d: sess=%+v batch=%+v", tIdx, outName, sid, sv, bv)
				}
			}
		}
	}
}
