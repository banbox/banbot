package factor

import (
	"math"
	"strings"
	"testing"
)

func TestFA06DiagonalIdentitiesAndCorrelatedCovarianceProperties(t *testing.T) {
	// 1. Orthogonal diagonal covariance with sigmas = [0.01, 0.02, 0.04] -> variances = [1e-4, 4e-4, 16e-4].
	diag3 := &CovarianceMatrix{
		SIDs: []int32{1, 2, 3},
		Matrix: [][]float64{
			{0.0001, 0, 0},
			{0, 0.0004, 0},
			{0, 0, 0.0016},
		},
	}
	selected3 := map[int32]float64{1: 1.0 / 3, 2: 1.0 / 3, 3: 1.0 / 3}
	scores3 := map[int32]float64{1: 1.0, 2: 1.0, 3: 1.0}
	ctx3 := PortfolioContext{
		Spec:       PortfolioSpec{Budget: FrozenBudget{NAV: 10000}},
		Covariance: diag3,
	}

	// ERC and risk-parity alias must both degrade to exact 1/sigma_i proportions: [4/7, 2/7, 1/7].
	for _, method := range []string{"erc", "risk-parity"} {
		cfg := PortfolioPolicyConfig{
			Policy:       "lifecycle-v1",
			LongNotional: 1.0,
			Allocation:   AllocationConfig{Method: method},
		}
		normCfg, err := NormalizePortfolioPolicyConfig(cfg)
		if err != nil {
			t.Fatalf("%s normalize error: %v", method, err)
		}
		if normCfg.Allocation.Method != "erc" {
			t.Fatalf("expected risk-parity canonicalized to erc, got %q", normCfg.Allocation.Method)
		}
		got, err := AllocateSelectedContext(selected3, cfg, ctx3, scores3)
		if err != nil {
			t.Fatalf("%s allocate error: %v", method, err)
		}
		want := map[int32]float64{1: 4.0 / 7.0, 2: 2.0 / 7.0, 3: 1.0 / 7.0}
		for sid, w := range want {
			if math.Abs(got[sid]-w) > 1e-11 {
				t.Fatalf("%s diagonal SID %d got %.12f want %.12f", method, sid, got[sid], w)
			}
		}
	}

	// Min-Variance on diagonal covariance must degrade to exact 1/sigma_i^2 proportions: [16/21, 4/21, 1/21].
	mvCfg := PortfolioPolicyConfig{
		Policy:       "lifecycle-v1",
		LongNotional: 1.0,
		Allocation:   AllocationConfig{Method: "min-variance"},
	}
	mvGot, err := AllocateSelectedContext(selected3, mvCfg, ctx3, scores3)
	if err != nil {
		t.Fatal(err)
	}
	wantMV := map[int32]float64{1: 16.0 / 21.0, 2: 4.0 / 21.0, 3: 1.0 / 21.0}
	for sid, w := range wantMV {
		if math.Abs(mvGot[sid]-w) > 1e-11 {
			t.Fatalf("min-variance diagonal SID %d got %.12f want %.12f", sid, mvGot[sid], w)
		}
	}

	// HRP on 4-asset power-of-two diagonal covariance [1e-4, 4e-4, 16e-4, 64e-4] must give exact 1/sigma_i^2: [64/85, 16/85, 4/85, 1/85].
	diag4 := &CovarianceMatrix{
		SIDs: []int32{1, 2, 3, 4},
		Matrix: [][]float64{
			{0.0001, 0, 0, 0},
			{0, 0.0004, 0, 0},
			{0, 0, 0.0016, 0},
			{0, 0, 0, 0.0064},
		},
	}
	selected4 := map[int32]float64{1: 0.25, 2: 0.25, 3: 0.25, 4: 0.25}
	scores4 := map[int32]float64{1: 1, 2: 1, 3: 1, 4: 1}
	hrpGot, err := AllocateSelectedContext(selected4, PortfolioPolicyConfig{
		Policy:       "lifecycle-v1",
		LongNotional: 1.0,
		Allocation:   AllocationConfig{Method: "hrp"},
	}, PortfolioContext{Spec: PortfolioSpec{Budget: FrozenBudget{NAV: 10000}}, Covariance: diag4}, scores4)
	if err != nil {
		t.Fatal(err)
	}
	wantHRP := map[int32]float64{1: 64.0 / 85.0, 2: 16.0 / 85.0, 3: 4.0 / 85.0, 4: 1.0 / 85.0}
	for sid, w := range wantHRP {
		if math.Abs(hrpGot[sid]-w) > 1e-11 {
			t.Fatalf("hrp diagonal SID %d got %.12f want %.12f", sid, hrpGot[sid], w)
		}
	}

	// 2. Correlated covariance: assets 1 & 2 have identical variance 0.04 and high correlation rho=0.9 (cov=0.036);
	// asset 3 has variance 0.04 and zero correlation with 1 & 2.
	corrCov := [][]float64{
		{0.04, 0.036, 0},
		{0.036, 0.04, 0},
		{0, 0, 0.04},
	}
	ercW, err := EqualRiskContribution(corrCov)
	if err != nil {
		t.Fatal(err)
	}
	// Check exact Equal Risk Contribution condition: w_i * (Sigma * w)_i == (w' Sigma w) / 3.
	portVar := 0.0
	rc := make([]float64, 3)
	for i := 0; i < 3; i++ {
		sw := 0.0
		for j := 0; j < 3; j++ {
			sw += corrCov[i][j] * ercW[j]
		}
		rc[i] = ercW[i] * sw
		portVar += rc[i]
	}
	for i := 0; i < 3; i++ {
		if math.Abs(rc[i]-portVar/3) > 1e-12 {
			t.Fatalf("ERC risk contribution mismatch at %d: got %.14f want %.14f (weights=%v)", i, rc[i], portVar/3, ercW)
		}
	}
	// Diversifier asset 3 must receive strictly larger weight than correlated twins 1 & 2 in both ERC and HRP!
	if !(ercW[2] > ercW[0] && math.Abs(ercW[0]-ercW[1]) < 1e-12) {
		t.Fatalf("ERC should overweight uncorrelated diversifier: %v", ercW)
	}
	hrpCorr, err := standaloneHRP(corrCov)
	if err != nil {
		t.Fatal(err)
	}
	if !(hrpCorr[2] > hrpCorr[0] && math.Abs(hrpCorr[0]-hrpCorr[1]) < 1e-12) {
		t.Fatalf("HRP should overweight uncorrelated diversifier: %v", hrpCorr)
	}

	// 3. Mean-Variance score tilt & RiskAversion monotonicity:
	diffScores := map[int32]float64{1: 2.0, 2: 1.0, 3: 0.5}
	equalVarCov := &CovarianceMatrix{
		SIDs:   []int32{1, 2, 3},
		Matrix: [][]float64{{0.01, 0, 0}, {0, 0.01, 0}, {0, 0, 0.01}},
	}
	lowRA, err := AllocateSelectedContext(selected3, PortfolioPolicyConfig{
		Policy: "lifecycle-v1", LongNotional: 1.0,
		Allocation: AllocationConfig{Method: "mean-variance", RiskAversion: 0.5},
	}, PortfolioContext{Spec: PortfolioSpec{Budget: FrozenBudget{NAV: 10000}}, Covariance: equalVarCov}, diffScores)
	if err != nil {
		t.Fatal(err)
	}
	highRA, err := AllocateSelectedContext(selected3, PortfolioPolicyConfig{
		Policy: "lifecycle-v1", LongNotional: 1.0,
		Allocation: AllocationConfig{Method: "mean-variance", RiskAversion: 10.0},
	}, PortfolioContext{Spec: PortfolioSpec{Budget: FrozenBudget{NAV: 10000}}, Covariance: equalVarCov}, diffScores)
	if err != nil {
		t.Fatal(err)
	}
	if !(lowRA[1] > lowRA[2] && lowRA[2] > lowRA[3]) {
		t.Fatalf("mean-variance did not tilt toward higher factor scores under equal variance: %+v", lowRA)
	}
	if !(lowRA[1] > highRA[1] && math.Abs(highRA[1]-1.0/3.0) < 0.05) {
		t.Fatalf("higher risk_aversion should shrink toward min-variance (1/3): lowRA=%+v highRA=%+v", lowRA, highRA)
	}
}

func TestFA06WarmupFallbackLongShortSidesAndLifecycleColumnBinding(t *testing.T) {
	// 1. Insufficient return rows (< RiskWindow) gracefully fall back to inverse-volatility (if Volatility present) or equal (if absent).
	selectedLS := map[int32]float64{1: 0.3, 2: 0.3, 3: -0.2, 4: -0.2}
	scoresLS := map[int32]float64{1: 2.0, 2: 1.0, 3: -1.0, 4: -3.0}
	shortHistory := []RiskReturnRow{
		{GridTime: 1000, Returns: map[int32]float64{1: 0.01, 2: -0.02, 3: 0.01, 4: -0.03}},
		{GridTime: 2000, Returns: map[int32]float64{1: -0.01, 2: 0.02, 3: -0.01, 4: 0.03}},
	}
	cfgLS := PortfolioPolicyConfig{
		Policy:        "lifecycle-v1",
		LongNotional:  0.6,
		ShortNotional: 0.4,
		Allocation:    AllocationConfig{Method: "erc", RiskWindow: 10},
	}
	// Fallback to equal when Volatility is absent:
	gotEq, err := AllocateSelectedContext(selectedLS, cfgLS, PortfolioContext{
		Spec:          PortfolioSpec{Budget: FrozenBudget{NAV: 10000}},
		ReturnHistory: shortHistory,
	}, scoresLS)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(gotEq[1]-0.3) > 1e-12 || math.Abs(gotEq[2]-0.3) > 1e-12 || math.Abs(gotEq[3]+0.2) > 1e-12 || math.Abs(gotEq[4]+0.2) > 1e-12 {
		t.Fatalf("expected equal warmup fallback, got %+v", gotEq)
	}

	// Fallback to inverse-volatility when Volatility is present:
	gotInvVol, err := AllocateSelectedContext(selectedLS, cfgLS, PortfolioContext{
		Spec:          PortfolioSpec{Budget: FrozenBudget{NAV: 10000}},
		ReturnHistory: shortHistory,
		Volatility:    map[int32]float64{1: 0.01, 2: 0.02, 3: 0.01, 4: 0.03},
	}, scoresLS)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(gotInvVol[1]-0.4) > 1e-12 || math.Abs(gotInvVol[2]-0.2) > 1e-12 || math.Abs(gotInvVol[3]+0.3) > 1e-12 || math.Abs(gotInvVol[4]+0.1) > 1e-12 {
		t.Fatalf("expected inverse-vol warmup fallback, got %+v", gotInvVol)
	}

	// 2. LifecyclePolicy.Propose binds "volatility" and "beta" directly from Frame.Values!
	u := Universe{Version: "u1", Investable: []int32{1, 2, 3}, Tradable: []int32{1, 2, 3}, Reference: []int32{1, 2, 3}, Static: true}
	f := Frame{
		SnapshotID: "snap", PlanHash: "fp", DecisionTime: 1000, GridTime: 1000,
		Values: map[string]map[int32]Numeric{
			"score":      {1: {3.0, Valid}, 2: {2.0, Valid}, 3: {1.0, Valid}},
			"volatility": {1: {0.01, Valid}, 2: {0.03, Valid}, 3: {0.05, Valid}},
			"beta":       {1: {2.0, Valid}, 2: {2.0, Valid}, 3: {1.0, Valid}},
		},
	}
	spec := PortfolioSpec{
		StrategyID: "s", AccountID: "a", DecisionTime: 1000, ExecutableAt: 1001, ExpireAt: 1999,
		PlanSequence: 1, SnapshotID: "snap", PlanHash: "sp", FactorPlanHash: "fp", UniverseVersion: "u1",
		Budget: FrozenBudget{Version: "b1", Currency: "USDT", NAV: 10000}, Mode: Full,
	}
	policy, err := NewLifecyclePolicy(PortfolioPolicyConfig{
		Policy:       "lifecycle-v1",
		LongNotional: 1.0,
		Selection:    SelectionConfig{LongK: 2},
		Allocation:   AllocationConfig{Method: "inverse-volatility", BetaCap: 0.5},
	})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := policy.Propose(PortfolioContext{
		Frame: f, Universe: u, Spec: spec, GridTime: 1000, BarMillis: 1000,
		Positions:  map[int32]PositionEvidence{},
		Marks:      map[int32]float64{1: 100, 2: 100, 3: 100},
		AssetNames: map[int32]string{1: "A", 2: "B", 3: "C"},
		SIDMappingVersion: "sid-v1",
	}, nil)
	if err != nil {
		t.Fatalf("Propose with Frame volatility/beta failed: %v", err)
	}
	// Before beta_cap: SID 1 gets 0.75 (1/0.01 / (1/0.01+1/0.03)), SID 2 gets 0.25; portfolio beta = 0.75*2 + 0.25*2 = 2.0.
	// BetaCap = 0.5 scales portfolio by 0.5 / 2.0 = 0.25 -> SID 1 = 0.1875, SID 2 = 0.0625!
	w1 := mustQuantity(proposal.Target.Allocations()[1].Value)
	w2 := mustQuantity(proposal.Target.Allocations()[2].Value)
	if math.Abs(w1-0.1875) > 1e-12 || math.Abs(w2-0.0625) > 1e-12 {
		t.Fatalf("expected SID1=0.1875 SID2=0.0625 after inv-vol + beta_cap=0.5, got w1=%g w2=%g", w1, w2)
	}

	// 3. Config validation rejects invalid fields.
	for _, bad := range []AllocationConfig{
		{Method: "unknown"},
		{Method: "erc", RiskWindow: 1},
		{Method: "erc", RiskWindow: -5},
		{Method: "hrp", Shrinkage: -0.1},
		{Method: "hrp", Shrinkage: 1.1},
		{Method: "hrp", RiskAversion: 2.0},
		{Method: "mean-variance", RiskAversion: -1.0},
		{Method: "inverse-volatility", VolatilityColumn: "   "},
		{Method: "equal", BetaColumn: "\t"},
	} {
		cfg := baseLifecycle()
		cfg.Allocation = bad
		if _, err := NormalizePortfolioPolicyConfig(cfg); err == nil {
			t.Fatalf("accepted invalid AllocationConfig: %+v", bad)
		}
	}
	// Direct AllocateSelected without AutoVolatilityFallback still rejects missing volatility strictly.
	if _, err := AllocateSelected(map[int32]float64{1: 1}, PortfolioPolicyConfig{
		Policy: "lifecycle-v1", LongNotional: 1, Allocation: AllocationConfig{Method: "inverse-volatility"},
	}, 1000, nil, nil); err == nil || !strings.Contains(err.Error(), "positive visible volatility") {
		t.Fatalf("expected missing volatility error, got %v", err)
	}
}
