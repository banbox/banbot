package runner

import (
	"context"
	"math"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

type fa06AllocCapture struct {
	decisions map[int64]map[int32]float64
	states    map[int64]backtest.State
}

func newFA06Capture() *fa06AllocCapture {
	return &fa06AllocCapture{
		decisions: map[int64]map[int32]float64{},
		states:    map[int64]backtest.State{},
	}
}

func (o *fa06AllocCapture) Decision(f factor.Frame, p *factor.TargetPortfolio, _ []factor.Diagnostic) error {
	if p != nil {
		o.decisions[f.DecisionTime] = p.Targets()
	}
	return nil
}
func (o *fa06AllocCapture) Evaluation(research.Report) error { return nil }
func (o *fa06AllocCapture) Executed(_ *factor.TargetPortfolio, s backtest.State, at int64) error {
	o.states[at] = s
	return nil
}
func (o *fa06AllocCapture) DecisionAllocation(f factor.Frame, t *factor.PortfolioTarget, _ []factor.Diagnostic) error {
	if t != nil {
		m := map[int32]float64{}
		for sid, a := range t.Allocations() {
			v, _ := strconv.ParseFloat(a.Value, 64)
			m[sid] = v
		}
		o.decisions[f.DecisionTime] = m
	}
	return nil
}
func (o *fa06AllocCapture) AllocationAccepted(_ *factor.PortfolioTarget, s backtest.State, at int64) error {
	o.states[at] = s
	return nil
}

func buildOrthogonalArchive(t *testing.T, prices [][]float64) Config {
	t.Helper()
	const hour int64 = 3600000
	nAssets := len(prices[0])
	store, err := factor.NewVersionStore(max(4000, len(prices)*nAssets*3))
	if err != nil {
		t.Fatal(err)
	}
	sids := make([]int32, nAssets)
	symbols := make(map[int32]string, nAssets)
	for i := 0; i < nAssets; i++ {
		sid := int32(i + 1)
		sids[i] = sid
		symbols[sid] = "ASSET" + strconv.Itoa(i+1)
	}
	for barIdx, row := range prices {
		at := int64(barIdx+1) * hour
		for i, sid := range sids {
			p := row[i]
			if err := store.Put(factor.VersionRecord{
				Series:        orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{"close": p}},
				EventTime:     at,
				AvailableAt:   at,
				IngestedAt:    at,
				Revision:      1,
				SourceVersion: "v1",
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Put(factor.VersionRecord{
				Series:        orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: at + 1, EndMS: at + 1, Closed: true, Values: map[string]any{"price": p}},
				EventTime:     at + 1,
				AvailableAt:   at + 1,
				IngestedAt:    at + 1,
				Revision:      1,
				SourceVersion: "v1",
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "fa06.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	end := int64(len(prices))*hour + 2
	return Config{
		Mode:             Weights,
		Chunks:           []Chunk{{Path: path, From: hour, To: end}},
		MaxRecords:       max(4000, len(prices)*nAssets*3),
		MaxPending:       16,
		DecisionInterval: hour,
		LatencyMS:        1,
		ExpiryMS:         60000,
		Snapshot: factor.SnapshotSpec{
			Universe:          factor.Universe{Version: "u-fa06", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true},
			SIDMap:            symbols,
			Schemas:           map[string]string{"kline": "s1", "tick": "s1"},
			SourceVersions:    map[string]string{"kline": "v1", "tick": "v1"},
			VisibilityPolicy:  "available-at",
			AdjustmentVersion: "raw",
		},
		Factor:     research.MomentumVolConfig{Source: "kline", Field: "close", TimeFrame: "1h", Window: 2},
		Manifest:   research.ManifestSpec{CodeRevision: "test", Currency: "USDT", Portfolio: research.PortfolioDefinition{K: nAssets, LongNotional: 1.0, ShortNotional: 0, Mode: factor.Full}, Costs: research.CostSpec{FundingPolicy: "explicit-zero"}},
		StrategyID: "fa06-strat",
		AccountID:  "fa06-acc",
		InitialNAV: 10000,
		Prices:     PriceStream{Source: "tick", TimeFrame: "event", Field: "price"},
	}
}

func TestFA06RegisterFactorExplicitVolatilityAndBetaColumns(t *testing.T) {
	if err := RegisterFactor("fa06-explicit-vol-beta-v1", func(c FactorContext) (FactorSpec, error) {
		closeNode := c.Close()
		ret1 := factor.Return(closeNode, 1)
		// Volatility proportional to close level? No, compute real rolling StdDev(ret1, 4, 1),
		// plus cross-sectionally distinct deterministic volatility & beta outputs to verify exact binding!
		return FactorSpec{Outputs: map[string]*factor.Node{
			"mom":        factor.Return(closeNode, 1),
			"volatility": factor.StdDev(ret1, 4, 1),
			"beta":       factor.Constant(2.0, c.TimeFrame),
		}}, c.Params.Err()
	}); err != nil {
		t.Fatal(err)
	}

	// Verify defaultComboColumns excludes reserved "volatility" and "beta" so score is not polluted!
	_, combo, err := CompileDefinition(Config{Definition: "fa06-explicit-vol-beta-v1", Factor: research.MomentumVolConfig{TimeFrame: "1h"}})
	if err != nil || len(combo.Columns) != 1 || combo.Columns[0] != "mom" {
		t.Fatalf("expected combo.Columns=[mom] excluding volatility/beta, got %+v err=%v", combo, err)
	}

	// Build 3 assets where returns over each 4-bar cycle have exact std ratio 1 : 2 : 4:
	// Patterns around +0.02 drift:
	// SID 1: +0.02 +/- 0.005 -> low vol
	// SID 2: +0.02 +/- 0.010 -> medium vol
	// SID 3: +0.02 +/- 0.020 -> high vol
	patterns := [][3]float64{
		{+1, +1, +1},
		{-1, +1, -1},
		{+1, -1, -1},
		{-1, -1, +1},
	}
	amps := [3]float64{0.005, 0.010, 0.020}
	prices := make([][]float64, 10)
	prices[0] = []float64{100, 100, 100}
	for step := 1; step < len(prices); step++ {
		prev := prices[step-1]
		pat := patterns[(step-1)%4]
		row := make([]float64, 3)
		for j := 0; j < 3; j++ {
			// Add tiny ranking separation on final bar so score is non-constant while keeping std ratio ~ 1:2:4
			r := 0.02 + amps[j]*pat[j] + float64(3-j)*1e-6
			row[j] = prev[j] * (1 + r)
		}
		prices[step] = row
	}
	const hour int64 = 3600000

	// 1. inverse-volatility + beta_cap=0.5 (without setting PolicyContext in Go, and even during bars 2..4 while StdDev(4) warms up!)
	invCfg := buildOrthogonalArchive(t, prices)
	invCfg.Definition = "fa06-explicit-vol-beta-v1"
	invCfg.Manifest.Portfolio.K = 3
	invCfg.Manifest.Portfolio.Allocation = &factor.AllocationConfig{
		Method:  "inverse-volatility",
		BetaCap: 0.5, // Since beta=2.0 for all 3 assets, total weight 1.0 has portfolio beta=2.0 -> scaled to total weight 0.25!
	}
	capInv := newFA06Capture()
	resInv, err := Run(context.Background(), invCfg, nil, capInv)
	if err != nil {
		t.Fatalf("inverse-volatility + beta_cap run failed: %v", err)
	}
	if resInv.TargetsAccepted == 0 {
		t.Fatalf("expected accepted targets, got %+v", resInv)
	}
	lastWeights := capInv.decisions[9*hour]
	// Total weight must be 0.25 (beta = 0.25 * 2.0 = 0.5 == BetaCap), in 4 : 2 : 1 proportions!
	totalW := lastWeights[1] + lastWeights[2] + lastWeights[3]
	if math.Abs(totalW-0.25) > 1e-6 {
		t.Fatalf("beta_cap=0.5 with beta=2.0 must cap gross long weight at 0.25, got %g (%+v)", totalW, lastWeights)
	}
	if math.Abs(lastWeights[1]/lastWeights[2]-2.0) > 1e-3 || math.Abs(lastWeights[2]/lastWeights[3]-2.0) > 1e-3 {
		t.Fatalf("expected 1/sigma weight ratios 2:1, got %+v", lastWeights)
	}

	// 2. vol-target = 0.003: caps each asset at VolTarget / (sigma_i * N) when tighter than 1/sigma share!
	vtCfg := buildOrthogonalArchive(t, prices)
	vtCfg.Definition = "fa06-explicit-vol-beta-v1"
	vtCfg.Manifest.Portfolio.K = 3
	vtCfg.Manifest.Portfolio.Allocation = &factor.AllocationConfig{
		Method:    "vol-target",
		VolTarget: 0.003,
	}
	capVT := newFA06Capture()
	if _, err := Run(context.Background(), vtCfg, nil, capVT); err != nil {
		t.Fatalf("vol-target run failed: %v", err)
	}
	wVT := capVT.decisions[9*hour]
	if !(wVT[1] > wVT[2] && wVT[2] > wVT[3] && wVT[1]+wVT[2]+wVT[3] < 0.5) {
		t.Fatalf("vol-target=0.003 did not scale down high-vol exposure properly: %+v", wVT)
	}
}

func TestFA06ScoreOnlyRegisterFactorZeroCodeAllCovarianceAndVolMethods(t *testing.T) {
	// Plain score-only strategy: NO "volatility" or "beta" output column!
	if err := RegisterFactor("fa06-plain-score-only-v1", func(c FactorContext) (FactorSpec, error) {
		// Deterministic distinct scores: close / lag(close, 4) - 1, plus tiny close tie-breaker
		return FactorSpec{Outputs: map[string]*factor.Node{
			"score": factor.Return(c.Close(), 4),
		}}, c.Params.Err()
	}); err != nil {
		t.Fatal(err)
	}

	// Construct 4 orthogonal Hadamard return series over 8-bar windows (total 13 prices = 12 return bars):
	// Pairwise sample covariance over any multiple of 4 bars is EXACTLY 0, with exact volatility ratios 1 : 2 : 4 : 8!
	// Wait: with 3 orthogonal Walsh vectors in R^4 (+ mean vector [1,1,1,1]), 3 zero-mean orthogonal vectors exist in R^4;
	// in R^8 (8-bar RiskWindow!), we have 7 mutually orthogonal zero-mean Walsh vectors, so all 4 assets have EXACT zero pairwise sample covariance!
	walsh8 := [8][4]float64{
		{+1, +1, +1, +1},
		{-1, +1, -1, +1},
		{+1, -1, -1, +1},
		{-1, -1, +1, +1},
		{+1, +1, +1, -1},
		{-1, +1, -1, -1},
		{+1, -1, -1, -1},
		{-1, -1, +1, -1},
	}
	sigmas := [4]float64{0.005, 0.010, 0.020, 0.040} // exact 1 : 2 : 4 : 8 ratio!
	drifts := [4]float64{0.014, 0.013, 0.012, 0.011} // constant per-asset drift does NOT affect demeaned sample covariance!
	const nBars = 14
	prices := make([][]float64, nBars)
	prices[0] = []float64{100, 100, 100, 100}
	for step := 1; step < nBars; step++ {
		prev := prices[step-1]
		w := walsh8[(step-1)%8]
		row := make([]float64, 4)
		for j := 0; j < 4; j++ {
			r := drifts[j] + sigmas[j]*w[j]
			row[j] = prev[j] * (1 + r)
		}
		prices[step] = row
	}
	const hour int64 = 3600000
	evalAt := int64(9) * hour // At bar 9, trailing 8 returns (bars 2..9) form one exact 8-row Walsh orthogonal block!

	methods := []string{"inverse-volatility", "erc", "risk-parity", "hrp", "min-variance", "mean-variance"}
	resultsByMethod := map[string]map[int32]float64{}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			cfg := buildOrthogonalArchive(t, prices)
			cfg.Definition = "fa06-plain-score-only-v1"
			// Intentionally omit Policy: "lifecycle-v1" to also verify automatic promotion when Allocation is set!
			cfg.Manifest.Portfolio.K = 4
			cfg.Manifest.Portfolio.LongNotional = 1.0
			cfg.Manifest.Portfolio.ShortNotional = 0.0
			cfg.Manifest.Portfolio.Allocation = &factor.AllocationConfig{
				Method:     method,
				RiskWindow: 8,
			}
			capOut := newFA06Capture()
			res, err := Run(context.Background(), cfg, nil, capOut)
			if err != nil {
				t.Fatalf("%s zero-code run failed: %v", method, err)
			}
			if res.TargetsAccepted == 0 {
				t.Fatalf("%s accepted 0 targets: %+v", method, res)
			}
			w := capOut.decisions[evalAt]
			if len(w) != 4 {
				t.Fatalf("%s at bar 9 expected 4 weights, got %+v", method, w)
			}
			// Every risk method must strictly assign lower weight to higher-volatility assets: w[1] > w[2] > w[3] > w[4]!
			if !(w[1] > w[2] && w[2] > w[3] && w[3] > w[4]) {
				t.Fatalf("%s failed monotonicity w1>w2>w3>w4 on sigmas 1:2:4:8: %+v", method, w)
			}
			sum := w[1] + w[2] + w[3] + w[4]
			if math.Abs(sum-1.0) > 1e-6 {
				t.Fatalf("%s weights should sum to 1.0, got %g (%+v)", method, sum, w)
			}
			resultsByMethod[method] = w
		})
	}

	// Verify exact analytical identities on the orthogonal 8-bar block:
	// 1) inverse-volatility, erc, and risk-parity must all equal [8/15, 4/15, 2/15, 1/15]!
	wantInvVol := map[int32]float64{1: 8.0 / 15.0, 2: 4.0 / 15.0, 3: 2.0 / 15.0, 4: 1.0 / 15.0}
	for _, m := range []string{"inverse-volatility", "erc", "risk-parity"} {
		got := resultsByMethod[m]
		for sid, want := range wantInvVol {
			if math.Abs(got[sid]-want) > 1e-9 {
				t.Fatalf("%s orthogonal SID %d got %.12f want %.12f (1/sigma)", m, sid, got[sid], want)
			}
		}
	}

	// 2) min-variance and hrp on 4 orthogonal assets must both equal [64/85, 16/85, 4/85, 1/85] (1/sigma^2)!
	wantInvVar := map[int32]float64{1: 64.0 / 85.0, 2: 16.0 / 85.0, 3: 4.0 / 85.0, 4: 1.0 / 85.0}
	for _, m := range []string{"min-variance", "hrp"} {
		got := resultsByMethod[m]
		for sid, want := range wantInvVar {
			if math.Abs(got[sid]-want) > 1e-9 {
				t.Fatalf("%s orthogonal SID %d got %.12f want %.12f (1/sigma^2)", m, sid, got[sid], want)
			}
		}
	}

	// 3) Replay vs Live parity for covariance allocation (HRP):
	liveCfg := buildOrthogonalArchive(t, prices)
	liveCfg.Definition = "fa06-plain-score-only-v1"
	liveCfg.Manifest.Labels = nil
	liveCfg.Manifest.Portfolio.K = 4
	liveCfg.Manifest.Portfolio.LongNotional = 1.0
	liveCfg.Manifest.Portfolio.ShortNotional = 0.0
	liveCfg.Manifest.Portfolio.Allocation = &factor.AllocationConfig{Method: "hrp", RiskWindow: 8}

	replayCap := newFA06Capture()
	if _, err := Run(context.Background(), liveCfg, nil, replayCap); err != nil {
		t.Fatal(err)
	}
	book, _ := backtest.NewBook(liveCfg.InitialNAV)
	sink := &bookPolicySink{book: book}
	now := hour
	liveOut := newFA06Capture()
	live, err := NewLive(liveCfg, sink, func() int64 { return now }, liveOut)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { live.Stop(); _ = live.Join(context.Background()) })
	for barIdx, row := range prices {
		at := int64(barIdx+1) * hour
		now = at
		for i, p := range row {
			sid := int32(i + 1)
			rec := factor.VersionRecord{
				Series:        orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{"close": p}},
				EventTime:     at,
				AvailableAt:   at,
				IngestedAt:    at,
				Revision:      1,
				SourceVersion: "v1",
			}
			if err := live.Observe(context.Background(), rec); err != nil {
				t.Fatal(err)
			}
		}
		if err := live.Flush(context.Background(), at); err != nil {
			t.Fatal(err)
		}
		now = at + 1
		for i, p := range row {
			sid := int32(i + 1)
			tick := factor.VersionRecord{
				Series:        orm.DataSeries{Source: "tick", TimeFrame: "event", Sid: sid, TimeMS: now, EndMS: now, Closed: true, Values: map[string]any{"price": p}},
				EventTime:     now,
				AvailableAt:   now,
				IngestedAt:    now,
				Revision:      1,
				SourceVersion: "v1",
			}
			if err := live.Observe(context.Background(), tick); err != nil {
				t.Fatal(err)
			}
		}
	}
	for sid := int32(1); sid <= 4; sid++ {
		if math.Abs(replayCap.decisions[evalAt][sid]-liveOut.decisions[evalAt][sid]) > 1e-12 {
			t.Fatalf("replay vs live HRP mismatch at bar 9 SID %d: replay=%g live=%g", sid, replayCap.decisions[evalAt][sid], liveOut.decisions[evalAt][sid])
		}
	}
}
