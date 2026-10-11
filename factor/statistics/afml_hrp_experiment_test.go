package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestHRPExperimentExtremeDimensionsFailBeforeAllocation(t *testing.T) {
	for _, dimensions := range [][2]int{{math.MaxInt, 1}, {1, math.MaxInt}, {math.MaxInt, math.MaxInt}, {-1, 2}} {
		c := DefaultHRPExperimentConfig()
		c.BaseAssets, c.CorrelatedAssets = dimensions[0], dimensions[1]
		if _, err := GenerateHRPData(c, 1); err == nil {
			t.Fatal("overflowing dimensions accepted", dimensions)
		}
	}
}

func TestHRPNumericalExampleSourceDimensionsAndKKT(t *testing.T) {
	r, err := HRPAllocationExample(12345)
	if err != nil {
		t.Fatal(err)
	}
	if r.Observations != 10000 || len(r.Parents) != 5 || len(r.Covariance) != 10 || r.MinimumVariance.KKTResidual > 1e-10 {
		t.Fatalf("invalid numerical example: %+v", r)
	}
	for _, weights := range [][]float64{r.HRP.Weights, r.IVP} {
		variance := 0.
		for i, w := range weights {
			for j, v := range weights {
				variance += w * v * r.Covariance[i][j]
			}
		}
		if variance < r.MinimumVariance.Variance-1e-10 {
			t.Fatal("minimum variance did not bound feasible allocation risk")
		}
	}
	for j, p := range r.Parents {
		rho := r.Covariance[p][j+5] / math.Sqrt(r.Covariance[p][p]*r.Covariance[j+5][j+5])
		if rho < .95 || rho > .99 {
			t.Fatalf("source parent correlation %.6f", rho)
		}
	}
}

func TestHRPExperimentSeedShapeShocksAndTrainCutoff(t *testing.T) {
	c := DefaultHRPExperimentConfig()
	c.Trials = 2
	d, err := GenerateHRPData(c, c.Seed)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := GenerateHRPData(c, c.Seed)
	other, _ := GenerateHRPData(c, c.Seed+1)
	if !reflect.DeepEqual(d, again) || reflect.DeepEqual(d, other) || len(d.Returns) != 520 || len(d.Returns[0]) != 10 || len(d.Shocks) != 4 {
		t.Fatal("seed/shape mismatch")
	}
	for _, s := range d.Shocks {
		if s.Row < 260 || s.Row >= 519 {
			t.Fatal("shock chronology mismatch")
		}
	}
	a, err := EvaluateHRPPath(d.Returns, c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 260; i < len(d.Returns); i++ {
		for j := range d.Returns[i] {
			d.Returns[i][j] *= .5
		}
	}
	b, err := EvaluateHRPPath(d.Returns, c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if !reflect.DeepEqual(a.Allocations[i], b.Allocations[i]) {
			t.Fatal("OOS perturbation changed prior fit")
		}
		if a.Allocations[i].TrainEnd != 260 || a.Allocations[i].OOSStart != 260 || a.Allocations[i].OOSEnd != 282 {
			t.Fatal("wrong cutoff")
		}
	}
	if reflect.DeepEqual(a.Metrics, b.Metrics) {
		t.Fatal("future returns did not affect performance")
	}
	r, err := RunHRPExperiment(c)
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := RunHRPExperiment(c)
	if err != nil || !reflect.DeepEqual(r, repeat) {
		t.Fatal("trial seed reproducibility failed")
	}
	for _, m := range r.Metrics {
		if m.MeanCosts <= 0 || m.MeanTradedNotional < 1 || m.VarianceSourceGross < 0 || m.VarianceNetBuyHold < 0 {
			t.Fatalf("invalid metrics %+v", m)
		}
	}
}
func TestHRPExperimentCostAndBudget(t *testing.T) {
	c := DefaultHRPExperimentConfig()
	c.Trials = 1
	d, _ := GenerateHRPData(c, c.Seed)
	with, err := EvaluateHRPPath(d.Returns, c)
	if err != nil {
		t.Fatal(err)
	}
	c.CostRate = 0
	without, err := EvaluateHRPPath(d.Returns, c)
	if err != nil {
		t.Fatal(err)
	}
	for i := range with.Metrics {
		if with.Metrics[i].SourceGrossReturn != without.Metrics[i].SourceGrossReturn || with.Metrics[i].NetBuyHoldReturn >= without.Metrics[i].NetBuyHoldReturn {
			t.Fatal("fees changed source oracle or failed to reduce net NAV")
		}
	}
	c.Trials = 10001
	if _, err := RunHRPExperiment(c); err == nil {
		t.Fatal("unbounded trials accepted")
	}
	c.Trials = 1
	d.Returns[0][0] = -1
	if _, err := EvaluateHRPPath(d.Returns, c); err == nil {
		t.Fatal("asset ruin accepted")
	}
}

func TestHRPExperimentSelfFinancingEntryFee(t *testing.T) {
	c := DefaultHRPExperimentConfig()
	c.Trials = 1
	c.RebalanceRows = 260
	d, err := GenerateHRPData(c, c.Seed)
	if err != nil {
		t.Fatal(err)
	}
	for i := 260; i < 520; i++ {
		for j := range d.Returns[i] {
			d.Returns[i][j] = 0
		}
	}
	r, err := EvaluateHRPPath(d.Returns, c)
	if err != nil {
		t.Fatal(err)
	}
	fee := c.CostRate / (1 + c.CostRate)
	for _, m := range r.Metrics {
		if math.Abs(m.NetBuyHoldReturn+fee) > 1e-13 || m.SourceGrossReturn != 0 || math.Abs(m.Costs-fee) > 1e-13 || math.Abs(m.TradedNotional-(1-fee)) > 1e-13 {
			t.Fatalf("fee-funded entry identity failed: %+v", m)
		}
	}
}
