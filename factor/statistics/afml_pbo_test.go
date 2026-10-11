package statistics

import (
	"errors"
	"math"
	"testing"
)

func afmlMeanMetric(values []float64) (float64, error) {
	sum := 0.
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values)), nil
}

func TestAFMLCSCVStableAndOverfit(t *testing.T) {
	stable := [][]float64{{2, 0}, {3, 1}, {4, 2}, {5, 3}}
	result, err := CombinatorialSymmetricCV(stable, 4, 6, afmlMeanMetric)
	if err != nil || len(result.Splits) != 6 || result.Probability != 0 {
		t.Fatalf("stable optimum: %+v %v", result, err)
	}
	for _, split := range result.Splits {
		if split.Selected != 0 {
			t.Fatalf("wrong stable optimum: %+v", split)
		}
		afmlMicroNear(t, split.Rank, 2./3, 1e-12)
		afmlMicroNear(t, split.Logit, math.Log(2), 1e-12)
	}
	// Selecting each half's winner yields the loser in its complementary half.
	overfit := [][]float64{{4, 0}, {2, 0}, {0, 4}, {0, 2}}
	bad, err := CombinatorialSymmetricCV(overfit, 2, 2, afmlMeanMetric)
	if err != nil || bad.Probability != 1 || bad.Splits[0].Selected != 0 || bad.Splits[1].Selected != 1 {
		t.Fatalf("overfit matrix: %+v %v", bad, err)
	}
	for _, split := range bad.Splits {
		afmlMicroNear(t, split.Rank, 1./3, 1e-12)
		afmlMicroNear(t, split.Logit, -math.Log(2), 1e-12)
	}
	ties, err := CombinatorialSymmetricCV([][]float64{{1, 1}, {2, 2}}, 2, 2, afmlMeanMetric)
	if err != nil || ties.Probability != 1 || ties.Splits[0].Selected != 0 || ties.Splits[0].Logit != 0 {
		t.Fatalf("median tie convention: %+v %v", ties, err)
	}
}

func TestAFMLCSCVChronologyBudgetAndErrors(t *testing.T) {
	matrix := make([][]float64, 16)
	for i := range matrix {
		matrix[i] = []float64{float64(i + 1), float64(i)}
	}
	chronological := func(values []float64) (float64, error) {
		for i := 1; i < len(values); i++ {
			if values[i] <= values[i-1] {
				t.Fatal("CSCV changed chronology")
			}
		}
		return afmlMeanMetric(values)
	}
	if _, err := CombinatorialSymmetricCV(matrix, 16, 12869, chronological); err == nil {
		t.Fatal("source S16 combination count not budgeted")
	}
	result, err := CombinatorialSymmetricCV(matrix, 16, 12870, chronological)
	if err != nil || len(result.Splits) != 12870 || result.Probability != 0 {
		t.Fatalf("source S16 mesh: splits%d probability%g err%v", len(result.Splits), result.Probability, err)
	}
	for _, input := range [][][]float64{
		nil, {{1}, {2}}, {{1, 2}, {3}}, {{1, 2}, {3, math.NaN()}}, {{1, 2}, {3, 4}, {5, 6}},
	} {
		if _, err := CombinatorialSymmetricCV(input, 2, 2, afmlMeanMetric); err == nil {
			t.Fatal("accepted invalid matrix")
		}
	}
	if _, err := CombinatorialSymmetricCV(matrix, 3, 100, chronological); err == nil {
		t.Fatal("accepted odd groups")
	}
	if _, err := CombinatorialSymmetricCV(matrix, 2, 2, func([]float64) (float64, error) { return math.Inf(1), nil }); err == nil {
		t.Fatal("accepted nonfinite metric")
	}
	expected := errors.New("metric unavailable")
	if _, err := CombinatorialSymmetricCV(matrix, 2, 2, func([]float64) (float64, error) { return 0, expected }); !errors.Is(err, expected) {
		t.Fatalf("metric error lost: %v", err)
	}
}

func TestAFMLCSCVSharpe(t *testing.T) {
	got, err := CSCVSharpe([]float64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	afmlMicroNear(t, got, 2, 0)
	if _, err := CSCVSharpe([]float64{1, 1}); err == nil {
		t.Fatal("constant returns given finite Sharpe")
	}
}
