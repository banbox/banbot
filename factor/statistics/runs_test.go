package statistics

import (
	"math"
	"testing"
)

func TestWaldWolfowitzRunsPermutationMoments(t *testing.T) {
	// Enumerate every equiprobable sequence conditional on3 signs of each
	// kind. This oracle is independent of the closed moment equations.
	var counts []float64
	for mask := 0; mask < 64; mask++ {
		signs, positives := make([]int, 6), 0
		for j := range signs {
			signs[j] = -1
			if mask&(1<<j) != 0 {
				signs[j] = 1
				positives++
			}
		}
		if positives != 3 {
			continue
		}
		r, err := WaldWolfowitzRuns(signs, false)
		if err != nil {
			t.Fatal(err)
		}
		counts = append(counts, float64(r.Runs))
	}
	mean, sigma, err := MeanStd(counts, 0)
	r, e := WaldWolfowitzRuns([]int{1, 1, 1, -1, -1, -1}, false)
	if err != nil || e != nil || len(counts) != 20 || math.Abs(mean-r.Expected) > 1e-12 || math.Abs(sigma*sigma-r.Variance) > 1e-12 || math.Abs(r.Z+2/math.Sqrt(1.2)) > 1e-12 {
		t.Fatal(mean, sigma, r, err, e)
	}
	corrected, _ := WaldWolfowitzRuns([]int{1, 1, 1, -1, -1, -1}, true)
	if math.Abs(corrected.Z+1.5/math.Sqrt(1.2)) > 1e-12 || corrected.NormalPValue <= r.NormalPValue {
		t.Fatal(corrected, r)
	}
}

func TestWaldWolfowitzRunsDegenerateAndInput(t *testing.T) {
	for _, signs := range [][]int{{1}, {1, 1, 1}, {1, -1}} {
		r, err := WaldWolfowitzRuns(signs, false)
		if err != nil || r.HasStandardizedStatistic || r.Variance != 0 {
			t.Fatal(r, err)
		}
	}
	for _, signs := range [][]int{nil, {0}, {1, 2, -1}} {
		if _, err := WaldWolfowitzRuns(signs, false); err == nil {
			t.Fatal("invalid sample accepted", signs)
		}
	}
}
