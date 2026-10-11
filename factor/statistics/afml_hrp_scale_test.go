package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestHRPFiniteCovarianceScaleInvariance(t *testing.T) {
	for _, covariance := range [][][]float64{
		{{1, 0}, {0, 1}},
		{{1, 1}, {1, 1}},
		{{.5, .1, 0}, {.1, .8, .1}, {0, .1, 1}},
	} {
		baseline, err := HierarchicalRiskParity(covariance)
		if err != nil {
			t.Fatal(err)
		}
		for _, scale := range []float64{1e-308, 1e308} {
			scaled := make([][]float64, len(covariance))
			for i, row := range covariance {
				scaled[i] = make([]float64, len(row))
				for j, v := range row {
					scaled[i][j] = v * scale
				}
			}
			result, err := HierarchicalRiskParity(scaled)
			if err != nil {
				t.Fatal(scale, err)
			}
			if !reflect.DeepEqual(result.Order, baseline.Order) {
				t.Fatal("scale changed order", scale, result, baseline)
			}
			for i, w := range result.Weights {
				if !afmlFinite(w) || math.Abs(w-baseline.Weights[i]) > 1e-12 {
					t.Fatal("scale changed allocation", scale, result, baseline)
				}
			}
		}
	}
}
