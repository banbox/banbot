package statistics

import "testing"

func TestMaximumBipartiteFlowRequiresResidualRerouting(t *testing.T) {
	// Initial greedy L0->R0 blocks L1. Residual rerouting must replace it with
	// L0->R1 and L1->R0 to reach the maximum of two.
	r, err := MaximumBipartiteFlow([]float64{1, 1}, []float64{1, 1}, []BipartiteEdge{{0, 0}, {0, 1}, {1, 0}})
	if err != nil || r.Quantity != 2 || len(r.Allocations) != 2 {
		t.Fatal(r, err)
	}
	for _, allocation := range r.Allocations {
		if allocation.Left == 0 && allocation.Right == 0 {
			t.Fatal("greedy allocation not rerouted", r)
		}
	}
	r, err = MaximumBipartiteFlow([]float64{.5, 2}, []float64{1, 1}, []BipartiteEdge{{0, 0}, {1, 0}, {1, 1}})
	if err != nil || r.Quantity != 2 {
		t.Fatal("fractional capacities", r, err)
	}
	if _, err := MaximumBipartiteFlow([]float64{1}, []float64{1}, []BipartiteEdge{{0, 0}, {0, 0}}); err == nil {
		t.Fatal("duplicate edges accepted")
	}
}
