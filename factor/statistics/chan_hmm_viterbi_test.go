package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestChanViterbiExhaustiveAndOnlinePrefix(t *testing.T) {
	m := BinaryHMM{Initial: [2]float64{.7, .3}, Transition: [2][2]float64{{.9, .1}, {.2, .8}}, Emission: [2][2]float64{{.6, .4}, {.1, .9}}}
	symbols := []int{0, 1, 1, 1, 0, 1}
	filter, err := m.ViterbiFilter()
	if err != nil {
		t.Fatal(err)
	}
	for n, symbol := range symbols {
		path, logp, err := m.Viterbi(symbols[:n+1])
		if err != nil {
			t.Fatal(err)
		}
		best, bestp := []int(nil), -1.0
		for mask := 0; mask < 1<<(n+1); mask++ {
			candidate := make([]int, n+1)
			for j := range candidate {
				candidate[j] = (mask >> (n - j)) & 1
			}
			p := m.Initial[candidate[0]] * m.Emission[candidate[0]][symbols[0]]
			for j := 1; j <= n; j++ {
				p *= m.Transition[candidate[j-1]][candidate[j]] * m.Emission[candidate[j]][symbols[j]]
			}
			if p > bestp {
				best, bestp = candidate, p
			}
		}
		if !reflect.DeepEqual(path, best) || math.Abs(logp-math.Log(bestp)) > 1e-12 {
			t.Fatalf("Viterbi not maximum joint path %v vs%v", path, best)
		}
		state, online, err := filter.Observe(symbol)
		if err != nil || state != path[n] || math.Abs(online-logp) > 1e-12 {
			t.Fatal("online path ending state differs")
		}
	}
	if _, _, err := m.Viterbi([]int{2}); err == nil {
		t.Fatal("invalid symbol accepted")
	}
	if _, _, err := m.Viterbi(nil); err == nil {
		t.Fatal("empty sequence accepted")
	}
}
