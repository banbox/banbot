package statistics

import (
	"fmt"
	"math"
)

// BinaryViterbiFilter tracks maximum joint path probabilities, rather than
// marginal filtered state probabilities. Ending states may change as new
// observations arrive; earlier decoded states are not irrevocable online labels.
type BinaryViterbiFilter struct {
	model  BinaryHMM
	scores [2]float64
	count  int
}

func (m BinaryHMM) ViterbiFilter() (*BinaryViterbiFilter, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	return &BinaryViterbiFilter{model: m}, nil
}

func (f *BinaryViterbiFilter) step(symbol int) ([2]int, error) {
	if f == nil || symbol < 0 || symbol > 1 {
		return [2]int{}, fmt.Errorf("statistics: invalid Viterbi observation/filter")
	}
	var next [2]float64
	var back [2]int
	for j := range next {
		if f.count == 0 {
			next[j] = math.Log(f.model.Initial[j])
		} else {
			a := f.scores[0] + math.Log(f.model.Transition[0][j])
			b := f.scores[1] + math.Log(f.model.Transition[1][j])
			next[j] = a
			if b > a {
				next[j], back[j] = b, 1
			}
		}
		next[j] += math.Log(f.model.Emission[j][symbol])
	}
	if math.IsInf(next[0], -1) && math.IsInf(next[1], -1) {
		return back, fmt.Errorf("statistics: impossible Viterbi sequence")
	}
	f.scores = next
	f.count++
	return back, nil
}

func (f *BinaryViterbiFilter) Observe(symbol int) (int, float64, error) {
	if _, err := f.step(symbol); err != nil {
		return 0, 0, err
	}
	state := 0
	if f.scores[1] > f.scores[0] {
		state = 1
	}
	return state, f.scores[state], nil
}

// Viterbi returns the globally most probable hidden path conditional on exactly
// the supplied observations. It must never decode future observations for a
// historical trading decision. Ties choose the lower state index.
func (m BinaryHMM) Viterbi(symbols []int) ([]int, float64, error) {
	if len(symbols) == 0 {
		return nil, 0, fmt.Errorf("statistics: empty Viterbi sequence")
	}
	f, err := m.ViterbiFilter()
	if err != nil {
		return nil, 0, err
	}
	back := make([][2]int, len(symbols))
	for i, symbol := range symbols {
		back[i], err = f.step(symbol)
		if err != nil {
			return nil, 0, err
		}
	}
	path := make([]int, len(symbols))
	last := len(path) - 1
	if f.scores[1] > f.scores[0] {
		path[last] = 1
	}
	probability := f.scores[path[last]]
	for i := last; i > 0; i-- {
		path[i-1] = back[i][path[i]]
	}
	return path, probability, nil
}
