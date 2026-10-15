package statistics

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestChanHMMForwardLikelihoodAndOnlinePrefix(t *testing.T) {
	m := BinaryHMM{Initial: [2]float64{.3, .7}, Transition: [2][2]float64{{.6, .4}, {.75, .25}}, Emission: [2][2]float64{{.19, .81}, {.97, .03}}}
	seq := []int{1, 0, 1}
	p, ll, err := m.Filter(seq)
	if err != nil {
		t.Fatal(err)
	}
	brute := 0.0
	terminal := [2]float64{}
	for a := 0; a < 2; a++ {
		for b := 0; b < 2; b++ {
			for c := 0; c < 2; c++ {
				prob := m.Initial[a] * m.Emission[a][1] * m.Transition[a][b] * m.Emission[b][0] * m.Transition[b][c] * m.Emission[c][1]
				brute += prob
				terminal[c] += prob
			}
		}
	}
	if math.Abs(math.Exp(ll)-brute) > 1e-12 || math.Abs(p[0]-terminal[0]/brute) > 1e-12 {
		t.Fatal("forward likelihood disagrees with enumeration")
	}
	prior := m.Initial
	for i, symbol := range seq {
		online, _, err := m.Update(prior, symbol)
		if err != nil {
			t.Fatal(err)
		}
		full, _, _ := m.Filter(seq[:i+1])
		if math.Abs(online[0]-full[0]) > 1e-12 {
			t.Fatal("online differs from prefix terminal posterior")
		}
		prior = m.PredictState(online)
	}
	predict := m.Predict(p)
	if math.Abs(predict[0]+predict[1]-1) > 1e-12 {
		t.Fatal("symbol prediction")
	}
}

func TestChanHMMTrainingLikelihoodAndReproducibility(t *testing.T) {
	r := rand.New(rand.NewSource(823))
	state := 0
	seq := make([]int, 1000)
	for i := range seq {
		if r.Float64() < .05 {
			state = 1 - state
		}
		seq[i] = state
		if r.Float64() < .08 {
			seq[i] = 1 - state
		}
	}
	s := HMMFitSpec{Restarts: 10, MaxIterations: 1000, Tolerance: 1e-7, Seed: 41}
	fit, err := FitBinaryHMM(seq, s)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := fitBinaryHMMRestarts(seq, s, false)
	if err != nil || !reflect.DeepEqual(fit, serial) {
		t.Fatalf("parallel restart scheduling changed the sequential oracle: parallel=%+v serial=%+v err=%v", fit, serial, err)
	}
	same, err := FitBinaryHMM(seq, s)
	if err != nil || !reflect.DeepEqual(fit, same) {
		t.Fatal("nondeterministic HMM fit")
	}
	iid := BinaryHMM{Initial: [2]float64{.5, .5}, Transition: [2][2]float64{{.5, .5}, {.5, .5}}, Emission: [2][2]float64{{.5, .5}, {.5, .5}}}
	_, ll, _ := iid.Filter(seq)
	if !fit.Converged || fit.LogLikelihood <= ll+100 {
		t.Fatalf("regime structure not learned %+v iid=%g", fit, ll)
	}
	if _, err = FitBinaryHMM([]int{0, 1, 2, 0}, s); err == nil {
		t.Fatal("bad symbols accepted")
	}
	s.MaxIterations = 1
	s.Tolerance = 1e-15
	if _, err = FitBinaryHMM(seq, s); err == nil {
		t.Fatal("nonconvergence accepted")
	}
}

func BenchmarkFitBinaryHMMParallel(b *testing.B) {
	seq := benchmarkHMMSequence()
	s := HMMFitSpec{Restarts: 10, MaxIterations: 300, Tolerance: 1e-7, Seed: 41}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := FitBinaryHMM(seq, s); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFitBinaryHMMSequentialOracle(b *testing.B) {
	seq := benchmarkHMMSequence()
	s := HMMFitSpec{Restarts: 10, MaxIterations: 300, Tolerance: 1e-7, Seed: 41}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fitBinaryHMMRestarts(seq, s, false); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkHMMSequence() []int {
	r := rand.New(rand.NewSource(823))
	state := 0
	seq := make([]int, 1000)
	for i := range seq {
		if r.Float64() < .05 {
			state = 1 - state
		}
		seq[i] = state
		if r.Float64() < .08 {
			seq[i] = 1 - state
		}
	}
	return seq
}
