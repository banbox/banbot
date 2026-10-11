package statistics

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
)

// BinaryHMM has two hidden states and symbols 0 (down) and 1 (up). Initial
// probabilities refer to the state emitting the first observation.
type BinaryHMM struct {
	Initial    [2]float64
	Transition [2][2]float64
	Emission   [2][2]float64
}

func (m BinaryHMM) validate() error {
	for _, row := range [][2]float64{m.Initial, m.Transition[0], m.Transition[1], m.Emission[0], m.Emission[1]} {
		if !finite(row[0]) || !finite(row[1]) || row[0] < 0 || row[1] < 0 || math.Abs(row[0]+row[1]-1) > 1e-10 {
			return fmt.Errorf("statistics: invalid HMM probabilities")
		}
	}
	return nil
}

// Update uses the supplied prior on the current emitting state. PredictState
// advances a filtered posterior to the next state; Predict returns its symbols.
func (m BinaryHMM) Update(prior [2]float64, symbol int) ([2]float64, float64, error) {
	if err := m.validate(); err != nil {
		return [2]float64{}, 0, err
	}
	if symbol < 0 || symbol > 1 || prior[0] < 0 || prior[1] < 0 || !finite(prior[0]) || !finite(prior[1]) || math.Abs(prior[0]+prior[1]-1) > 1e-10 {
		return [2]float64{}, 0, fmt.Errorf("statistics: invalid HMM observation/prior")
	}
	p := [2]float64{prior[0] * m.Emission[0][symbol], prior[1] * m.Emission[1][symbol]}
	scale := p[0] + p[1]
	if scale <= 0 || !finite(scale) {
		return p, 0, fmt.Errorf("statistics: impossible HMM observation")
	}
	p[0] /= scale
	p[1] /= scale
	return p, math.Log(scale), nil
}
func (m BinaryHMM) PredictState(p [2]float64) [2]float64 {
	return [2]float64{p[0]*m.Transition[0][0] + p[1]*m.Transition[1][0], p[0]*m.Transition[0][1] + p[1]*m.Transition[1][1]}
}
func (m BinaryHMM) Predict(p [2]float64) [2]float64 {
	next := m.PredictState(p)
	return [2]float64{next[0]*m.Emission[0][0] + next[1]*m.Emission[1][0], next[0]*m.Emission[0][1] + next[1]*m.Emission[1][1]}
}
func (m BinaryHMM) Filter(symbols []int) ([2]float64, float64, error) {
	posterior := m.Initial
	logL := 0.0
	for i, symbol := range symbols {
		if i > 0 {
			posterior = m.PredictState(posterior)
		}
		p, l, err := m.Update(posterior, symbol)
		if err != nil {
			return p, 0, err
		}
		posterior = p
		logL += l
	}
	return posterior, logL, m.validate()
}

type HMMFitSpec struct {
	Restarts, MaxIterations int
	Tolerance               float64
	Seed                    int64
}
type HMMFit struct {
	Model         BinaryHMM
	LogLikelihood float64
	Iterations    int
	Converged     bool
}

// FitBinaryHMM runs scaled Baum-Welch EM from reproducible random starts and
// selects only converged starts by training likelihood. No pseudocount is added.
func FitBinaryHMM(symbols []int, s HMMFitSpec) (HMMFit, error) {
	return fitBinaryHMMRestarts(symbols, s, true)
}

// fitBinaryHMMRestarts keeps random-start generation and result selection
// sequential while allowing the independent EM fits to run concurrently.
// parallel=false is a test oracle for the original restart ordering.
func fitBinaryHMMRestarts(symbols []int, s HMMFitSpec, parallel bool) (HMMFit, error) {
	if len(symbols) < 4 || s.Restarts < 1 || s.Restarts > 100 || s.MaxIterations < 1 || s.MaxIterations > 100000 || !finite(s.Tolerance) || s.Tolerance <= 0 {
		return HMMFit{}, fmt.Errorf("statistics: invalid HMM fit specification")
	}
	for _, v := range symbols {
		if v < 0 || v > 1 {
			return HMMFit{}, fmt.Errorf("statistics: invalid HMM symbol")
		}
	}
	r := rand.New(rand.NewSource(s.Seed))
	starts := make([]BinaryHMM, s.Restarts)
	for restart := 0; restart < s.Restarts; restart++ {
		row := func() [2]float64 { p := .1 + .8*r.Float64(); return [2]float64{p, 1 - p} }
		starts[restart] = BinaryHMM{Initial: row(), Transition: [2][2]float64{row(), row()}, Emission: [2][2]float64{row(), row()}}
	}
	results := make([]HMMFit, s.Restarts)
	errors := make([]error, s.Restarts)
	run := func(restart int) {
		results[restart], errors[restart] = fitBinaryHMMRestart(symbols, s, starts[restart])
	}
	if !parallel || s.Restarts == 1 {
		for restart := range starts {
			run(restart)
		}
	} else {
		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}
		if workers > 10 {
			workers = 10
		}
		jobs := make(chan int)
		var wg sync.WaitGroup
		wg.Add(workers)
		for worker := 0; worker < workers; worker++ {
			go func() {
				defer wg.Done()
				for restart := range jobs {
					run(restart)
				}
			}()
		}
		for restart := range starts {
			jobs <- restart
		}
		close(jobs)
		wg.Wait()
	}
	best := HMMFit{LogLikelihood: math.Inf(-1)}
	for restart := range results {
		// Errors are inspected in restart order, matching the sequential
		// implementation even when a later worker finishes first.
		if errors[restart] != nil {
			return HMMFit{}, errors[restart]
		}
		if results[restart].Converged && results[restart].LogLikelihood > best.LogLikelihood {
			best = results[restart]
		}
	}
	if !best.Converged {
		return HMMFit{}, fmt.Errorf("statistics: no HMM restart converged within iteration budget")
	}
	return best, nil
}

func fitBinaryHMMRestart(symbols []int, s HMMFitSpec, m BinaryHMM) (HMMFit, error) {
	_, previous, err := m.Filter(symbols)
	if err != nil {
		return HMMFit{}, err
	}
	for iter := 1; iter <= s.MaxIterations; iter++ {
		n := len(symbols)
		alpha := make([][2]float64, n)
		beta := make([][2]float64, n)
		scales := make([]float64, n)
		prior := m.Initial
		for t, symbol := range symbols {
			if t > 0 {
				prior = m.PredictState(alpha[t-1])
			}
			for j := 0; j < 2; j++ {
				alpha[t][j] = prior[j] * m.Emission[j][symbol]
				scales[t] += alpha[t][j]
			}
			if scales[t] <= 0 {
				return HMMFit{}, fmt.Errorf("statistics: impossible HMM training symbol")
			}
			for j := 0; j < 2; j++ {
				alpha[t][j] /= scales[t]
			}
		}
		beta[n-1] = [2]float64{1, 1}
		for t := n - 2; t >= 0; t-- {
			for j := 0; j < 2; j++ {
				for k := 0; k < 2; k++ {
					beta[t][j] += m.Transition[j][k] * m.Emission[k][symbols[t+1]] * beta[t+1][k] / scales[t+1]
				}
			}
		}
		var next BinaryHMM
		var gammaSum, transitionSum [2]float64
		for t, symbol := range symbols {
			gamma := [2]float64{alpha[t][0] * beta[t][0], alpha[t][1] * beta[t][1]}
			den := gamma[0] + gamma[1]
			for j := 0; j < 2; j++ {
				gamma[j] /= den
				next.Emission[j][symbol] += gamma[j]
				gammaSum[j] += gamma[j]
			}
			if t == 0 {
				next.Initial = gamma
			}
			if t+1 < n {
				var xi [2][2]float64
				total := 0.0
				for j := 0; j < 2; j++ {
					for k := 0; k < 2; k++ {
						xi[j][k] = alpha[t][j] * m.Transition[j][k] * m.Emission[k][symbols[t+1]] * beta[t+1][k]
						total += xi[j][k]
					}
				}
				for j := 0; j < 2; j++ {
					for k := 0; k < 2; k++ {
						v := xi[j][k] / total
						next.Transition[j][k] += v
						transitionSum[j] += v
					}
				}
			}
		}
		for j := 0; j < 2; j++ {
			if gammaSum[j] <= 0 || transitionSum[j] <= 0 {
				next.Transition[j] = m.Transition[j]
				next.Emission[j] = m.Emission[j]
				continue
			}
			for k := 0; k < 2; k++ {
				next.Emission[j][k] /= gammaSum[j]
				next.Transition[j][k] /= transitionSum[j]
			}
		}
		_, ll, err := next.Filter(symbols)
		if err != nil {
			return HMMFit{}, err
		}
		if ll+1e-8 < previous {
			return HMMFit{}, fmt.Errorf("statistics: HMM likelihood decreased")
		}
		m = next
		delta := ll - previous
		previous = ll
		if delta <= s.Tolerance*(1+math.Abs(ll)) {
			return HMMFit{Model: m, LogLikelihood: ll, Iterations: iter, Converged: true}, nil
		}
	}
	return HMMFit{}, nil
}
