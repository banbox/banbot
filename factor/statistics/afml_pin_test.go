package statistics

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestPINLikelihoodAndBayesianQuotes(t *testing.T) {
	m := PINModel{.5, .5, 4, 2}
	ll, err := m.LogLikelihood([]PINCounts{{1, 2}})
	// Independent direct Poisson mixture with factorials.
	poisson := func(n int, lambda float64) float64 {
		f, _ := math.Lgamma(float64(n) + 1)
		return math.Exp(float64(n)*math.Log(lambda) - lambda - f)
	}
	want := math.Log(.5*poisson(1, 2)*poisson(2, 2) + .25*poisson(1, 2)*poisson(2, 6) + .25*poisson(1, 6)*poisson(2, 2))
	if err != nil || math.Abs(ll-want) > 1e-12 {
		t.Fatal(ll, want, err)
	}
	p, _ := m.Posterior()
	bid, ask, expected, err := p.Quotes(90, 100, 110)
	if err != nil || expected != 100 || math.Abs(bid-96.66666666666667) > 1e-12 || math.Abs(ask-103.33333333333333) > 1e-12 {
		t.Fatal(bid, ask, expected, err)
	}
	prob, _ := m.Probability()
	if prob != 1./3 {
		t.Fatal(prob)
	}
	updated, err := p.Update(PINCounts{1, 0}, .1)
	if err != nil {
		t.Fatal(err)
	}
	if updated.LogProbabilities[2] <= updated.LogProbabilities[1] {
		t.Fatal("buy did not favor good news", updated)
	}
	// Splitting the same count/time evidence into successive increments must
	// leave regime probabilities unchanged; factorials cancel in Bayes ratios.
	a, _ := p.Update(PINCounts{2, 1}, .4)
	b, _ := p.Update(PINCounts{1, 0}, .1)
	b, _ = b.Update(PINCounts{1, 1}, .3)
	for i := range a.LogProbabilities {
		if math.Abs(a.LogProbabilities[i]-b.LogProbabilities[i]) > 1e-12 {
			t.Fatal(a, b)
		}
	}
	quiet, _ := p.Update(PINCounts{}, 1)
	if quiet.LogProbabilities[0] <= p.LogProbabilities[0] {
		t.Fatal("no-trade survival ignored")
	}
	if p != (PINPosterior{m, [3]float64{math.Log(.5), math.Log(.25), math.Log(.25)}}) {
		t.Fatal("posterior input mutated")
	}
}

func TestPINStableCountsFitAndBudgets(t *testing.T) {
	m := PINModel{.4, .3, 20, 8}
	if ll, err := m.LogLikelihood([]PINCounts{{10000, 12000}}); err != nil || !afmlFinite(ll) {
		t.Fatal(ll, err)
	}
	rng := rand.New(rand.NewSource(196))
	poisson := func(lambda float64) int {
		threshold := math.Exp(-lambda)
		p, n := 1., 0
		for p > threshold {
			p *= rng.Float64()
			n++
		}
		return n - 1
	}
	data := make([]PINCounts, 600)
	for i := range data {
		buy, sell := m.Epsilon, m.Epsilon
		if rng.Float64() < m.Alpha {
			if rng.Float64() < m.Delta {
				sell += m.Mu
			} else {
				buy += m.Mu
			}
		}
		data[i] = PINCounts{poisson(buy), poisson(sell)}
	}
	fit, err := FitPIN(data, PINFitSpec{16000, 4000, 4 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	baseline, _ := m.LogLikelihood(data)
	if fit.LogLikelihood < baseline-1e-5 || math.Abs(fit.Model.Mu-20) > 3 || math.Abs(fit.Model.Epsilon-8) > 1 || math.Abs(fit.Model.Alpha-.4) > .08 || math.Abs(fit.Model.Delta-.3) > .12 {
		t.Fatal(fit, baseline)
	}
	if fit.Evaluations > 16000 || fit.ConvergedStarts == 0 {
		t.Fatal("evaluation budget", fit)
	}
	if _, err := FitPIN(data, PINFitSpec{40, 1, time.Second}); err == nil {
		t.Fatal("unconverged fit accepted")
	}
	for _, model := range []PINModel{{-1, .5, 1, 1}, {.5, 2, 1, 1}, {.5, .5, -1, 1}, {.5, .5, 1, 0}} {
		if _, err := model.Probability(); err == nil {
			t.Fatal("invalid model", model)
		}
	}
	p, _ := m.Posterior()
	p.LogProbabilities = [3]float64{}
	if _, err := p.Update(PINCounts{}, 1); err == nil {
		t.Fatal("unnormalized checkpoint accepted")
	}
	if _, err := m.LogLikelihood([]PINCounts{{-1, 0}}); err == nil {
		t.Fatal("negative counts accepted")
	}
}
