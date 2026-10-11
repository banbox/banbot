package statistics

import (
	"math"
	"testing"
)

func TestAFMLAttributedReturnWeights(t *testing.T) {
	// Log changes .1,-.1,.2. Shared middle change contributes -.05 to
	// each event. Abs(sum) gives .05,.15, unlike sum(abs) .15,.25.
	w, err := AttributedReturnWeights([]float64{1, math.Exp(.1), 1, math.Exp(.2)}, []EventSpan{{1, 2}, {2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, w[0], .5)
	afmlNear(t, w[1], 1.5)
	if _, err = AttributedReturnWeights([]float64{1, 1}, []EventSpan{{0, 1}}); err == nil {
		t.Fatal("zero return mass accepted")
	}
	if _, err = AttributedReturnWeights([]float64{1, 0}, []EventSpan{{0, 1}}); err == nil {
		t.Fatal("zero price accepted")
	}
}

func TestAFMLUniquenessTimeDecay(t *testing.T) {
	for _, tc := range []struct {
		c    float64
		want []float64
	}{{1, []float64{1, 1, 1}}, {.5, []float64{.625, .75, 1}}, {-.5, []float64{0, 0, 1}}} {
		got, err := UniquenessTimeDecay([]float64{1, 1, 2}, tc.c)
		if err != nil {
			t.Fatal(err)
		}
		for i := range got {
			afmlNear(t, got[i], tc.want[i])
		}
	}
	for _, c := range []float64{-1, 2, math.NaN()} {
		if _, err := UniquenessTimeDecay([]float64{1}, c); err == nil {
			t.Fatal("invalid decay accepted")
		}
	}
	if _, err := UniquenessTimeDecay([]float64{0, 0}, 0); err == nil {
		t.Fatal("zero uniqueness accepted")
	}
}

func TestAFMLPCARiskWeights(t *testing.T) {
	cov := [][]float64{{4, 0}, {0, 1}}
	w, err := PCARiskWeights(cov, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, w[0], 0)
	afmlNear(t, w[1], 2)
	w, err = PCARiskWeights(cov, []float64{.25, .75}, 2)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, w[0], .5)
	afmlNear(t, w[1], math.Sqrt(3))
	afmlNear(t, 4*w[0]*w[0]+w[1]*w[1], 4)
	if _, err = PCARiskWeights(cov, []float64{.3, .3}, 2); err == nil {
		t.Fatal("invalid risk shares accepted")
	}
	if _, err = PCARiskWeights([][]float64{{1, 1}, {1, 1}}, nil, 1); err == nil {
		t.Fatal("zero-variance allocated component accepted")
	}
	if _, err = PCARiskWeights([][]float64{{1, 2}, {2, 1}}, []float64{1, 0}, 1); err == nil {
		t.Fatal("indefinite covariance accepted")
	}
}
