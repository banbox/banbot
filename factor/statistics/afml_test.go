package statistics

import (
	"math"
	"reflect"
	"testing"
)

func afmlNear(t *testing.T, got, want float64) {
	t.Helper()
	if !afmlFinite(got) || !afmlFinite(want) || math.Abs(got-want) > 1e-10 {
		t.Fatalf("got %.14g want %.14g", got, want)
	}
}

func TestAFMLBarrierMaturedLabelIgnoresMalformedSuffix(t *testing.T) {
	prefix, err := TripleBarrierLabel([]float64{100, 110}, 0, -1, .05, 1, 1, 1, false)
	if err != nil || !prefix.Resolved || prefix.End != 1 {
		t.Fatalf("valid first-touch prefix: %+v %v", prefix, err)
	}
	for _, suffix := range []float64{math.NaN(), math.Inf(1), 0, -1} {
		got, err := TripleBarrierLabel([]float64{100, 110, suffix}, 0, -1, .05, 1, 1, 1, false)
		if err != nil || got != prefix {
			t.Fatalf("suffix changed matured label: %+v %v", got, err)
		}
		if _, err = TripleBarrierLabel([]float64{100, suffix, 110}, 0, -1, .05, 1, 1, 1, false); err == nil {
			t.Fatal("invalid visited close accepted")
		}
	}
}

func TestAFMLCUSUMStrictCrossingsAndPrefix(t *testing.T) {
	x := []float64{0, 1, 2, 1, 0, -1, 0, 1, 2}
	got, err := CUSUMEvents(x, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Equality at 2 does not trigger. Negative run crosses -2 at index 5.
	if !reflect.DeepEqual(got, []int{5, 8}) {
		t.Fatal(got)
	}
	prefix, _ := CUSUMEvents(x[:6], 2)
	if !reflect.DeepEqual(prefix, []int{5}) {
		t.Fatal(prefix)
	}
	if _, err = CUSUMEvents(x, 0); err == nil {
		t.Fatal("zero threshold accepted")
	}
	if _, err = CUSUMEvents([]float64{0, math.NaN()}, 1); err == nil {
		t.Fatal("NaN accepted")
	}
}

func TestAFMLTripleBarrierSourceStrictAndMeta(t *testing.T) {
	prices := []float64{100, 101, 102, 97, 104}
	label, err := TripleBarrierLabel(prices, 0, 4, .01, 1, 1, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	// Binary .01 is slightly below the 101/100-1 result, so use an exactly
	// representable target to verify the source's strict barrier inequality.
	strict, err := TripleBarrierLabel([]float64{100, 125, 126}, 0, 2, .25, 1, 1, 1, false)
	if err != nil || strict.End != 2 || strict.Barrier != "profit" {
		t.Fatalf("%+v %v", strict, err)
	}
	if label.Label != 1 || !label.Resolved {
		t.Fatal(label)
	}
	short, err := TripleBarrierLabel([]float64{100, 90, 80}, 0, 2, .15, 1, 2, -1, true)
	if err != nil || short.Label != 1 || short.Barrier != "profit" {
		t.Fatalf("%+v %v", short, err)
	}
	afmlNear(t, short.SideReturn, .2)
	expiry, err := TripleBarrierLabel([]float64{100, 95, 98}, 0, 2, .1, 0, 2, 1, true)
	if err != nil || expiry.Label != 0 || expiry.Barrier != "vertical" {
		t.Fatalf("%+v %v", expiry, err)
	}
	unresolved, err := TripleBarrierLabel([]float64{100, 101}, 0, -1, .1, 0, 0, 1, true)
	if err != nil || unresolved.Resolved || unresolved.End != -1 {
		t.Fatalf("%+v %v", unresolved, err)
	}
	if _, err = TripleBarrierLabel(prices, 0, 4, .1, 1, 2, 1, false); err == nil {
		t.Fatal("asymmetric learning-side labels accepted")
	}
	if _, err = TripleBarrierLabel([]float64{100, 0}, 0, 1, .1, 1, 1, 1, false); err == nil {
		t.Fatal("zero price accepted")
	}
}

func TestAFMLUniquenessPrintedMatrixAndBootstrap(t *testing.T) {
	// The 4.5.3 printed matrix has six rows and columns [0:2], [2:3], [4:5].
	spans := []EventSpan{{0, 2}, {2, 3}, {4, 5}}
	u, err := EventUniqueness(6, spans)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, u[0], 5./6)
	afmlNear(t, u[1], .75)
	afmlNear(t, u[2], 1)
	draws, err := SequentialBootstrap(6, spans, 100, 42)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := SequentialBootstrap(6, spans, 100, 42)
	if !reflect.DeepEqual(draws, again) {
		t.Fatal("seed not deterministic")
	}
	for _, i := range draws {
		if i < 0 || i >= 3 {
			t.Fatal(i)
		}
	}
	// One event's repeated draws remain valid rather than being suppressed.
	one, _ := SequentialBootstrap(3, []EventSpan{{0, 2}}, 4, 1)
	if !reflect.DeepEqual(one, []int{0, 0, 0, 0}) {
		t.Fatal(one)
	}
	if _, err = EventUniqueness(6, []EventSpan{{2, 6}}); err == nil {
		t.Fatal("out of bounds accepted")
	}
}

func TestAFMLFractionalDifferencePolynomialAndMissing(t *testing.T) {
	w, err := FractionalDiffWeights(2, 1e-5, 10)
	if err != nil || !reflect.DeepEqual(w, []float64{1, -2, 1}) {
		t.Fatalf("%v %v", w, err)
	}
	x := []float64{0, 1, 4, 9, 16}
	d, err := FractionalDifference(x, w)
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(d[0]) || !math.IsNaN(d[1]) {
		t.Fatal(d)
	}
	for _, v := range d[2:] {
		afmlNear(t, v, 2)
	}
	w, _ = FractionalDiffWeights(.5, .05, 100)
	if !reflect.DeepEqual(w, []float64{1, -.5, -.125, -.0625}) {
		t.Fatal(w)
	}
	missing, _ := FractionalDifference([]float64{1, math.NaN(), 3}, []float64{1, -1})
	if !math.IsNaN(missing[1]) {
		t.Fatal(missing)
	}
	afmlNear(t, missing[2], 2)
	identity, _ := FractionalDiffWeights(0, .001, 1)
	if !reflect.DeepEqual(identity, []float64{1}) {
		t.Fatal(identity)
	}
	if _, err = FractionalDiffWeights(.1, 1e-8, 2); err == nil {
		t.Fatal("unconverged kernel accepted")
	}
	prefix, _ := FractionalDifference(x[:4], []float64{1, -1})
	full, _ := FractionalDifference(x, []float64{1, -1})
	for i := 1; i < len(prefix); i++ {
		afmlNear(t, prefix[i], full[i])
	}
}

func TestAFMLPurgedFoldsAndCPCVPaths(t *testing.T) {
	spans := make([]EventSpan, 12)
	for i := range spans {
		spans[i] = EventSpan{i * 3, i*3 + 3}
	}
	folds, err := PurgedKFold(spans, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range folds {
		for _, i := range s.Train {
			for _, j := range s.Test {
				a, b := spans[i], spans[j]
				if a.Start <= b.End && a.End >= b.Start {
					t.Fatalf("overlap train=%d test=%d", i, j)
				}
			}
		}
	}
	if !reflect.DeepEqual(folds[0].Train, []int{5, 6, 7, 8, 9, 10, 11}) {
		t.Fatal(folds[0])
	}
	plan, err := CombinatorialPurgedCV(spans, 6, 2, 1, 15)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Splits) != 15 || len(plan.Paths) != 5 {
		t.Fatalf("%d splits %d paths", len(plan.Splits), len(plan.Paths))
	}
	for g := 0; g < 6; g++ {
		seen := map[int]bool{}
		for _, path := range plan.Paths {
			split := path[g]
			if seen[split] {
				t.Fatal("test occurrence reused")
			}
			seen[split] = true
			found := false
			for _, tg := range plan.Splits[split].TestGroups {
				found = found || tg == g
			}
			if !found {
				t.Fatal("path references wrong group")
			}
		}
	}
	if _, err = CombinatorialPurgedCV(spans, 6, 2, 0, 14); err == nil {
		t.Fatal("split budget ignored")
	}
	remainder := append(spans, EventSpan{36, 37})
	p, _ := CombinatorialPurgedCV(remainder, 6, 1, 0, 6)
	if len(p.Groups[5]) != 3 {
		t.Fatal(p.Groups)
	}
}

func TestAFMLHRPDiagonalSingularAndInvalid(t *testing.T) {
	r, err := HierarchicalRiskParity([][]float64{{1, 0}, {0, 4}})
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, r.Weights[0], .8)
	afmlNear(t, r.Weights[1], .2)
	singular := [][]float64{{1, 1, 0}, {1, 1, 0}, {0, 0, 1}}
	r, err = HierarchicalRiskParity(singular)
	if err != nil {
		t.Fatal(err)
	}
	total := 0.
	for _, w := range r.Weights {
		if w < 0 || !afmlFinite(w) {
			t.Fatal(r)
		}
		total += w
	}
	afmlNear(t, total, 1)
	again, _ := HierarchicalRiskParity(singular)
	if !reflect.DeepEqual(r, again) {
		t.Fatal("unstable clustering ties")
	}
	if _, err = HierarchicalRiskParity([][]float64{{1, 2}, {2, 1}}); err == nil {
		t.Fatal("non PSD accepted")
	}
	if _, err = HierarchicalRiskParity([][]float64{{0, 0}, {0, 1}}); err == nil {
		t.Fatal("zero marginal variance accepted")
	}
}
