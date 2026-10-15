package statistics

import (
	"errors"
	"math/big"
	"sort"
)

type PurgedSplit struct{ Train, Test, TestGroups []int }

func afmlCVSpans(spans []EventSpan, groups, embargoBars int) error {
	if groups < 2 || groups > len(spans) || embargoBars < 0 {
		return errors.New("statistics: invalid CV groups or embargo")
	}
	for i, s := range spans {
		if s.Start < 0 || s.End < s.Start || (i > 0 && s.Start <= spans[i-1].Start) {
			return errors.New("statistics: CV requires strictly ordered event starts and valid ends")
		}
	}
	return nil
}

func afmlPurgedSplit(spans []EventSpan, test, testGroups []int, embargoBars int) PurgedSplit {
	split := PurgedSplit{Test: append([]int(nil), test...), TestGroups: append([]int(nil), testGroups...)}
	inTest := make([]bool, len(spans))
	for _, i := range test {
		inTest[i] = true
	}
	for i, s := range spans {
		if inTest[i] {
			continue
		}
		keep := true
		for _, j := range test {
			t := spans[j]
			// Closed intervals: sharing a boundary price is conservatively purged.
			if s.Start <= t.End && s.End >= t.Start {
				keep = false
				break
			}
			// Embargo measured in EVENT ROWS after the first row at/after End.
			after := sort.Search(len(spans), func(k int) bool { return spans[k].Start >= t.End })
			if i >= after && i-after < embargoBars {
				keep = false
				break
			}
		}
		if keep {
			split.Train = append(split.Train, i)
		}
	}
	return split
}

// PurgedKFold uses contiguous, unshuffled array_split-style folds (7.3).
// Embargo is in observation rows; e.g. floor(n*0.01) for the book's 1% example.
// Unlike the printed snippet's <= boundary shortcut, closed-interval overlap
// is rejected consistently with section 7.4.1, including touching endpoints.
func PurgedKFold(spans []EventSpan, folds, embargoBars int) ([]PurgedSplit, error) {
	if err := afmlCVSpans(spans, folds, embargoBars); err != nil {
		return nil, err
	}
	out := make([]PurgedSplit, 0, folds)
	base, extra, start := len(spans)/folds, len(spans)%folds, 0
	for g := 0; g < folds; g++ {
		size := base
		if g < extra {
			size++
		}
		test := make([]int, size)
		for j := range test {
			test[j] = start + j
		}
		out = append(out, afmlPurgedSplit(spans, test, []int{g}, embargoBars))
		start += size
	}
	return out, nil
}

// CPCVPlan contains every combinatorial split and backtest-path assignments.
// Paths[path][group] is the split index that supplies that group's predictions.
// CV scenarios train on both sides of a held-out period; they are not historical
// walk-forward simulations and must not be deployed as PIT model artifacts.
type CPCVPlan struct {
	Splits []PurgedSplit
	Paths  [][]int
	Groups [][]int
}

// CombinatorialPurgedCV follows AFML 12.4's grouping (remainder in last group),
// combinations and per-label purging. maxSplits bounds combinatorial explosion.
func CombinatorialPurgedCV(spans []EventSpan, groups, testGroups, embargoBars, maxSplits int) (CPCVPlan, error) {
	if err := afmlCVSpans(spans, groups, embargoBars); err != nil {
		return CPCVPlan{}, err
	}
	if testGroups < 1 || testGroups > groups/2 || maxSplits < 1 {
		return CPCVPlan{}, errors.New("statistics: invalid CPCV test groups or split budget")
	}
	// Saturating combination count before allocating or iterating the mesh.
	combinationCount := new(big.Int).Binomial(int64(groups), int64(testGroups))
	if combinationCount.Cmp(big.NewInt(int64(maxSplits))) > 0 {
		return CPCVPlan{}, errors.New("statistics: CPCV exceeds maxSplits")
	}
	plan := CPCVPlan{Groups: make([][]int, groups)}
	base := len(spans) / groups
	for g := 0; g < groups; g++ {
		end := (g + 1) * base
		if g == groups-1 {
			end = len(spans)
		}
		for i := g * base; i < end; i++ {
			plan.Groups[g] = append(plan.Groups[g], i)
		}
	}
	comb := make([]int, testGroups)
	for i := range comb {
		comb[i] = i
	}
	for {
		var test []int
		for _, g := range comb {
			test = append(test, plan.Groups[g]...)
		}
		plan.Splits = append(plan.Splits, afmlPurgedSplit(spans, test, comb, embargoBars))
		at := testGroups - 1
		for at >= 0 && comb[at] == groups-testGroups+at {
			at--
		}
		if at < 0 {
			break
		}
		comb[at]++
		for i := at + 1; i < testGroups; i++ {
			comb[i] = comb[i-1] + 1
		}
	}
	pathCount := int(new(big.Int).Binomial(int64(groups-1), int64(testGroups-1)).Int64())
	plan.Paths = make([][]int, pathCount)
	for p := range plan.Paths {
		plan.Paths[p] = make([]int, groups)
	}
	used := make([]int, groups)
	for i, split := range plan.Splits {
		for _, g := range split.TestGroups {
			plan.Paths[used[g]][g] = i
			used[g]++
		}
	}
	return plan, nil
}
