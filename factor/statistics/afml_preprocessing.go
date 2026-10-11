package statistics

import (
	"errors"
	"math"
	"slices"
)

// ExpandingFractionalDifference implements AFML snippet 5.2, including its
// full-prefix absolute-weight skip count. Unlike FFD, all earlier observations
// retain coefficients and the initial constant can generate negative drift.
// Call on a declared training prefix: its length affects warmup selection.
// NaN history is forward-filled, leading NaNs omitted, current NaNs retained.
// Returned skip counts observations after the first valid value. maxWork bounds
// the quadratic convolution; it never silently truncates the expanding kernel.
func ExpandingFractionalDifference(values []float64, d, lossThreshold float64, maxWork int64) ([]float64, int, error) {
	n := len(values)
	if n == 0 || n > 100000 || !afmlFinite(d) || d < 0 || !afmlFinite(lossThreshold) || lossThreshold < 0 || lossThreshold > 1 || maxWork < int64(n)*(int64(n)+1)/2 {
		return nil, 0, errors.New("statistics: invalid expanding fracdiff parameters or work budget")
	}
	w := make([]float64, n)
	w[0] = 1
	total := 1.
	for k := 1; k < n; k++ {
		w[k] = -w[k-1] * (d - float64(k) + 1) / float64(k)
		total += math.Abs(w[k])
		if !afmlFinite(w[k]) || !afmlFinite(total) {
			return nil, 0, errors.New("statistics: expanding fracdiff weight overflow")
		}
	}
	cumulative, skip := 0., 0
	for k := n - 1; k >= 0; k-- {
		cumulative += math.Abs(w[k])
		if cumulative/total > lossThreshold {
			skip++
		}
	}
	out := make([]float64, n)
	filled := make([]float64, n)
	first, last := -1, math.NaN()
	for i, value := range values {
		out[i] = math.NaN()
		if math.IsInf(value, 0) {
			return nil, 0, errors.New("statistics: infinite expanding fracdiff input")
		}
		if !math.IsNaN(value) {
			last = value
			if first < 0 {
				first = i
			}
		}
		filled[i] = last
		if first < 0 || i-first < skip || math.IsNaN(value) {
			continue
		}
		sum := 0.
		for j := first; j <= i; j++ {
			sum += w[i-j] * filled[j]
		}
		if !afmlFinite(sum) {
			return nil, 0, errors.New("statistics: expanding fracdiff result overflow")
		}
		out[i] = sum
	}
	return out, skip, nil
}

// DropRareLabels returns retained row indices and the classes removed by AFML
// snippet 3.8. Its actual comparison drops a class at minFraction equality;
// it stops with two classes, even if one remains rare. Ties use first appearance
// rather than an unstable map iteration or pandas-version-specific tie order.
func DropRareLabels(labels []int, minFraction float64) ([]int, []int, error) {
	if len(labels) == 0 || len(labels) > 10000000 || !afmlFinite(minFraction) || minFraction < 0 || minFraction > 1 {
		return nil, nil, errors.New("statistics: invalid rare-label inputs")
	}
	indices := make([]int, len(labels))
	for i := range indices {
		indices[i] = i
	}
	var dropped []int
	for {
		counts := map[int]int{}
		var classes []int
		for _, i := range indices {
			if counts[labels[i]] == 0 {
				classes = append(classes, labels[i])
			}
			counts[labels[i]]++
		}
		if len(classes) < 3 {
			break
		}
		rare := classes[0]
		for _, label := range classes[1:] {
			if counts[label] < counts[rare] {
				rare = label
			}
		}
		if float64(counts[rare])/float64(len(indices)) > minFraction {
			break
		}
		dropped = append(dropped, rare)
		kept := indices[:0]
		for _, i := range indices {
			if labels[i] != rare {
				kept = append(kept, i)
			}
		}
		indices = kept
	}
	return slices.Clone(indices), dropped, nil
}
