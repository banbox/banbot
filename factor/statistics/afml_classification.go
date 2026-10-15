package statistics

import (
	"errors"
	"math"
	"sort"
)

type BinaryClassificationReport struct {
	TruePositive, FalsePositive, TrueNegative, FalseNegative float64
	Accuracy, Precision, Recall, F1, LogLoss, ROCAUC         float64
	PrecisionValid, RecallValid, F1Valid, AUCValid           bool
}

// BinaryClassification evaluates frozen out-of-sample probabilities and weights.
// Scores equal to threshold predict positive. Undefined scores remain NaN with
// explicit validity flags. AUC is pairwise weighted and awards half for ties.
// epsilon clips log-loss only; it never changes thresholding or ROC ranking.
func BinaryClassification(labels []int, probabilities, weights []float64, threshold, epsilon float64) (BinaryClassificationReport, error) {
	r := BinaryClassificationReport{Precision: math.NaN(), Recall: math.NaN(), F1: math.NaN(), ROCAUC: math.NaN()}
	if len(labels) == 0 || len(labels) != len(probabilities) || (weights != nil && len(weights) != len(labels)) || !afmlFinite(threshold) || threshold < 0 || threshold > 1 || !afmlFinite(epsilon) || epsilon <= 0 || epsilon >= .5 {
		return r, errors.New("statistics: invalid binary scoring inputs")
	}
	type row struct{ p, positive, negative float64 }
	rows := make([]row, len(labels))
	total, loss := 0., 0.
	for i, label := range labels {
		p, w := probabilities[i], 1.
		if weights != nil {
			w = weights[i]
		}
		if (label != 0 && label != 1) || !afmlFinite(p) || p < 0 || p > 1 || !afmlFinite(w) || w < 0 {
			return r, errors.New("statistics: invalid binary class/probability/weight")
		}
		total += w
		clipped := math.Max(epsilon, math.Min(1-epsilon, p))
		if label == 1 {
			loss -= w * math.Log(clipped)
			rows[i] = row{p: p, positive: w}
			if p >= threshold {
				r.TruePositive += w
			} else {
				r.FalseNegative += w
			}
		} else {
			loss -= w * math.Log1p(-clipped)
			rows[i] = row{p: p, negative: w}
			if p >= threshold {
				r.FalsePositive += w
			} else {
				r.TrueNegative += w
			}
		}
	}
	if total <= 0 || !afmlFinite(total) || !afmlFinite(loss) {
		return r, errors.New("statistics: binary score mass overflow/zero")
	}
	r.Accuracy, r.LogLoss = (r.TruePositive+r.TrueNegative)/total, loss/total
	if denom := r.TruePositive + r.FalsePositive; denom > 0 {
		r.Precision, r.PrecisionValid = r.TruePositive/denom, true
	}
	if denom := r.TruePositive + r.FalseNegative; denom > 0 {
		r.Recall, r.RecallValid = r.TruePositive/denom, true
	}
	if denom := 2*(r.TruePositive/total) + r.FalsePositive/total + r.FalseNegative/total; denom > 0 {
		r.F1, r.F1Valid = 2*(r.TruePositive/total)/denom, true
	}
	pos, neg := r.TruePositive+r.FalseNegative, r.TrueNegative+r.FalsePositive
	if pos > 0 && neg > 0 {
		sort.Slice(rows, func(i, j int) bool { return rows[i].p < rows[j].p })
		// Normalize first to avoid overflow in products of raw input weights.
		below, area := 0., 0.
		for begin := 0; begin < len(rows); {
			end := begin + 1
			for end < len(rows) && rows[end].p == rows[begin].p {
				end++
			}
			gp, gn := 0., 0.
			for _, row := range rows[begin:end] {
				gp += row.positive / pos
				gn += row.negative / neg
			}
			area += gp * (below + .5*gn)
			below += gn
			begin = end
		}
		r.ROCAUC, r.AUCValid = area, true
	}
	return r, nil
}
