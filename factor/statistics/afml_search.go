package statistics

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
)

type ParameterTrial struct {
	Parameters map[string]float64
	FoldScores []float64
	MeanScore  float64
}
type PurgedSearchResult struct {
	Trials []ParameterTrial
	Best   int
}
type AFMLParameterFoldFit func(parameters map[string]float64, trainIndices, testIndices []int, trainWeights []float64, classes []int) ([][]float64, error)

// PurgedParameterSearch preserves every supplied trial and scores each through
// weighted purged folds. It chooses the first tied maximum mean fold score.
// The declared trials may be a deterministic grid or seeded randomized list;
// scoring="auto" follows snippet9.1 ({0,1}: F1; otherwise negative log
// loss). Explicit F1 uses label1 as positive. No refit/deployment is implied.
func PurgedParameterSearch(candidates []map[string]float64, maxTrials int, spans []EventSpan, labels, classes []int, weights []float64, folds, embargo int, scoring string, fit AFMLParameterFoldFit) (PurgedSearchResult, error) {
	r := PurgedSearchResult{Best: -1}
	if scoring == "auto" {
		// Snippet9.1 routes exactly the observed label set {0,1} to F1.
		seen := map[int]bool{}
		for _, label := range labels {
			seen[label] = true
		}
		scoring = "neg_log_loss"
		if len(seen) == 2 && seen[0] && seen[1] {
			scoring = "f1"
		}
	}
	if scoring == "f1" && (len(classes) != 2 || !((classes[0] == 0 && classes[1] == 1) || (classes[0] == 1 && classes[1] == 0))) {
		return r, errors.New("statistics: F1 search requires classes zero and one")
	}
	if fit == nil || len(candidates) == 0 || maxTrials < len(candidates) || (scoring != "accuracy" && scoring != "neg_log_loss" && scoring != "f1") {
		return r, errors.New("statistics: invalid purged parameter search/budget")
	}
	clone := func(p map[string]float64) map[string]float64 {
		out := map[string]float64{}
		for key, v := range p {
			out[key] = v
		}
		return out
	}
	best := math.Inf(-1)
	for index, parameters := range candidates {
		for key, v := range parameters {
			if key == "" || !afmlFinite(v) {
				return PurgedSearchResult{}, errors.New("statistics: invalid trial parameter")
			}
		}
		owned := clone(parameters)
		reports, err := PurgedClassificationScores(spans, labels, classes, weights, folds, embargo, 1e-15, func(train, test []int, w []float64, c []int) ([][]float64, error) {
			return fit(clone(owned), train, test, w, c)
		})
		if err != nil {
			return PurgedSearchResult{}, fmt.Errorf("statistics: trial%d: %w", index, err)
		}
		trial := ParameterTrial{Parameters: owned}
		for _, report := range reports {
			score := report.Accuracy
			if scoring == "neg_log_loss" {
				score = -report.LogLoss
			} else if scoring == "f1" {
				positive := 1
				if report.Classes[0] == 1 {
					positive = 0
				}
				negative := 1 - positive
				tp := report.Confusion[positive][positive]
				fp := report.Confusion[negative][positive]
				fn := report.Confusion[positive][negative]
				// Source sklearn convention: undefined F1 becomes zero.
				scale := math.Max(tp, math.Max(fp, fn))
				score = 0
				if scale > 0 {
					score = 2 * (tp / scale) / (2*(tp/scale) + fp/scale + fn/scale)
				}
			}
			trial.FoldScores = append(trial.FoldScores, score)
			trial.MeanScore += score / float64(len(reports))
		}
		r.Trials = append(r.Trials, trial)
		if trial.MeanScore > best {
			best = trial.MeanScore
			r.Best = index
		}
	}
	return r, nil
}

// Linspace includes both endpoints; count=1 follows numpy's start-only rule.
func Linspace(start, end float64, count int) ([]float64, error) {
	if count < 1 || !afmlFinite(start) || !afmlFinite(end) {
		return nil, errors.New("statistics: invalid linspace")
	}
	out := make([]float64, count)
	out[0] = start
	for i := 1; i < count; i++ {
		p := float64(i) / float64(count-1)
		out[i] = (1-p)*start + p*end
	}
	if count > 1 {
		out[count-1] = end
	}
	return out, nil
}

func LogUniformQuantile(low, high, p float64) (float64, error) {
	if !afmlFinite(low) || !afmlFinite(high) || low <= 0 || high <= low || !afmlFinite(p) || p < 0 || p > 1 {
		return 0, errors.New("statistics: invalid loguniform specification")
	}
	if p == 0 {
		return low, nil
	}
	if p == 1 {
		return high, nil
	}
	return math.Exp((1-p)*math.Log(low) + p*math.Log(high)), nil
}
func LogUniformCDF(low, high, x float64) (float64, error) {
	if _, err := LogUniformQuantile(low, high, 0); err != nil {
		return 0, err
	}
	if math.IsNaN(x) {
		return 0, errors.New("statistics: NaN loguniform input")
	}
	if x <= low {
		return 0, nil
	}
	if x >= high {
		return 1, nil
	}
	return (math.Log(x) - math.Log(low)) / (math.Log(high) - math.Log(low)), nil
}

// UniformDownsample samples without replacement, returns sorted source row
// identities, and uses a local RNG so concurrent experiments cannot perturb it.
func UniformDownsample(population, count int, seed int64) ([]int, error) {
	if population < 1 || count < 1 || count > population {
		return nil, errors.New("statistics: invalid downsample count")
	}
	r := rand.New(rand.NewSource(seed))
	p := r.Perm(population)
	p = append([]int(nil), p[:count]...)
	sort.Ints(p)
	return p, nil
}

// LinearPartitions implements source snippet20.5 ceil(linspace).
func LinearPartitions(atoms, threads int) ([]int, error) {
	if atoms < 1 || threads < 1 {
		return nil, errors.New("statistics: invalid partition size")
	}
	m := min(atoms, threads)
	p := make([]int, m+1)
	for i := range p {
		p[i] = int(math.Ceil(float64(atoms) * float64(i) / float64(m)))
	}
	return p, nil
}

// NestedPartitions implements snippet20.6's cumulative triangular-work roots
// and nearest-even rounding. Upper reverses partition sizes for heavy first rows.
func NestedPartitions(atoms, threads int, upper bool) ([]int, error) {
	if atoms < 1 || threads < 1 {
		return nil, errors.New("statistics: invalid nested partition size")
	}
	m := min(atoms, threads)
	p := make([]int, m+1)
	last := 0.
	work := float64(atoms) * (float64(atoms) + 1) / float64(m)
	for i := 1; i <= m; i++ {
		last = (math.Sqrt(1+4*(last*last+last+work)) - 1) / 2
		p[i] = int(math.RoundToEven(last))
	}
	p[m] = atoms
	if upper {
		q := make([]int, m+1)
		for i := 1; i <= m; i++ {
			q[i] = q[i-1] + p[m-i+1] - p[m-i]
		}
		p = q
	}
	return p, nil
}
