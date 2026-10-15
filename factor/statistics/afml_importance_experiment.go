package statistics

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

// ImportanceExperimentConfig bounds an AFML 8.5–8.6 distribution adaptation.
// Work is a conservative row*tree*feature*fold allowance, not a CPU-time promise.
type ImportanceExperimentConfig struct {
	Seed                                                              int64
	Rows, Features, Informative, Redundant, Trees, Folds, Instruments int
	MaxWork                                                           int64
}

func DefaultImportanceExperimentConfig() ImportanceExperimentConfig {
	return ImportanceExperimentConfig{Seed: 0, Rows: 1200, Features: 40, Informative: 10, Redundant: 10, Trees: 24, Folds: 3, Instruments: 2, MaxWork: 100000000}
}

// SourceImportanceExperimentConfig declares snippet 8.7/8.8 scale. Raising the
// work budget is explicit; the full source-scale experiment is computationally expensive.
func SourceImportanceExperimentConfig() ImportanceExperimentConfig {
	c := DefaultImportanceExperimentConfig()
	c.Rows, c.Trees, c.Folds, c.MaxWork = 10000, 1000, 10, 100000000000
	return c
}

type SyntheticImportanceData struct {
	Rows                  [][]float64
	Labels                []int
	Names, Groups         []string
	RedundantCoefficients [][]float64
}

func validateImportanceExperiment(c ImportanceExperimentConfig) error {
	if c.Rows < 20 || c.Rows > 1000000 || c.Features < 2 || c.Features > 100 || c.Informative < 1 || c.Redundant < 0 || c.Informative+c.Redundant >= c.Features || c.Trees < 2 || c.Trees > 10000 || c.Folds < 2 || c.Folds > 100 || c.Instruments < 1 || c.Instruments > 10 || c.Rows/c.Instruments < c.Folds*4 {
		return errors.New("statistics: invalid importance experiment dimensions")
	}
	work := int64(c.Rows) * int64(c.Trees) * int64(c.Features) * int64(c.Folds) * 8
	if c.MaxWork < work {
		return fmt.Errorf("statistics: importance experiment budget requires at least %d work units", work)
	}
	return nil
}

// GenerateSyntheticImportanceData uses independent standard normals, a linear
// threshold label, and random linear redundant columns. It preserves I/R/N order
// and owns every slice. This is not sklearn make_classification's cluster algorithm.
func GenerateSyntheticImportanceData(c ImportanceExperimentConfig) (SyntheticImportanceData, error) {
	d := SyntheticImportanceData{}
	if err := validateImportanceExperiment(c); err != nil {
		return d, err
	}
	rng := rand.New(rand.NewSource(c.Seed))
	for j := 0; j < c.Features; j++ {
		group, index := "I", j
		if j >= c.Informative {
			group, index = "R", j-c.Informative
		}
		if j >= c.Informative+c.Redundant {
			group, index = "N", j-c.Informative-c.Redundant
		}
		d.Names = append(d.Names, fmt.Sprintf("%s_%d", group, index))
		d.Groups = append(d.Groups, group)
	}
	for j := 0; j < c.Redundant; j++ {
		coef := make([]float64, c.Informative)
		for k := range coef {
			coef[k] = rng.NormFloat64()
		}
		d.RedundantCoefficients = append(d.RedundantCoefficients, coef)
	}
	for i := 0; i < c.Rows; i++ {
		row, score := make([]float64, c.Features), 0.
		for j := 0; j < c.Informative; j++ {
			row[j] = rng.NormFloat64()
			score += row[j]
		}
		for j, coef := range d.RedundantCoefficients {
			for k, v := range coef {
				row[c.Informative+j] += v * row[k]
			}
		}
		for j := c.Informative + c.Redundant; j < c.Features; j++ {
			row[j] = rng.NormFloat64()
		}
		label := 0
		if score > 0 {
			label = 1
		}
		d.Rows, d.Labels = append(d.Rows, row), append(d.Labels, label)
	}
	return d, nil
}

// ImportanceNumber serializes undefined scores as null plus an explicit flag.
type ImportanceNumber struct {
	Value     *float64
	Undefined bool
}

func importanceNumber(v float64) ImportanceNumber {
	if !afmlFinite(v) {
		return ImportanceNumber{Undefined: true}
	}
	return ImportanceNumber{Value: &v}
}

type ImportanceScore struct {
	Mean, StdError ImportanceNumber
	ValidValues    int
	Values         []ImportanceNumber
}

func importanceScore(s FeatureImportance) ImportanceScore {
	r := ImportanceScore{Mean: importanceNumber(s.Mean), StdError: importanceNumber(s.StdError), ValidValues: s.ValidFolds}
	for _, v := range s.FoldValues {
		r.Values = append(r.Values, importanceNumber(v))
	}
	return r
}

type ImportanceFeatureEvidence struct {
	Name, Group   string
	MDI, MDA, SFI ImportanceScore
}
type ImportanceFoldAudit struct {
	Train, Test                        []int
	PCA                                PCAModel
	PCABaseline                        float64
	PCAFeatures                        []ImportanceFeatureEvidence
	KendallMDI, KendallMDA, KendallSFI ImportanceNumber
}
type ImportanceAnalysis struct {
	Name       string
	SourceRows []int
	Features   []ImportanceFeatureEvidence
	Baseline   []float64
	Folds      []ImportanceFoldAudit
}
type ImportanceExperimentResult struct {
	Config                ImportanceExperimentConfig
	Distribution          string
	EstimatedWork         int64
	RedundantCoefficients [][]float64
	Analyses              []ImportanceAnalysis
	// ParallelMean summarizes instrument means with equal instrument weights.
	// Its Values/StdError describe instruments, not folds or decision trees.
	ParallelMean []ImportanceFeatureEvidence
}

func importanceFit(c ImportanceExperimentConfig) AFMLWeightedClassifierFit {
	return func(x [][]float64, y []int, w []float64, _ []int) (AFMLProbabilityPredictor, error) {
		forest, err := TrainRandomForest(x, y, w, RandomForestConfig{Trees: c.Trees, MaxDepth: 6, MinSamplesLeaf: 5, MaxFeatures: 1, SampleFraction: 1, Seed: c.Seed, Criterion: "entropy", BalancedSubsample: true})
		if err != nil {
			return nil, err
		}
		return forestImportancePredictor(forest), nil
	}
}
func forestImportancePredictor(forest *RandomForest) AFMLProbabilityPredictor {
	return func(rows [][]float64) ([][]float64, error) {
		p := make([][]float64, len(rows))
		for i, row := range rows {
			v, err := forest.PredictProbability(row)
			if err != nil {
				return nil, err
			}
			p[i] = []float64{1 - v, v}
		}
		return p, nil
	}
}
func importanceForest(x [][]float64, y []int, c ImportanceExperimentConfig) (*RandomForest, error) {
	return TrainRandomForest(x, y, nil, RandomForestConfig{Trees: c.Trees, MaxDepth: 6, MinSamplesLeaf: 5, MaxFeatures: 1, Seed: c.Seed, Criterion: "entropy", BalancedSubsample: true})
}
func importanceKendall(scores []FeatureImportance) (ImportanceNumber, error) {
	x, y := []float64{}, []float64{}
	for j, s := range scores {
		if afmlFinite(s.Mean) {
			x = append(x, s.Mean)
			y = append(y, 1/float64(j+1))
		}
	}
	if len(x) < 2 {
		return ImportanceNumber{Undefined: true}, nil
	}
	order := make([]int, len(x))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool { return x[order[i]] > x[order[j]] })
	ranks := make([]int, len(x))
	for rank, i := range order {
		ranks[i] = rank
	}
	v, err := HyperbolicWeightedKendall(x, y, ranks, len(x)*len(x))
	return importanceNumber(v), err
}

// importancePCAFold transforms held-out rows with a training-only PCA. Each
// fold has its own basis and retained dimension; scores are never pooled as if
// differently fitted eigenvectors were the same features.
func importancePCAFold(x [][]float64, y []int, split PurgedSplit, c ImportanceExperimentConfig) (ImportanceFoldAudit, error) {
	a := ImportanceFoldAudit{Train: append([]int(nil), split.Train...), Test: append([]int(nil), split.Test...)}
	train, ty, _ := afmlImportanceRows(x, y, nil, split.Train, -1)
	test, vy, _ := afmlImportanceRows(x, y, nil, split.Test, -1)
	model, err := FitOrthogonalFeatures(train, .95)
	if err != nil {
		return a, err
	}
	a.PCA = model
	for i, row := range train {
		train[i], err = model.Transform(row)
		if err != nil {
			return a, err
		}
	}
	for i, row := range test {
		test[i], err = model.Transform(row)
		if err != nil {
			return a, err
		}
	}
	f, err := importanceForest(train, ty, c)
	if err != nil {
		return a, err
	}
	mdi, err := f.MeanDecreaseImpurity()
	if err != nil {
		return a, err
	}
	predict := forestImportancePredictor(f)
	p, err := predict(test)
	if err != nil {
		return a, err
	}
	base, err := afmlImportanceScore(vy, []int{0, 1}, p, nil, "accuracy")
	if err != nil {
		return a, err
	}
	a.PCABaseline = base
	mda, sfi := make([]FeatureImportance, len(mdi)), make([]FeatureImportance, len(mdi))
	rng := rand.New(rand.NewSource(c.Seed))
	for j := range mdi {
		indices := make([]int, len(test))
		for i := range indices {
			indices[i] = i
		}
		permuted, _, _ := afmlImportanceRows(test, vy, nil, indices, -1)
		perm := rng.Perm(len(test))
		for i := range perm {
			permuted[i][j] = test[perm[i]][j]
		}
		p, err = predict(permuted)
		if err != nil {
			return a, err
		}
		score, e := afmlImportanceScore(vy, []int{0, 1}, p, nil, "accuracy")
		if e != nil {
			return a, e
		}
		value := 0.
		if score == 1 {
			mda[j] = afmlImportanceSummary(nil)
		} else {
			value = (base - score) / (1 - score)
			mda[j] = afmlImportanceSummary([]float64{value})
		}
		tr := make([][]float64, len(train))
		te := make([][]float64, len(test))
		for i := range tr {
			tr[i] = []float64{train[i][j]}
		}
		for i := range te {
			te[i] = []float64{test[i][j]}
		}
		sf, e := importanceForest(tr, ty, c)
		if e != nil {
			return a, e
		}
		p, e = forestImportancePredictor(sf)(te)
		if e != nil {
			return a, e
		}
		score, e = afmlImportanceScore(vy, []int{0, 1}, p, nil, "accuracy")
		if e != nil {
			return a, e
		}
		sfi[j] = afmlImportanceSummary([]float64{score})
		a.PCAFeatures = append(a.PCAFeatures, ImportanceFeatureEvidence{fmt.Sprintf("PC_%d", j+1), "PCA", importanceScore(mdi[j]), importanceScore(mda[j]), importanceScore(sfi[j])})
	}
	a.KendallMDI, err = importanceKendall(mdi)
	if err != nil {
		return a, err
	}
	a.KendallMDA, err = importanceKendall(mda)
	if err != nil {
		return a, err
	}
	a.KendallSFI, err = importanceKendall(sfi)
	return a, err
}

func runImportanceAnalysis(d SyntheticImportanceData, indices []int, name string, c ImportanceExperimentConfig) (ImportanceAnalysis, error) {
	a := ImportanceAnalysis{Name: name, SourceRows: append([]int(nil), indices...)}
	x, y, _ := afmlImportanceRows(d.Rows, d.Labels, nil, indices, -1)
	spans := make([]EventSpan, len(x))
	for i := range spans {
		spans[i] = EventSpan{Start: i, End: i}
	}
	f, err := importanceForest(x, y, c)
	if err != nil {
		return a, err
	}
	mdi, err := f.MeanDecreaseImpurity()
	if err != nil {
		return a, err
	}
	mda, err := PurgedFeatureImportance(x, y, []int{0, 1}, nil, spans, c.Folds, 0, "accuracy", false, c.Seed, importanceFit(c))
	if err != nil {
		return a, err
	}
	sfi, err := PurgedFeatureImportance(x, y, []int{0, 1}, nil, spans, c.Folds, 0, "accuracy", true, c.Seed, importanceFit(c))
	if err != nil {
		return a, err
	}
	a.Baseline = mda.Baseline
	for j := range mdi {
		a.Features = append(a.Features, ImportanceFeatureEvidence{d.Names[j], d.Groups[j], importanceScore(mdi[j]), importanceScore(mda.Features[j]), importanceScore(sfi.Features[j])})
	}
	splits, err := PurgedKFold(spans, c.Folds, 0)
	if err != nil {
		return a, err
	}
	for _, split := range splits {
		audit, e := importancePCAFold(x, y, split, c)
		if e != nil {
			return a, e
		}
		a.Folds = append(a.Folds, audit)
	}
	return a, nil
}

// RunImportanceExperiment compares contiguous synthetic instrument partitions
// with their stacked union. These IID scenarios do not establish market alpha.
// MDI uses the analysis sample; MDA/SFI are purged CV. PCA MDI uses fold training
// only, while PCA MDA/SFI use that fold's held-out observations.
func RunImportanceExperiment(c ImportanceExperimentConfig) (ImportanceExperimentResult, error) {
	r := ImportanceExperimentResult{Config: c, Distribution: "Gaussian linear-threshold adaptation; not sklearn make_classification", EstimatedWork: int64(c.Rows) * int64(c.Trees) * int64(c.Features) * int64(c.Folds) * 8}
	d, err := GenerateSyntheticImportanceData(c)
	if err != nil {
		return r, err
	}
	r.RedundantCoefficients = d.RedundantCoefficients
	all := make([]int, c.Rows)
	for i := range all {
		all[i] = i
	}
	a, err := runImportanceAnalysis(d, all, "stacked", c)
	if err != nil {
		return r, err
	}
	r.Analyses = append(r.Analyses, a)
	if c.Instruments > 1 {
		for instrument := 0; instrument < c.Instruments; instrument++ {
			lo, hi := instrument*c.Rows/c.Instruments, (instrument+1)*c.Rows/c.Instruments
			a, err = runImportanceAnalysis(d, all[lo:hi], fmt.Sprintf("instrument_%d", instrument), c)
			if err != nil {
				return r, err
			}
			r.Analyses = append(r.Analyses, a)
		}
		for j, feature := range r.Analyses[0].Features {
			var mdi, mda, sfi []float64
			for _, analysis := range r.Analyses[1:] {
				f := analysis.Features[j]
				if f.MDI.Mean.Value != nil {
					mdi = append(mdi, *f.MDI.Mean.Value)
				}
				if f.MDA.Mean.Value != nil {
					mda = append(mda, *f.MDA.Mean.Value)
				}
				if f.SFI.Mean.Value != nil {
					sfi = append(sfi, *f.SFI.Mean.Value)
				}
			}
			r.ParallelMean = append(r.ParallelMean, ImportanceFeatureEvidence{feature.Name, feature.Group, importanceScore(afmlImportanceSummary(mdi)), importanceScore(afmlImportanceSummary(mda)), importanceScore(afmlImportanceSummary(sfi))})
		}
	}
	return r, nil
}
