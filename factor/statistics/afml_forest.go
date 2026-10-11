package statistics

import (
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"sort"
)

// RandomForestConfig declares the binary classifier's complete training policy.
// MaxFeatures=0 uses floor(sqrt(feature count)); SampleFraction=0 uses 1.
// ClassWeights=nil gives both classes weight 1. Criterion is gini or entropy.
type RandomForestConfig struct {
	Trees, MaxDepth, MinSamplesLeaf, MaxFeatures int
	SampleFraction                               float64
	Seed                                         int64
	Criterion                                    string
	ClassWeights                                 map[int]float64
	BalancedSubsample                            bool
}

func DefaultRandomForestConfig() RandomForestConfig {
	return RandomForestConfig{Trees: 100, MaxDepth: 6, MinSamplesLeaf: 5, SampleFraction: 1, Criterion: "entropy"}
}

type afmlForestNode struct {
	Feature     int     `json:"feature"`
	Threshold   float64 `json:"threshold"`
	Probability float64 `json:"probability"`
	Mass        float64 `json:"mass,omitempty"`
	Impurity    float64 `json:"impurity,omitempty"`
	Left, Right int
}
type afmlForestArtifact struct {
	Version  string             `json:"version"`
	Features int                `json:"features"`
	Config   RandomForestConfig `json:"config"`
	Trees    [][]afmlForestNode `json:"trees"`
}

// RandomForest is immutable after fitting. Its owned trees/config are private;
// Snapshot returns independent JSON bytes, and PredictProbability has no state.
type RandomForest struct{ artifact afmlForestArtifact }

func afmlForestConfig(config RandomForestConfig, features int) (RandomForestConfig, error) {
	if config.SampleFraction == 0 {
		config.SampleFraction = 1
	}
	if config.MaxFeatures == 0 {
		config.MaxFeatures = int(math.Sqrt(float64(features)))
	}
	if config.Criterion == "" {
		config.Criterion = "gini"
	}
	if config.Trees < 1 || config.MaxDepth < 1 || config.MinSamplesLeaf < 1 || config.MaxFeatures < 1 || config.MaxFeatures > features || !afmlFinite(config.SampleFraction) || config.SampleFraction <= 0 || config.SampleFraction > 1 || (config.Criterion != "gini" && config.Criterion != "entropy") {
		return config, errors.New("statistics: invalid random forest configuration")
	}
	owned := map[int]float64{0: 1, 1: 1}
	for label, w := range config.ClassWeights {
		if (label != 0 && label != 1) || !afmlFinite(w) || w <= 0 {
			return config, errors.New("statistics: class weights must be positive for binary labels")
		}
		owned[label] = w
	}
	config.ClassWeights = owned
	return config, nil
}

// TrainRandomForest fits bootstrapped CART trees with random feature subsets at
// every split, weighted impurity and weighted leaf probabilities (AFML 6.4).
// Samples must be already visible/matured and purged by the caller. Zero-weight
// samples are omitted before bootstrapping; nil weights means unit weight.
func TrainRandomForest(features [][]float64, labels []int, weights []float64, config RandomForestConfig) (*RandomForest, error) {
	return trainAFMLForest(features, labels, weights, config, 0, nil)
}

// TrainSequentialBootstrapForest replaces each ordinary bootstrap with AFML
// snippet4.5's overlap-aware draws. Set MaxFeatures to the feature count for
// bagged decision trees, or a smaller subset for a sequentially sampled RF.
// Spans are mature training-label metadata and are not retained by the model.
func TrainSequentialBootstrapForest(features [][]float64, labels []int, weights []float64, config RandomForestConfig, barCount int, spans []EventSpan) (*RandomForest, error) {
	if len(spans) != len(features) {
		return nil, errors.New("statistics: sequential forest event dimensions mismatch")
	}
	if err := afmlValidateSpans(barCount, spans); err != nil {
		return nil, err
	}
	return trainAFMLForest(features, labels, weights, config, barCount, spans)
}

func trainAFMLForest(features [][]float64, labels []int, weights []float64, config RandomForestConfig, barCount int, spans []EventSpan) (*RandomForest, error) {
	if len(features) == 0 || len(features) != len(labels) || len(features[0]) == 0 || (weights != nil && len(weights) != len(labels)) {
		return nil, errors.New("statistics: invalid random forest training dimensions")
	}
	columns := len(features[0])
	config, err := afmlForestConfig(config, columns)
	if err != nil {
		return nil, err
	}
	fitWeights := make([]float64, len(labels))
	var active []int
	for i, row := range features {
		if len(row) != columns || (labels[i] != 0 && labels[i] != 1) {
			return nil, errors.New("statistics: forest needs consistent features and binary labels")
		}
		for _, x := range row {
			if !afmlFinite(x) {
				return nil, errors.New("statistics: forest features must be finite")
			}
		}
		w := 1.
		if weights != nil {
			w = weights[i]
		}
		if !afmlFinite(w) || w < 0 {
			return nil, errors.New("statistics: invalid forest sample weight")
		}
		fitWeights[i] = w * config.ClassWeights[labels[i]]
		if w > 0 && fitWeights[i] == 0 {
			return nil, errors.New("statistics: forest effective weight underflow")
		}
		if !afmlFinite(fitWeights[i]) {
			return nil, errors.New("statistics: forest weight overflow")
		}
		if fitWeights[i] > 0 {
			active = append(active, i)
		}
	}
	if len(active) == 0 {
		return nil, errors.New("statistics: forest has no positive-weight samples")
	}
	// Normalize weights to bound accumulation. Reject ratios below floating-point
	// resolution rather than leave positive-weight samples with zero bootstrap mass.
	maxWeight := 0.
	for _, w := range fitWeights {
		maxWeight = math.Max(maxWeight, w)
	}
	for i := range fitWeights {
		positive := fitWeights[i] > 0
		fitWeights[i] /= maxWeight
		if positive && fitWeights[i] == 0 {
			return nil, errors.New("statistics: forest normalized weight underflow")
		}
	}
	model := &RandomForest{artifact: afmlForestArtifact{Version: "afml-rf-v2", Features: columns, Config: config}}
	rng := rand.New(rand.NewSource(config.Seed))
	sampleCount := int(math.Ceil(float64(len(active)) * config.SampleFraction))
	var activeSpans []EventSpan
	if spans != nil {
		for _, i := range active {
			activeSpans = append(activeSpans, spans[i])
		}
	}
	for tree := 0; tree < config.Trees; tree++ {
		rows := make([]int, sampleCount)
		if spans == nil {
			for i := range rows {
				rows[i] = active[rng.Intn(len(active))]
			}
		} else {
			draws, err := SequentialBootstrap(barCount, activeSpans, sampleCount, rng.Int63())
			if err != nil {
				return nil, err
			}
			for i, draw := range draws {
				rows[i] = active[draw]
			}
		}
		treeWeights := fitWeights
		if config.BalancedSubsample {
			counts := [2]int{}
			for _, i := range rows {
				counts[labels[i]]++
			}
			classes := 0
			for _, count := range counts {
				if count > 0 {
					classes++
				}
			}
			treeWeights = append([]float64(nil), fitWeights...)
			for _, i := range active {
				if count := counts[labels[i]]; count > 0 {
					treeWeights[i] *= float64(len(rows)) / float64(classes*count)
					if !afmlFinite(treeWeights[i]) || treeWeights[i] == 0 {
						return nil, errors.New("statistics: balanced forest weight overflow/underflow")
					}
				}
			}
		}
		var nodes []afmlForestNode
		var build func([]int, int) int
		build = func(rows []int, depth int) int {
			mass, positive := 0., 0.
			for _, i := range rows {
				mass += treeWeights[i]
				if labels[i] == 1 {
					positive += treeWeights[i]
				}
			}
			node := afmlForestNode{Feature: -1, Left: -1, Right: -1, Probability: positive / mass, Mass: mass, Impurity: afmlImpurity(positive/mass, config.Criterion)}
			at := len(nodes)
			nodes = append(nodes, node)
			if depth >= config.MaxDepth || len(rows)/2 < config.MinSamplesLeaf || positive == 0 || positive == mass {
				return at
			}
			candidates := rng.Perm(columns)[:config.MaxFeatures]
			sort.Ints(candidates)
			bestGain, bestFeature, bestThreshold := 0., -1, 0.
			parentImpurity := afmlImpurity(positive/mass, config.Criterion)
			for _, feature := range candidates {
				ordered := append([]int(nil), rows...)
				sort.SliceStable(ordered, func(a, b int) bool { return features[ordered[a]][feature] < features[ordered[b]][feature] })
				leftMass, leftPositive := 0., 0.
				for pos := 0; pos < len(ordered)-1; pos++ {
					i := ordered[pos]
					leftMass += treeWeights[i]
					if labels[i] == 1 {
						leftPositive += treeWeights[i]
					}
					if pos+1 < config.MinSamplesLeaf || len(ordered)-pos-1 < config.MinSamplesLeaf {
						continue
					}
					a, b := features[i][feature], features[ordered[pos+1]][feature]
					if a == b {
						continue
					}
					rightMass, rightPositive := mass-leftMass, positive-leftPositive
					if leftMass <= 0 || rightMass <= 0 {
						continue
					}
					lp, rp := math.Max(0, math.Min(1, leftPositive/leftMass)), math.Max(0, math.Min(1, rightPositive/rightMass))
					gain := parentImpurity - (leftMass/mass)*afmlImpurity(lp, config.Criterion) - (rightMass/mass)*afmlImpurity(rp, config.Criterion)
					if gain > bestGain {
						threshold := a/2 + b/2
						if threshold >= b {
							threshold = a
						}
						bestGain, bestFeature, bestThreshold = gain, feature, threshold
					}
				}
			}
			if bestFeature < 0 {
				return at
			}
			var left, right []int
			for _, i := range rows {
				if features[i][bestFeature] <= bestThreshold {
					left = append(left, i)
				} else {
					right = append(right, i)
				}
			}
			li, ri := build(left, depth+1), build(right, depth+1)
			nodes[at].Feature = bestFeature
			nodes[at].Threshold = bestThreshold
			nodes[at].Left = li
			nodes[at].Right = ri
			return at
		}
		build(rows, 0)
		model.artifact.Trees = append(model.artifact.Trees, nodes)
	}
	return model, nil
}

func afmlImpurity(p float64, criterion string) float64 {
	if p <= 0 || p >= 1 {
		return 0
	}
	if criterion == "entropy" {
		return -p*math.Log2(p) - (1-p)*math.Log2(1-p)
	}
	return 2 * p * (1 - p)
}

func (f *RandomForest) PredictProbability(features []float64) (float64, error) {
	if f == nil || len(features) != f.artifact.Features || len(f.artifact.Trees) == 0 {
		return 0, errors.New("statistics: forest prediction dimension mismatch")
	}
	for _, x := range features {
		if !afmlFinite(x) {
			return 0, errors.New("statistics: forest prediction must be finite")
		}
	}
	sum := 0.
	for _, tree := range f.artifact.Trees {
		at := 0
		for tree[at].Feature >= 0 {
			node := tree[at]
			if features[node.Feature] <= node.Threshold {
				at = node.Left
			} else {
				at = node.Right
			}
		}
		sum += tree[at].Probability
	}
	return sum / float64(len(f.artifact.Trees)), nil
}

func (f *RandomForest) Snapshot() ([]byte, error) {
	if f == nil {
		return nil, errors.New("statistics: nil forest")
	}
	return json.Marshal(f.artifact)
}

// RestoreRandomForest validates version, shape and every rooted tree before use.
// Metadata maturity/PIT and content hashes belong to research.ModelArtifact.
func RestoreRandomForest(raw []byte) (*RandomForest, error) {
	var artifact afmlForestArtifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		return nil, err
	}
	if (artifact.Version != "afml-rf-v1" && artifact.Version != "afml-rf-v2") || artifact.Features < 1 {
		return nil, errors.New("statistics: unsupported forest artifact")
	}
	config, err := afmlForestConfig(artifact.Config, artifact.Features)
	if err != nil {
		return nil, err
	}
	artifact.Config = config
	if len(artifact.Trees) != config.Trees {
		return nil, errors.New("statistics: forest tree count mismatch")
	}
	for _, tree := range artifact.Trees {
		if len(tree) == 0 {
			return nil, errors.New("statistics: empty forest tree")
		}
		seen := make([]bool, len(tree))
		var visit func(int, int) error
		visit = func(at, depth int) error {
			if at < 0 || at >= len(tree) || seen[at] || depth > config.MaxDepth {
				return errors.New("statistics: invalid forest tree topology")
			}
			seen[at] = true
			n := tree[at]
			if artifact.Version == "afml-rf-v2" && (!afmlFinite(n.Mass) || n.Mass <= 0 || !afmlFinite(n.Impurity) || math.Abs(n.Impurity-afmlImpurity(n.Probability, config.Criterion)) > 1e-10) {
				return errors.New("statistics: invalid forest impurity metadata")
			}
			if !afmlFinite(n.Probability) || n.Probability < 0 || n.Probability > 1 || !afmlFinite(n.Threshold) {
				return errors.New("statistics: invalid forest node values")
			}
			if n.Feature == -1 {
				if n.Left != -1 || n.Right != -1 {
					return errors.New("statistics: forest leaf has children")
				}
				return nil
			}
			if n.Feature < 0 || n.Feature >= artifact.Features {
				return errors.New("statistics: invalid forest split feature")
			}
			if err := visit(n.Left, depth+1); err != nil {
				return err
			}
			if err := visit(n.Right, depth+1); err != nil {
				return err
			}
			if artifact.Version == "afml-rf-v2" {
				l, r := tree[n.Left], tree[n.Right]
				if math.Abs(l.Mass+r.Mass-n.Mass) > 1e-10*n.Mass || math.Abs(l.Mass*l.Probability+r.Mass*r.Probability-n.Mass*n.Probability) > 1e-10*n.Mass || n.Mass*n.Impurity-l.Mass*l.Impurity-r.Mass*r.Impurity < -1e-10*n.Mass {
					return errors.New("statistics: inconsistent forest split metadata")
				}
			}
			return nil
		}
		if err := visit(0, 0); err != nil {
			return nil, err
		}
		for _, ok := range seen {
			if !ok {
				return nil, errors.New("statistics: forest contains unreachable nodes")
			}
		}
	}
	return &RandomForest{artifact: artifact}, nil
}
