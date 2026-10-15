package statistics

import (
	"errors"
	"math"
	"math/rand"
	"sort"
)

// SequentialBaggingConfig describes chapter5 exercise6's full-feature entropy
// trees. Each sampled bag gets freshly computed chapter4 return-attribution
// weights, including repeated spans in concurrency. DecayC=1 disables decay;
// other values use chronological cumulative uniqueness on sampled occurrences.
type SequentialBaggingConfig struct {
	Trees, MaxDepth, MinSamplesLeaf int
	SampleFraction, DecayC          float64
	Seed                            int64
	MaxWork                         int64
}

type BaggingAudit struct {
	Draws   []int
	Weights []float64
}

type afmlClassNode struct {
	Feature, Left, Right int
	Threshold            float64
	Probability          [3]float64
}

// SequentialBaggedClassifier owns immutable fitted trees. Class order is
// {-1,0,+1}; neutral is an actual class, not a down/negative binary proxy.
type SequentialBaggedClassifier struct {
	columns int
	trees   [][]afmlClassNode
	audit   []BaggingAudit
}

func entropy3(mass [3]float64) float64 {
	total := mass[0] + mass[1] + mass[2]
	if total <= 0 {
		return 0
	}
	value := 0.
	for _, m := range mass {
		if m > 0 {
			p := m / total
			value -= p * math.Log2(p)
		}
	}
	return value
}

// BootstrapReturnWeights recomputes attribution AFTER drawing, on the bag's
// occurrence spans, not the original dataset weights. Optional time decay is
// sorted by event start (stable among repeated starts), then restored to draw
// order. Zero-attribution occurrences retain zero mass; an all-zero bag errors.
func BootstrapReturnWeights(close []float64, spans []EventSpan, draws []int, decayC float64) ([]float64, error) {
	if len(draws) == 0 || !afmlFinite(decayC) || decayC <= -1 || decayC > 1 {
		return nil, errors.New("statistics: invalid bag attribution draws/decay")
	}
	bag := make([]EventSpan, len(draws))
	for i, draw := range draws {
		if draw < 0 || draw >= len(spans) {
			return nil, errors.New("statistics: invalid bag event index")
		}
		bag[i] = spans[draw]
	}
	weights, err := AttributedReturnWeights(close, bag)
	if err != nil || decayC == 1 {
		return weights, err
	}
	uniqueness, err := EventUniqueness(len(close), bag)
	if err != nil {
		return nil, err
	}
	order := make([]int, len(bag))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool { return bag[order[i]].Start < bag[order[j]].Start })
	clock := make([]float64, len(order))
	for i, at := range order {
		clock[i] = uniqueness[at]
	}
	decay, err := UniquenessTimeDecay(clock, decayC)
	if err != nil {
		return nil, err
	}
	for i, at := range order {
		weights[at] *= decay[i]
	}
	return weights, nil
}

// TrainSequentialBaggedClassifier accepts only mature event spans and closes
// visible at fit time. MaxWork bounds conservative sampling/span visits and
// split comparisons before fitting. No class substitution or global fixed
// sample weights is applied; each bag re-estimates the attribution itself.
func TrainSequentialBaggedClassifier(features [][]float64, labels []int, close []float64, spans []EventSpan, config SequentialBaggingConfig) (*SequentialBaggedClassifier, error) {
	if len(features) == 0 || len(features) != len(labels) || len(spans) != len(labels) || len(features[0]) == 0 {
		return nil, errors.New("statistics: invalid sequential classifier dimensions")
	}
	if err := afmlValidateSpans(len(close), spans); err != nil {
		return nil, err
	}
	if config.Trees < 1 || config.Trees > 256 || config.MaxDepth < 1 || config.MaxDepth > 16 || config.MinSamplesLeaf < 1 || !afmlFinite(config.SampleFraction) || config.SampleFraction <= 0 || config.SampleFraction > 1 || !afmlFinite(config.DecayC) || config.DecayC <= -1 || config.DecayC > 1 || config.MaxWork < 1 {
		return nil, errors.New("statistics: invalid sequential classifier configuration")
	}
	columns := len(features[0])
	spanVisits := 0.
	for i, row := range features {
		if len(row) != columns || labels[i] < -1 || labels[i] > 1 {
			return nil, errors.New("statistics: classifier requires rectangular finite features and labels -1,0,1")
		}
		for _, value := range row {
			if !afmlFinite(value) {
				return nil, errors.New("statistics: nonfinite sequential classifier feature")
			}
		}
		spanVisits += float64(spans[i].End - spans[i].Start + 1)
	}
	for _, price := range close {
		if !afmlFinite(price) || price <= 0 {
			return nil, errors.New("statistics: sequential classifier requires positive finite prices")
		}
	}
	drawCount := int(math.Ceil(float64(len(labels)) * config.SampleFraction))
	work := float64(config.Trees) * (float64(drawCount)*spanVisits + float64(drawCount*columns*config.MaxDepth)*math.Ceil(math.Log2(float64(drawCount)+1)) + float64(4*len(close)+drawCount))
	if work > float64(config.MaxWork) {
		return nil, errors.New("statistics: sequential classifier exceeds work budget")
	}
	model := &SequentialBaggedClassifier{columns: columns}
	rng := rand.New(rand.NewSource(config.Seed))
	for tree := 0; tree < config.Trees; tree++ {
		draws, err := SequentialBootstrap(len(close), spans, drawCount, rng.Int63())
		if err != nil {
			return nil, err
		}
		weights, err := BootstrapReturnWeights(close, spans, draws, config.DecayC)
		if err != nil {
			return nil, err
		}
		model.audit = append(model.audit, BaggingAudit{append([]int(nil), draws...), append([]float64(nil), weights...)})
		var rows []int
		for i, w := range weights {
			if w > 0 {
				rows = append(rows, i)
			}
		}
		if len(rows) == 0 {
			return nil, errors.New("statistics: sampled bag has no positive attribution after decay")
		}
		var nodes []afmlClassNode
		var build func([]int, int) int
		build = func(rows []int, depth int) int {
			mass := [3]float64{}
			for _, at := range rows {
				mass[labels[draws[at]]+1] += weights[at]
			}
			total := mass[0] + mass[1] + mass[2]
			node := afmlClassNode{Feature: -1, Left: -1, Right: -1}
			for i := range mass {
				node.Probability[i] = mass[i] / total
			}
			index := len(nodes)
			nodes = append(nodes, node)
			parent := entropy3(mass)
			if depth >= config.MaxDepth || len(rows)/2 < config.MinSamplesLeaf || parent == 0 {
				return index
			}
			bestGain, bestFeature, threshold := 0., -1, 0.
			for feature := 0; feature < columns; feature++ {
				ordered := append([]int(nil), rows...)
				sort.SliceStable(ordered, func(i, j int) bool {
					return features[draws[ordered[i]]][feature] < features[draws[ordered[j]]][feature]
				})
				left := [3]float64{}
				for pos := 0; pos < len(ordered)-1; pos++ {
					at := ordered[pos]
					left[labels[draws[at]]+1] += weights[at]
					if pos+1 < config.MinSamplesLeaf || len(ordered)-pos-1 < config.MinSamplesLeaf {
						continue
					}
					a, b := features[draws[at]][feature], features[draws[ordered[pos+1]]][feature]
					if a == b {
						continue
					}
					right := [3]float64{}
					for i := range right {
						right[i] = math.Max(0, mass[i]-left[i])
					}
					lm, rm := left[0]+left[1]+left[2], right[0]+right[1]+right[2]
					if lm <= 0 || rm <= 0 {
						continue
					}
					gain := parent - lm/total*entropy3(left) - rm/total*entropy3(right)
					if gain > bestGain {
						bestGain, bestFeature, threshold = gain, feature, a/2+b/2
						if threshold >= b {
							threshold = a
						}
					}
				}
			}
			if bestFeature < 0 {
				return index
			}
			var left, right []int
			for _, at := range rows {
				if features[draws[at]][bestFeature] <= threshold {
					left = append(left, at)
				} else {
					right = append(right, at)
				}
			}
			li, ri := build(left, depth+1), build(right, depth+1)
			nodes[index].Feature, nodes[index].Threshold, nodes[index].Left, nodes[index].Right = bestFeature, threshold, li, ri
			return index
		}
		build(rows, 0)
		model.trees = append(model.trees, nodes)
	}
	return model, nil
}

func (m *SequentialBaggedClassifier) PredictProbabilities(features []float64) ([3]float64, error) {
	out := [3]float64{}
	if m == nil || len(m.trees) == 0 || len(features) != m.columns {
		return out, errors.New("statistics: sequential prediction dimensions mismatch")
	}
	for _, value := range features {
		if !afmlFinite(value) {
			return out, errors.New("statistics: sequential prediction must be finite")
		}
	}
	for _, tree := range m.trees {
		at := 0
		for tree[at].Feature >= 0 {
			node := tree[at]
			if features[node.Feature] <= node.Threshold {
				at = node.Left
			} else {
				at = node.Right
			}
		}
		for i, p := range tree[at].Probability {
			out[i] += p / float64(len(m.trees))
		}
	}
	return out, nil
}

func (m *SequentialBaggedClassifier) Audits() []BaggingAudit {
	if m == nil {
		return nil
	}
	out := make([]BaggingAudit, len(m.audit))
	for i, bag := range m.audit {
		out[i] = BaggingAudit{append([]int(nil), bag.Draws...), append([]float64(nil), bag.Weights...)}
	}
	return out
}
