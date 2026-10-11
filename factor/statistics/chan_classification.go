package statistics

import (
	"fmt"
	"math/rand"
	"slices"
)

type ClassificationEnsembleConfig struct {
	Tree                    CARTConfig
	Trees, SubspaceFeatures int
	Seed                    int64
	ProbabilityVote         bool
}
type classificationTree struct {
	tree     *CART
	features []int
}
type ClassificationEnsemble struct {
	trees           []classificationTree
	dimensions      int
	probabilityVote bool
}

// FitClassificationEnsemble contrasts bootstrap/node-wise random forests with
// fixed random feature subspaces per learner. Binary leaf responses are hard
// classes by default; probability averaging is a distinct explicit convention.
func FitClassificationEnsemble(x [][]float64, y []float64, c ClassificationEnsembleConfig) (*ClassificationEnsemble, error) {
	c.Tree.Criterion = "gini"
	if err := validateCART(x, y, c.Tree); err != nil {
		return nil, err
	}
	if c.Trees < 1 || c.Trees > 1000 || c.SubspaceFeatures < 0 || c.SubspaceFeatures > len(x[0]) {
		return nil, fmt.Errorf("statistics: invalid classification ensemble budget/subspace")
	}
	m := &ClassificationEnsemble{dimensions: len(x[0]), probabilityVote: c.ProbabilityVote}
	rng := rand.New(rand.NewSource(c.Seed))
	for i := 0; i < c.Trees; i++ {
		features := make([]int, len(x[0]))
		for j := range features {
			features[j] = j
		}
		spec := c.Tree
		spec.Seed = c.Seed + int64(i) + 1
		var rows [][]float64
		var labels []float64
		if c.SubspaceFeatures > 0 {
			features = rng.Perm(len(x[0]))[:c.SubspaceFeatures]
			slices.Sort(features)
			spec.MaxFeatures = len(features)
			rows = make([][]float64, len(x))
			labels = slices.Clone(y)
			for j, row := range x {
				rows[j] = make([]float64, len(features))
				for k, feature := range features {
					rows[j][k] = row[feature]
				}
			}
		} else {
			rows = make([][]float64, len(x))
			labels = make([]float64, len(y))
			for j := range rows {
				index := rng.Intn(len(x))
				rows[j], labels[j] = x[index], y[index]
			}
		}
		tree, err := FitCART(rows, labels, spec)
		if err != nil {
			return nil, err
		}
		m.trees = append(m.trees, classificationTree{tree, slices.Clone(features)})
	}
	return m, nil
}
func (m *ClassificationEnsemble) Predict(x []float64) (float64, error) {
	if m == nil || len(x) != m.dimensions || len(m.trees) == 0 {
		return 0, fmt.Errorf("statistics: invalid classification ensemble prediction")
	}
	for _, v := range x {
		if !finite(v) {
			return 0, fmt.Errorf("statistics: nonfinite classification predictor")
		}
	}
	votes := 0.0
	for _, tree := range m.trees {
		row := make([]float64, len(tree.features))
		for i, feature := range tree.features {
			row[i] = x[feature]
		}
		v, err := tree.tree.Predict(row)
		if err != nil {
			return 0, err
		}
		if !m.probabilityVote {
			if v > .5 {
				v = 1
			} else {
				v = 0
			}
		}
		votes += v
	}
	return votes / float64(len(m.trees)), nil
}
