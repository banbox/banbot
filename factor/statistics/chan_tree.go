package statistics

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
)

type CARTConfig struct {
	MinLeaf, MaxDepth, MaxSplits, MaxFeatures int
	Seed                                      int64
	Criterion                                 string
}
type cartNode struct {
	Feature               int
	Threshold, Prediction float64
	Left, Right           *cartNode
}
type CART struct {
	root      *cartNode
	features  int
	Low, High float64
	Splits    int
}

func validateCART(x [][]float64, y []float64, c CARTConfig) error {
	if len(x) == 0 || len(x) != len(y) || len(x[0]) == 0 || c.MinLeaf < 1 || c.MaxDepth < 1 || c.MaxDepth > 64 || c.MaxSplits < 1 || c.MaxFeatures < 1 || c.MaxFeatures > len(x[0]) || (c.Criterion != "sse" && c.Criterion != "gini") {
		return fmt.Errorf("statistics: invalid CART data/configuration")
	}
	for i, row := range x {
		if len(row) != len(x[0]) || !finite(y[i]) || (c.Criterion == "gini" && y[i] != 0 && y[i] != 1) {
			return fmt.Errorf("statistics: invalid CART labels/dimensions")
		}
		for _, v := range row {
			if !finite(v) {
				return fmt.Errorf("statistics: nonfinite CART feature")
			}
		}
	}
	return nil
}

// FitCART minimizes child SSE; for binary labels this is exactly proportional
// to weighted Gini impurity. Thresholds use adjacent distinct feature midpoints.
func FitCART(x [][]float64, y []float64, c CARTConfig) (*CART, error) {
	if err := validateCART(x, y, c); err != nil {
		return nil, err
	}
	labelMean, labelScale, err := MeanStd(y, 0)
	if err != nil {
		return nil, err
	}
	if labelScale == 0 {
		labelScale = 1
	}
	labels := make([]float64, len(y))
	originalLabels := y
	for i, v := range y {
		labels[i] = (v - labelMean) / labelScale
	}
	y = labels
	model := &CART{features: len(x[0]), Low: math.Inf(1), High: math.Inf(-1)}
	r := rand.New(rand.NewSource(c.Seed))
	indices := make([]int, len(y))
	for i := range indices {
		indices[i] = i
	}
	var build func([]int, int) *cartNode
	build = func(rows []int, depth int) *cartNode {
		mean, sumSquares := 0.0, 0.0
		for _, i := range rows {
			mean += y[i] / float64(len(rows))
			sumSquares += y[i] * y[i]
		}
		node := &cartNode{Feature: -1, Prediction: labelMean + labelScale*mean}
		if c.Criterion == "gini" {
			positive := 0
			for _, i := range rows {
				if originalLabels[i] == 1 {
					positive++
				}
			}
			node.Prediction = float64(positive) / float64(len(rows))
		}
		parentLoss := math.Max(0, sumSquares-mean*mean*float64(len(rows)))
		if depth < c.MaxDepth && len(rows) >= 2*c.MinLeaf && model.Splits < c.MaxSplits && parentLoss > 0 {
			features := r.Perm(model.features)[:c.MaxFeatures]
			slices.Sort(features)
			bestLoss := parentLoss
			bestFeature := -1
			bestThreshold := 0.0
			for _, feature := range features {
				ordered := slices.Clone(rows)
				slices.SortStableFunc(ordered, func(a, b int) int {
					if x[a][feature] < x[b][feature] {
						return -1
					}
					if x[a][feature] > x[b][feature] {
						return 1
					}
					return 0
				})
				total := mean * float64(len(rows))
				leftSum, leftSquares := 0.0, 0.0
				for split := 1; split < len(ordered); split++ {
					v := y[ordered[split-1]]
					leftSum += v
					leftSquares += v * v
					if split < c.MinLeaf || len(ordered)-split < c.MinLeaf || x[ordered[split-1]][feature] == x[ordered[split]][feature] {
						continue
					}
					rightSum := total - leftSum
					loss := math.Max(0, leftSquares-leftSum*leftSum/float64(split)) + math.Max(0, sumSquares-leftSquares-rightSum*rightSum/float64(len(rows)-split))
					if loss < bestLoss-1e-14*math.Max(1, parentLoss) {
						bestLoss, bestFeature = loss, feature
						bestThreshold = x[ordered[split-1]][feature]/2 + x[ordered[split]][feature]/2
					}
				}
			}
			if bestFeature >= 0 {
				var left, right []int
				for _, i := range rows {
					if x[i][bestFeature] <= bestThreshold {
						left = append(left, i)
					} else {
						right = append(right, i)
					}
				}
				node.Feature, node.Threshold = bestFeature, bestThreshold
				model.Splits++
				node.Left, node.Right = build(left, depth+1), build(right, depth+1)
				return node
			}
		}
		model.Low = math.Min(model.Low, node.Prediction)
		model.High = math.Max(model.High, node.Prediction)
		return node
	}
	model.root = build(indices, 0)
	return model, nil
}
func (m *CART) Predict(row []float64) (float64, error) {
	if m == nil || m.root == nil || len(row) != m.features {
		return 0, fmt.Errorf("statistics: invalid CART prediction dimensions")
	}
	for _, v := range row {
		if !finite(v) {
			return 0, fmt.Errorf("statistics: nonfinite CART prediction")
		}
	}
	node := m.root
	for node.Feature >= 0 {
		if row[node.Feature] <= node.Threshold {
			node = node.Left
		} else {
			node = node.Right
		}
	}
	return node.Prediction, nil
}

// SelectCARTFold reproduces the source choice of the single five-fold tree with
// the lowest validation MSE (classification error for Gini). This is not a refit
// on the complete trainset.
func SelectCARTFold(x [][]float64, y []float64, c CARTConfig, folds int) (*CART, float64, error) {
	if err := validateCART(x, y, c); err != nil {
		return nil, 0, err
	}
	if folds < 2 || folds > len(y) {
		return nil, 0, fmt.Errorf("statistics: invalid CART folds")
	}
	permutation := rand.New(rand.NewSource(c.Seed)).Perm(len(y))
	bestLoss := math.Inf(1)
	var best *CART
	for fold := 0; fold < folds; fold++ {
		var trainX, testX [][]float64
		var trainY, testY []float64
		for j, i := range permutation {
			if j%folds == fold {
				testX = append(testX, x[i])
				testY = append(testY, y[i])
			} else {
				trainX = append(trainX, x[i])
				trainY = append(trainY, y[i])
			}
		}
		model, err := FitCART(trainX, trainY, c)
		if err != nil {
			return nil, 0, err
		}
		loss := 0.0
		for i, row := range testX {
			p, _ := model.Predict(row)
			if c.Criterion == "gini" {
				if p > .5 {
					p = 1
				} else {
					p = 0
				}
			}
			loss += (p - testY[i]) * (p - testY[i]) / float64(len(testY))
		}
		if loss < bestLoss {
			best, bestLoss = model, loss
		}
	}
	return best, bestLoss, nil
}

type TreeEnsembleConfig struct {
	Tree             CARTConfig
	Trees            int
	Bootstrap, Boost bool
	LearningRate     float64
	Seed             int64
}
type TreeEnsemble struct {
	trees        []*CART
	boost        bool
	rate, offset float64
}

func FitTreeEnsemble(x [][]float64, y []float64, c TreeEnsembleConfig) (*TreeEnsemble, error) {
	if err := validateCART(x, y, c.Tree); err != nil {
		return nil, err
	}
	if c.Trees < 1 || c.Trees > 10000 || (c.Boost && c.Bootstrap) || !finite(c.LearningRate) || (c.Boost && (c.LearningRate <= 0 || c.LearningRate > 1)) {
		return nil, fmt.Errorf("statistics: invalid tree ensemble configuration")
	}
	m := &TreeEnsemble{boost: c.Boost, rate: c.LearningRate}
	r := rand.New(rand.NewSource(c.Seed))
	residual := slices.Clone(y)
	if c.Boost {
		for _, v := range y {
			m.offset += v / float64(len(y))
		}
		for i := range residual {
			residual[i] -= m.offset
		}
	}
	for k := 0; k < c.Trees; k++ {
		features, labels := x, y
		if c.Boost {
			labels = residual
		} else if c.Bootstrap {
			features = make([][]float64, len(y))
			labels = make([]float64, len(y))
			for i := range labels {
				idx := r.Intn(len(y))
				features[i], labels[i] = x[idx], y[idx]
			}
		}
		treeConfig := c.Tree
		treeConfig.Seed = r.Int63()
		tree, err := FitCART(features, labels, treeConfig)
		if err != nil {
			return nil, err
		}
		m.trees = append(m.trees, tree)
		if c.Boost {
			for i, row := range x {
				p, _ := tree.Predict(row)
				residual[i] -= c.LearningRate * p
			}
		}
	}
	return m, nil
}
func (m *TreeEnsemble) Predict(row []float64) (float64, error) {
	if m == nil || len(m.trees) == 0 {
		return 0, fmt.Errorf("statistics: empty tree ensemble")
	}
	out := m.offset
	for _, tree := range m.trees {
		p, err := tree.Predict(row)
		if err != nil {
			return 0, err
		}
		if m.boost {
			out += m.rate * p
		} else {
			out += p / float64(len(m.trees))
		}
	}
	if !finite(out) {
		return 0, fmt.Errorf("statistics: tree ensemble overflow")
	}
	return out, nil
}
