package statistics

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
)

type NeuralConfig struct {
	Hidden                      []int
	Epochs, Patience, Networks  int
	LearningRate, TrainFraction float64
	Seed                        int64
	Average                     bool
	Activation                  string
	ValidationOnly              bool
}
type neuralLayer struct{ Weights [][]float64 }
type NeuralNetwork struct {
	layers                    []neuralLayer
	center, scale             []float64
	targetCenter, targetScale float64
	ValidationLoss            float64
	Epochs                    int
	activation                string
}
type NeuralEnsemble struct {
	Models  []*NeuralNetwork
	Average bool
	Best    int
}

func neuralForward(layers []neuralLayer, row []float64, activation string) [][]float64 {
	activations := [][]float64{row}
	for l, layer := range layers {
		input := activations[len(activations)-1]
		out := make([]float64, len(layer.Weights))
		for j, w := range layer.Weights {
			out[j] = w[len(input)]
			for k, v := range input {
				out[j] += w[k] * v
			}
			if l < len(layers)-1 {
				if activation == "tanh" {
					out[j] = math.Tanh(out[j])
				} else if out[j] >= 0 {
					out[j] = 1 / (1 + math.Exp(-out[j]))
				} else {
					e := math.Exp(out[j])
					out[j] = e / (1 + e)
				}
			}
		}
		activations = append(activations, out)
	}
	return activations
}
func neuralClone(layers []neuralLayer) []neuralLayer {
	out := make([]neuralLayer, len(layers))
	for i, l := range layers {
		out[i].Weights = make([][]float64, len(l.Weights))
		for j, w := range l.Weights {
			out[i].Weights[j] = slices.Clone(w)
		}
	}
	return out
}
func (m *NeuralNetwork) Predict(x []float64) (float64, error) {
	if m == nil || len(x) != len(m.center) {
		return 0, fmt.Errorf("statistics: invalid neural prediction")
	}
	row := make([]float64, len(x))
	for j, v := range x {
		if !finite(v) {
			return 0, fmt.Errorf("statistics: nonfinite neural predictor")
		}
		row[j] = (v - m.center[j]) / m.scale[j]
	}
	a := neuralForward(m.layers, row, m.activation)
	v := m.targetCenter + m.targetScale*a[len(a)-1][0]
	if !finite(v) {
		return 0, fmt.Errorf("statistics: neural prediction overflow")
	}
	return v, nil
}

// FitNeuralNetwork uses logistic sigmoid hidden units (optional explicit tanh), linear output and seeded
// full-batch Adam. Random training/validation separation, training-only min/max
// normalization and restoration of the best validation checkpoint are explicit.
// Adam is a local optimizer choice, not MATLAB Levenberg-Marquardt equivalence.
func FitNeuralNetwork(x [][]float64, y []float64, c NeuralConfig) (*NeuralNetwork, error) {
	if c.Activation == "" {
		c.Activation = "logistic"
	}
	if c.Activation != "logistic" && c.Activation != "tanh" {
		return nil, fmt.Errorf("statistics: invalid neural activation")
	}
	if len(c.Hidden) == 0 {
		c.Hidden = []int{1}
	}
	if c.Epochs == 0 {
		c.Epochs = 1000
	}
	if c.Patience == 0 {
		c.Patience = 1
	}
	if c.LearningRate == 0 {
		c.LearningRate = .01
	}
	if c.TrainFraction == 0 {
		c.TrainFraction = .6
	}
	if len(x) < 5 || len(x) != len(y) || len(x[0]) == 0 || len(x) > 100000 || len(c.Hidden) > 3 || c.Epochs < 1 || c.Epochs > 10000 || c.Patience < 1 || !finite(c.LearningRate) || c.LearningRate <= 0 || c.LearningRate > 1 || !finite(c.TrainFraction) || c.TrainFraction <= 0 || c.TrainFraction >= 1 {
		return nil, fmt.Errorf("statistics: invalid neural sample/configuration")
	}
	sizes := append([]int{len(x[0])}, c.Hidden...)
	sizes = append(sizes, 1)
	budget := 0
	for i := 1; i < len(sizes); i++ {
		if sizes[i] < 1 || sizes[i] > 64 {
			return nil, fmt.Errorf("statistics: invalid neural architecture")
		}
		budget += sizes[i] * (sizes[i-1] + 1)
	}
	if budget > 10000 {
		return nil, fmt.Errorf("statistics: neural parameter budget exceeded")
	}
	for i, row := range x {
		if len(row) != len(x[0]) || !finite(y[i]) {
			return nil, fmt.Errorf("statistics: invalid neural data")
		}
		for _, v := range row {
			if !finite(v) {
				return nil, fmt.Errorf("statistics: nonfinite neural feature")
			}
		}
	}
	rng := rand.New(rand.NewSource(c.Seed))
	perm := rng.Perm(len(y))
	trainCount := int(math.Round(c.TrainFraction * float64(len(y))))
	if trainCount < 2 || trainCount >= len(y) {
		return nil, fmt.Errorf("statistics: invalid neural split")
	}
	train, validation := perm[:trainCount], perm[trainCount:]
	m := &NeuralNetwork{center: make([]float64, len(x[0])), scale: make([]float64, len(x[0])), activation: c.Activation}
	for j := range m.center {
		lo, hi := math.Inf(1), math.Inf(-1)
		for _, i := range train {
			lo = math.Min(lo, x[i][j])
			hi = math.Max(hi, x[i][j])
		}
		m.center[j] = (lo + hi) / 2
		m.scale[j] = (hi - lo) / 2
		if m.scale[j] == 0 {
			m.scale[j] = 1
		}
		if !finite(m.center[j]) || !finite(m.scale[j]) {
			return nil, fmt.Errorf("statistics: neural scaling overflow")
		}
	}
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, i := range train {
		lo = math.Min(lo, y[i])
		hi = math.Max(hi, y[i])
	}
	m.targetCenter = (lo + hi) / 2
	m.targetScale = (hi - lo) / 2
	if m.targetScale == 0 {
		m.targetScale = 1
	}
	if !finite(m.targetCenter) || !finite(m.targetScale) {
		return nil, fmt.Errorf("statistics: neural target scaling overflow")
	}
	xnorm := make([][]float64, len(x))
	ynorm := make([]float64, len(y))
	for i, row := range x {
		xnorm[i] = make([]float64, len(row))
		for j, v := range row {
			xnorm[i][j] = (v - m.center[j]) / m.scale[j]
		}
		ynorm[i] = (y[i] - m.targetCenter) / m.targetScale
	}
	layers := make([]neuralLayer, len(sizes)-1)
	for l := range layers {
		layers[l].Weights = make([][]float64, sizes[l+1])
		for j := range layers[l].Weights {
			layers[l].Weights[j] = make([]float64, sizes[l]+1)
			for k := 0; k < sizes[l]; k++ {
				layers[l].Weights[j][k] = rng.NormFloat64() / math.Sqrt(float64(sizes[l]))
			}
		}
	}
	zeros := func() []neuralLayer {
		z := neuralClone(layers)
		for l := range z {
			for j := range z[l].Weights {
				clear(z[l].Weights[j])
			}
		}
		return z
	}
	moment, variance := zeros(), zeros()
	loss := func(rows []int) float64 {
		value := 0.0
		for _, i := range rows {
			a := neuralForward(layers, xnorm[i], c.Activation)
			d := a[len(a)-1][0] - ynorm[i]
			value += d * d / float64(len(rows))
		}
		return value
	}
	bestLoss := loss(validation)
	best := neuralClone(layers)
	bad := 0
	for epoch := 1; epoch <= c.Epochs; epoch++ {
		gradient := zeros()
		for _, i := range train {
			a := neuralForward(layers, xnorm[i], c.Activation)
			delta := []float64{2 * (a[len(a)-1][0] - ynorm[i]) / float64(len(train))}
			for l := len(layers) - 1; l >= 0; l-- {
				previous := make([]float64, len(a[l]))
				for j, d := range delta {
					for k, v := range a[l] {
						gradient[l].Weights[j][k] += d * v
						previous[k] += d * layers[l].Weights[j][k]
					}
					gradient[l].Weights[j][len(a[l])] += d
				}
				if l > 0 {
					for k := range previous {
						if c.Activation == "tanh" {
							previous[k] *= 1 - a[l][k]*a[l][k]
						} else {
							previous[k] *= a[l][k] * (1 - a[l][k])
						}
					}
				}
				delta = previous
			}
		}
		for l := range layers {
			for j, w := range layers[l].Weights {
				for k := range w {
					g := gradient[l].Weights[j][k]
					moment[l].Weights[j][k] = .9*moment[l].Weights[j][k] + .1*g
					variance[l].Weights[j][k] = .999*variance[l].Weights[j][k] + .001*g*g
					adjustedM := moment[l].Weights[j][k] / (1 - math.Pow(.9, float64(epoch)))
					adjustedV := variance[l].Weights[j][k] / (1 - math.Pow(.999, float64(epoch)))
					w[k] -= c.LearningRate * adjustedM / (math.Sqrt(adjustedV) + 1e-8)
				}
			}
		}
		v := loss(validation)
		if !finite(v) {
			return nil, fmt.Errorf("statistics: neural optimizer overflow")
		}
		m.Epochs = epoch
		if v < bestLoss {
			bestLoss, best, bad = v, neuralClone(layers), 0
		} else {
			bad++
		}
		if !c.ValidationOnly && bad >= c.Patience {
			break
		}
	}
	if c.ValidationOnly {
		best, bestLoss = neuralClone(layers), loss(validation)
	}
	m.layers = best
	m.ValidationLoss = bestLoss * m.targetScale * m.targetScale
	return m, nil
}
func FitNeuralEnsemble(x [][]float64, y []float64, c NeuralConfig) (*NeuralEnsemble, error) {
	if c.Networks == 0 {
		c.Networks = 100
	}
	if c.Networks < 1 || c.Networks > 1000 {
		return nil, fmt.Errorf("statistics: invalid neural ensemble count")
	}
	e := &NeuralEnsemble{Average: c.Average}
	bestLoss := math.Inf(1)
	for i := 0; i < c.Networks; i++ {
		spec := c
		spec.ValidationOnly = true
		spec.Seed += int64(i)
		m, err := FitNeuralNetwork(x, y, spec)
		if err != nil {
			return nil, err
		}
		if m.ValidationLoss < bestLoss {
			bestLoss, e.Best = m.ValidationLoss, i
		}
		e.Models = append(e.Models, m)
	}
	return e, nil
}
func (e *NeuralEnsemble) Predict(x []float64) (float64, error) {
	if e == nil || len(e.Models) == 0 || e.Best < 0 || e.Best >= len(e.Models) {
		return 0, fmt.Errorf("statistics: invalid neural ensemble")
	}
	if !e.Average {
		return e.Models[e.Best].Predict(x)
	}
	v := 0.0
	for _, m := range e.Models {
		p, err := m.Predict(x)
		if err != nil {
			return 0, err
		}
		v += p / float64(len(e.Models))
	}
	if !finite(v) {
		return 0, fmt.Errorf("statistics: neural ensemble overflow")
	}
	return v, nil
}
