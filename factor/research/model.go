package research

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/banbox/banbot/factor"
)

type TrainingSample struct {
	SID                                                                int32
	DecisionTime, FeaturesAvailableAt, LabelEnd, MatureAt, AvailableAt int64
	Features                                                           []float64
	Label                                                              float64
}
type TrainingWindow struct {
	Start, End, FitAsOf, TestStart, TestEnd, EmbargoMS int64
	MinSamples                                         int
}

// VisibleTrainingRows purges labels reaching the test interval and applies a
// pre-test embargo. Feature publication must precede its original decision.
func VisibleTrainingRows(rows []TrainingSample, window TrainingWindow) ([]TrainingSample, error) {
	if window.Start <= 0 || window.End < window.Start || window.FitAsOf < window.End || window.TestStart <= window.FitAsOf || window.TestEnd < window.TestStart || window.EmbargoMS < 0 || window.MinSamples < 1 {
		return nil, errors.New("research: invalid rolling training window")
	}
	var result []TrainingSample
	for _, row := range rows {
		if row.DecisionTime < window.Start || row.DecisionTime > window.End {
			continue
		}
		if row.SID <= 0 || row.FeaturesAvailableAt <= 0 || row.FeaturesAvailableAt > row.DecisionTime || row.LabelEnd <= row.DecisionTime || row.MatureAt < row.LabelEnd || row.AvailableAt <= 0 {
			return nil, errors.New("research: invalid training sample chronology/features")
		}
		if row.MatureAt > window.FitAsOf || row.AvailableAt > window.FitAsOf || row.LabelEnd >= window.TestStart-window.EmbargoMS {
			continue
		}
		if len(row.Features) == 0 || !finiteValues(row.Features...) || !finiteValues(row.Label) {
			return nil, errors.New("research: invalid visible training features/label")
		}
		row.Features = slices.Clone(row.Features)
		result = append(result, row)
	}
	slices.SortFunc(result, func(a, b TrainingSample) int {
		if a.DecisionTime < b.DecisionTime {
			return -1
		}
		if a.DecisionTime > b.DecisionTime {
			return 1
		}
		if a.SID < b.SID {
			return -1
		}
		if a.SID > b.SID {
			return 1
		}
		return 0
	})
	for i := 1; i < len(result); i++ {
		if result[i].DecisionTime == result[i-1].DecisionTime && result[i].SID == result[i-1].SID {
			return nil, errors.New("research: duplicate training sample")
		}
	}
	if len(result) < window.MinSamples {
		return nil, errors.New("research: insufficient visible purged training samples")
	}
	return result, nil
}
func RollingWindows(start, end, trainMS, testMS, stepMS, embargoMS int64, minSamples int) ([]TrainingWindow, error) {
	if start <= 0 || end <= start || trainMS <= 0 || testMS <= 0 || stepMS <= 0 || embargoMS < 0 || minSamples < 1 {
		return nil, errors.New("research: invalid rolling schedule")
	}
	var result []TrainingWindow
	for testStart := start + trainMS + embargoMS; testStart <= end-testMS; testStart += stepMS {
		if len(result) >= 100000 {
			return nil, errors.New("research: rolling window budget exceeded")
		}
		trainEnd := testStart - embargoMS - 1
		result = append(result, TrainingWindow{Start: trainEnd - trainMS + 1, End: trainEnd, FitAsOf: trainEnd, TestStart: testStart, TestEnd: testStart + testMS - 1, EmbargoMS: embargoMS, MinSamples: minSamples})
	}
	return result, nil
}

// Predictor is inference-only. ModelFactory creates an independent trainer per
// fit; Restore must reconstruct the same immutable predictor from payload.
type Predictor interface {
	Predict([]float64) (float64, error)
	Snapshot() (json.RawMessage, error)
}
type Trainer interface {
	Fit([]TrainingSample) (Predictor, error)
	Restore(json.RawMessage) (Predictor, error)
}
type ModelFactory func(json.RawMessage) (Trainer, error)

var modelRegistry = struct {
	sync.RWMutex
	factories map[string]ModelFactory
}{factories: map[string]ModelFactory{}}

func RegisterModel(name string, factory ModelFactory) error {
	if name == "" || factory == nil {
		return errors.New("research: model requires versioned name and factory")
	}
	modelRegistry.Lock()
	defer modelRegistry.Unlock()
	if modelRegistry.factories[name] != nil {
		return errors.New("research: model already registered")
	}
	modelRegistry.factories[name] = factory
	return nil
}

type LinearModel struct{ Coefficients []float64 }

func (m *LinearModel) Predict(features []float64) (float64, error) {
	if len(m.Coefficients) != len(features)+1 || !finiteValues(features...) {
		return 0, errors.New("research: model feature mismatch")
	}
	value := m.Coefficients[0]
	for i, v := range features {
		value += m.Coefficients[i+1] * v
	}
	if !finiteValues(value) {
		return 0, errors.New("research: nonfinite prediction")
	}
	return value, nil
}
func (m *LinearModel) Snapshot() (json.RawMessage, error) { return json.Marshal(m) }

type linearTrainer struct{ Ridge float64 }

func (t linearTrainer) Fit(rows []TrainingSample) (Predictor, error) {
	if len(rows) == 0 || len(rows[0].Features) == 0 {
		return nil, errors.New("research: empty fit")
	}
	p := len(rows[0].Features) + 1
	x := make([][]float64, 0, len(rows)+p)
	y := make([]float64, 0, len(rows)+p)
	for _, row := range rows {
		if len(row.Features)+1 != p {
			return nil, errors.New("research: inconsistent training feature count")
		}
		x = append(x, append([]float64{1}, row.Features...))
		y = append(y, row.Label)
	}
	if t.Ridge > 0 {
		for j := 1; j < p; j++ {
			row := make([]float64, p)
			row[j] = math.Sqrt(t.Ridge)
			x = append(x, row)
			y = append(y, 0)
		}
	}
	coef, _, err := factor.LeastSquares(x, y, nil)
	if err != nil {
		return nil, err
	}
	return &LinearModel{coef}, nil
}
func (t linearTrainer) Restore(raw json.RawMessage) (Predictor, error) {
	var model LinearModel
	if err := json.Unmarshal(raw, &model); err != nil {
		return nil, err
	}
	if len(model.Coefficients) < 2 || !finiteValues(model.Coefficients...) {
		return nil, errors.New("research: invalid linear model payload")
	}
	return &model, nil
}
func init() {
	_ = RegisterModel("linear-ridge-v1", func(raw json.RawMessage) (Trainer, error) {
		var t linearTrainer
		if len(raw) > 0 {
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&t); err != nil {
				return nil, err
			}
		}
		if !finiteValues(t.Ridge) || t.Ridge < 0 {
			return nil, errors.New("research: invalid ridge")
		}
		return t, nil
	})
}

type ModelArtifact struct {
	ID                        string
	SchemaVersion             int
	Model, ManifestID         string
	Features                  []string
	Window                    TrainingWindow
	SampleCutoff, AvailableAt int64
	Samples                   int
	Parameters, Payload       json.RawMessage
}

func artifactID(artifact ModelArtifact) (string, error) { artifact.ID = ""; return hashJSON(artifact) }
func FitModel(name, manifest string, features []string, params json.RawMessage, rows []TrainingSample, window TrainingWindow, availableAt int64) (ModelArtifact, Predictor, error) {
	if manifest == "" || availableAt < window.FitAsOf || len(features) == 0 {
		return ModelArtifact{}, nil, errors.New("research: invalid model lineage/publication")
	}
	seen := map[string]bool{}
	for _, name := range features {
		if name == "" || seen[name] {
			return ModelArtifact{}, nil, errors.New("research: invalid model feature declaration")
		}
		seen[name] = true
	}
	visible, err := VisibleTrainingRows(rows, window)
	if err != nil {
		return ModelArtifact{}, nil, err
	}
	for _, row := range visible {
		if len(row.Features) != len(features) {
			return ModelArtifact{}, nil, errors.New("research: model feature dimensions mismatch")
		}
	}
	modelRegistry.RLock()
	factory := modelRegistry.factories[name]
	modelRegistry.RUnlock()
	if factory == nil {
		return ModelArtifact{}, nil, errors.New("research: unregistered model")
	}
	trainer, err := factory(slices.Clone(params))
	if err != nil {
		return ModelArtifact{}, nil, err
	}
	predictor, err := trainer.Fit(visible)
	if err != nil {
		return ModelArtifact{}, nil, err
	}
	payload, err := predictor.Snapshot()
	if err != nil {
		return ModelArtifact{}, nil, err
	}
	artifact := ModelArtifact{SchemaVersion: 1, Model: name, ManifestID: manifest, Features: slices.Clone(features), Window: window, AvailableAt: availableAt, Samples: len(visible), Parameters: slices.Clone(params), Payload: slices.Clone(payload)}
	for _, row := range visible {
		artifact.SampleCutoff = max(artifact.SampleCutoff, row.DecisionTime)
	}
	artifact.ID, err = artifactID(artifact)
	return artifact, predictor, err
}
func RestoreModel(artifact ModelArtifact, asof int64) (Predictor, error) {
	id, err := artifactID(artifact)
	if err != nil {
		return nil, err
	}
	w := artifact.Window
	if artifact.SchemaVersion != 1 || artifact.ID != id || artifact.ManifestID == "" || w.Start <= 0 || w.End < w.Start || w.FitAsOf < w.End || w.TestStart <= w.FitAsOf || w.TestEnd < w.TestStart || w.EmbargoMS < 0 || w.MinSamples < 1 || artifact.Samples < w.MinSamples || artifact.SampleCutoff < w.Start || len(artifact.Features) == 0 || artifact.AvailableAt < artifact.Window.FitAsOf || artifact.AvailableAt > asof || artifact.Window.End >= asof || artifact.SampleCutoff > artifact.Window.End {
		return nil, errors.New("research: model artifact corrupt or not PIT-visible")
	}
	seen := map[string]bool{}
	for _, feature := range artifact.Features {
		if feature == "" || seen[feature] {
			return nil, errors.New("research: invalid restored feature declaration")
		}
		seen[feature] = true
	}
	modelRegistry.RLock()
	factory := modelRegistry.factories[artifact.Model]
	modelRegistry.RUnlock()
	if factory == nil {
		return nil, errors.New("research: unregistered artifact model")
	}
	trainer, err := factory(slices.Clone(artifact.Parameters))
	if err != nil {
		return nil, err
	}
	return trainer.Restore(slices.Clone(artifact.Payload))
}

// PublishModel writes one immutable content-addressed file. A interrupted temp
// write leaves the published version intact; retry validates an existing file.
func PublishModel(directory string, artifact ModelArtifact) (string, error) {
	if _, err := RestoreModel(artifact, max(artifact.AvailableAt, artifact.Window.End+1)); err != nil {
		return "", err
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return "", err
	}
	path := filepath.Join(directory, artifact.ID+".json")
	data, err := json.Marshal(artifact)
	if err != nil {
		return "", err
	}
	if old, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(old, data) {
			return "", errors.New("research: published artifact conflicts")
		}
		return path, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	file, err := os.CreateTemp(directory, ".model-*.tmp")
	if err != nil {
		return "", err
	}
	temp := file.Name()
	defer os.Remove(temp)
	if _, err = file.Write(data); err != nil {
		file.Close()
		return "", err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return "", err
	}
	if err = file.Close(); err != nil {
		return "", err
	}
	if err = os.Rename(temp, path); err != nil {
		return "", err
	}
	return path, nil
}
func LoadModel(path string, asof int64) (ModelArtifact, Predictor, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ModelArtifact{}, nil, err
	}
	var artifact ModelArtifact
	if err = json.Unmarshal(data, &artifact); err != nil {
		return ModelArtifact{}, nil, err
	}
	predictor, err := RestoreModel(artifact, asof)
	if err != nil {
		return ModelArtifact{}, nil, fmt.Errorf("research: restore model: %w", err)
	}
	return artifact, predictor, nil
}
