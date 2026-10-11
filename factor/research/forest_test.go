package research

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestForestFitArtifactsRespectMaturityAndPublication(t *testing.T) {
	var rows []TrainingSample
	for i := int64(1); i <= 80; i++ {
		label := float64(i % 2)
		rows = append(rows, TrainingSample{SID: 1, DecisionTime: i * 10, FeaturesAvailableAt: i * 10, LabelEnd: i*10 + 1, MatureAt: i*10 + 1, AvailableAt: i*10 + 1, Features: []float64{label}, Label: label})
	}
	w := TrainingWindow{Start: 1, End: 1000, FitAsOf: 1000, TestStart: 1001, TestEnd: 1100, MinSamples: 40}
	params := json.RawMessage(`{"Trees":16,"MaxDepth":3,"MinSamplesLeaf":1,"MaxFeatures":1,"Seed":19}`)
	a, p, err := FitModel(RandomForestModel, "manifest-v1", []string{"primary-quality"}, params, rows, w, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []float64{0, 1} {
		pred, err := p.Predict([]float64{x})
		if err != nil || pred != x {
			t.Fatalf("binary learner predict %g: %g %v", x, pred, err)
		}
	}
	if _, err = RestoreModel(a, 999); err == nil {
		t.Fatal("used model before publication")
	}
	restored, err := RestoreModel(a, 1001)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, a.Payload) {
		t.Fatal("restored forest differs")
	}
	// A future-label row must not change model training or serialized trees.
	rows = append(rows, TrainingSample{SID: 1, DecisionTime: 900, FeaturesAvailableAt: 900, LabelEnd: 1200, MatureAt: 1200, AvailableAt: 1200, Features: []float64{0}, Label: 1})
	b, _, err := FitModel(RandomForestModel, "manifest-v1", []string{"primary-quality"}, params, rows, w, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID || !bytes.Equal(a.Payload, b.Payload) {
		t.Fatal("immature future label leaked into forest")
	}
}

func TestForestRejectsContinuousLabelsAndUnknownParameters(t *testing.T) {
	tt := forestTrainer{}
	if _, err := tt.Fit([]TrainingSample{{Features: []float64{1}, Label: .5}}); err == nil {
		t.Fatal("continuous label accepted as binary")
	}
	modelRegistry.RLock()
	factory := modelRegistry.factories[RandomForestModel]
	modelRegistry.RUnlock()
	for _, raw := range []string{`{"Typo":1}`, `{} {}`} {
		if _, err := factory(json.RawMessage(raw)); err == nil {
			t.Fatal("bad forest config accepted")
		}
	}
}
