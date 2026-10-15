package research

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/banbox/banbot/factor/statistics"
)

const RandomForestModel = "afml-random-forest-v1"

type forestTrainer struct{ config statistics.RandomForestConfig }
type forestPredictor struct{ model *statistics.RandomForest }

func (p *forestPredictor) Predict(features []float64) (float64, error) {
	return p.model.PredictProbability(features)
}
func (p *forestPredictor) Snapshot() (json.RawMessage, error) {
	raw, err := p.model.Snapshot()
	return json.RawMessage(raw), err
}

func (t forestTrainer) Fit(rows []TrainingSample) (Predictor, error) {
	x := make([][]float64, len(rows))
	y := make([]int, len(rows))
	for i, row := range rows {
		if row.Label != 0 && row.Label != 1 {
			return nil, errors.New("research: random forest requires binary meta labels")
		}
		x[i] = row.Features
		y[i] = int(row.Label)
	}
	m, err := statistics.TrainRandomForest(x, y, nil, t.config)
	if err != nil {
		return nil, err
	}
	return &forestPredictor{m}, nil
}
func (t forestTrainer) Restore(raw json.RawMessage) (Predictor, error) {
	m, err := statistics.RestoreRandomForest(raw)
	if err != nil {
		return nil, err
	}
	return &forestPredictor{m}, nil
}

func init() {
	if err := RegisterModel(RandomForestModel, func(raw json.RawMessage) (Trainer, error) {
		config := statistics.DefaultRandomForestConfig()
		if len(raw) > 0 {
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&config); err != nil {
				return nil, err
			}
			var extra any
			if err := decoder.Decode(&extra); err != io.EOF {
				return nil, errors.New("research: random forest parameters contain trailing JSON")
			}
		}
		return forestTrainer{config}, nil
	}); err != nil {
		panic(err)
	}
}
