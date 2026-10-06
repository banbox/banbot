package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
)

// ModelBuilderConfig declares the exact training-data manifest expected at
// inference. Artifacts may contain successive rolling-window versions; their
// training and publication boundaries determine selection, never future scores.
type ModelBuilderConfig struct {
	ManifestID string
	Artifacts  []research.ModelArtifact
}

func RegisterModelPortfolioBuilder(name string, config ModelBuilderConfig) error {
	if config.ManifestID == "" || len(config.Artifacts) == 0 {
		return errors.New("runner: model builder needs training manifest and artifacts")
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return err
	}
	var frozen ModelBuilderConfig
	if err = json.Unmarshal(raw, &frozen); err != nil {
		return err
	}
	for _, artifact := range frozen.Artifacts {
		if artifact.ManifestID != frozen.ManifestID {
			return errors.New("runner: model builder artifact manifest mismatch")
		}
		if _, err := research.RestoreModel(artifact, max(artifact.AvailableAt, artifact.Window.End+1)); err != nil {
			return err
		}
	}
	slices.SortFunc(frozen.Artifacts, func(a, b research.ModelArtifact) int {
		if a.AvailableAt < b.AvailableAt {
			return -1
		}
		if a.AvailableAt > b.AvailableAt {
			return 1
		}
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	raw, err = json.Marshal(frozen)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(sum[:])
	err = RegisterPortfolioBuilder(name, func(frame factor.Frame, universe factor.Universe, spec factor.PortfolioSpec, definition research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		if frame.SnapshotID != spec.SnapshotID || frame.PlanHash != spec.FactorPlanHash || frame.DecisionTime != spec.DecisionTime || universe.Version != spec.UniverseVersion {
			return nil, nil, errors.New("runner: model builder frame identity mismatch")
		}
		var selected *research.ModelArtifact
		for i := range frozen.Artifacts {
			artifact := &frozen.Artifacts[i]
			if artifact.AvailableAt <= frame.DecisionTime && artifact.Window.End < frame.DecisionTime {
				selected = artifact
			}
		}
		if selected == nil {
			return nil, []factor.Diagnostic{{Code: "model-unavailable", Detail: "no model artifact is matured and published at this decision"}}, nil
		}
		predictor, err := research.RestoreModel(*selected, frame.DecisionTime)
		if err != nil {
			return nil, nil, err
		}
		scores := map[int32]factor.Numeric{}
		for _, sid := range universe.Investable {
			features := make([]float64, len(selected.Features))
			status := factor.Valid
			for i, column := range selected.Features {
				v, exists := frame.Values[column][sid]
				if !exists {
					status = factor.Missing
					break
				}
				if v.Validity != factor.Valid {
					status = v.Validity
					break
				}
				if math.IsNaN(v.Value) || math.IsInf(v.Value, 0) {
					status = factor.NonFinite
					break
				}
				features[i] = v.Value
			}
			if status != factor.Valid {
				scores[sid] = factor.Numeric{Validity: status}
				continue
			}
			value, err := predictor.Predict(features)
			if err != nil {
				return nil, nil, err
			}
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, nil, errors.New("runner: nonfinite model output")
			}
			scores[sid] = factor.Numeric{Value: value, Validity: factor.Valid}
		}
		prediction := factor.CloneFrame(frame)
		prediction.Values["model-score"] = scores
		diagnostics := []factor.Diagnostic{{Code: "model-inference-v1", Detail: fmt.Sprintf("artifact=%s builder=%s trained-through=%d published=%d", selected.ID, fingerprint, selected.Window.End, selected.AvailableAt)}}
		spec.Diagnostics = append(spec.Diagnostics, diagnostics...)
		var target *factor.TargetPortfolio
		var selection []factor.Diagnostic
		if definition.Policy != "" {
			policy, policyErr := definition.PolicyConfig()
			if policyErr != nil {
				return nil, nil, policyErr
			}
			target, selection, err = factor.SelectPortfolioScore(prediction, universe, spec, policy, "model-score", nil)
		} else {
			target, selection, err = factor.TopBottomKNotional(prediction, "model-score", universe, spec, definition.K, definition.LongNotional, definition.ShortNotional)
		}
		return target, append(diagnostics, selection...), err
	})
	if err != nil {
		return err
	}
	return RegisterPortfolioBuilderIdentity(name, fingerprint)
}
