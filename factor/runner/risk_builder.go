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

type RiskBuilderConfig struct {
	ScoreColumn, VolatilityColumn                                     string
	RiskAversion, Gross, MaxWeight, MaxNet, MaxBeta, VolatilityTarget float64
	LongOnly, LimitNet, LimitBeta                                     bool
	Groups                                                            map[int32]string
	GroupCaps                                                         map[string]float64
	Beta                                                              map[int32]float64
	// A pre-fitted covariance uses fixed asset ordering and explicit publication.
	SIDs                     []int32
	Covariance               [][]float64
	TrainingEnd, AvailableAt int64
}

// RegisterRiskPortfolioBuilder installs an immutable native Go builder. The
// selected registered name must be versioned with its configuration; the
// config hash is emitted in every target's diagnostic evidence. Turnover is a
// stateful policy constraint, so this stateless builder does not infer it.
func RegisterRiskPortfolioBuilder(name string, config RiskBuilderConfig) error {
	raw, err := json.Marshal(config)
	if err != nil {
		return err
	}
	var frozen RiskBuilderConfig
	if err = json.Unmarshal(raw, &frozen); err != nil {
		return err
	}
	if frozen.RiskAversion <= 0 || frozen.Gross <= 0 || math.IsNaN(frozen.RiskAversion) || math.IsInf(frozen.RiskAversion, 0) {
		return errors.New("runner: risk builder needs risk aversion and gross budget")
	}
	if frozen.ScoreColumn == "" {
		frozen.ScoreColumn = "score"
	}
	if frozen.VolatilityColumn == "" && len(frozen.Covariance) == 0 {
		return errors.New("runner: risk builder needs visible volatility or fitted covariance")
	}
	if len(frozen.Covariance) > 0 && (len(frozen.SIDs) != len(frozen.Covariance) || frozen.TrainingEnd <= 0 || frozen.AvailableAt < frozen.TrainingEnd) {
		return errors.New("runner: risk covariance requires PIT publication and asset identity")
	}
	indices := map[int32]int{}
	for i, sid := range frozen.SIDs {
		if sid <= 0 {
			return errors.New("runner: invalid risk SID")
		}
		if _, exists := indices[sid]; exists {
			return errors.New("runner: duplicate risk SID")
		}
		indices[sid] = i
	}
	raw, err = json.Marshal(frozen)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(sum[:])
	err = RegisterPortfolioBuilder(name, func(frame factor.Frame, universe factor.Universe, spec factor.PortfolioSpec, definition research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		if frame.SnapshotID != spec.SnapshotID || frame.PlanHash != spec.FactorPlanHash || frame.DecisionTime != spec.DecisionTime || universe.Version != spec.UniverseVersion {
			return nil, nil, errors.New("runner: risk builder frame identity mismatch")
		}
		if len(frozen.Covariance) > 0 && (frozen.AvailableAt > frame.DecisionTime || frozen.TrainingEnd >= frame.DecisionTime) {
			return nil, nil, errors.New("runner: covariance not visible at decision")
		}
		tradable := map[int32]bool{}
		for _, sid := range universe.Tradable {
			tradable[sid] = true
		}
		sids := slices.Clone(universe.Investable)
		slices.Sort(sids)
		sids = slices.Compact(sids)
		var assets []int32
		var expected, variances, betas []float64
		var groups []string
		for _, sid := range sids {
			value, exists := frame.Values[frozen.ScoreColumn][sid]
			if !tradable[sid] || !exists || value.Validity != factor.Valid || math.IsNaN(value.Value) || math.IsInf(value.Value, 0) {
				continue
			}
			variance := 0.0
			if len(frozen.Covariance) == 0 {
				v, exists := frame.Values[frozen.VolatilityColumn][sid]
				if !exists || v.Validity != factor.Valid || v.Value <= 0 || math.IsNaN(v.Value) || math.IsInf(v.Value, 0) {
					continue
				}
				variance = v.Value * v.Value
			} else if _, exists := indices[sid]; !exists {
				continue
			}
			if frozen.LimitBeta {
				beta, exists := frozen.Beta[sid]
				if !exists {
					return nil, nil, errors.New("runner: missing declared beta exposure")
				}
				betas = append(betas, beta)
			}
			assets = append(assets, sid)
			expected = append(expected, value.Value)
			variances = append(variances, variance)
			groups = append(groups, frozen.Groups[sid])
		}
		if len(assets) == 0 {
			return nil, []factor.Diagnostic{{Code: "risk-empty-universe", Detail: "no tradable asset with valid score and risk estimate"}}, nil
		}
		covariance := make([][]float64, len(assets))
		for i, sid := range assets {
			covariance[i] = make([]float64, len(assets))
			if len(frozen.Covariance) == 0 {
				covariance[i][i] = variances[i]
				continue
			}
			row := frozen.Covariance[indices[sid]]
			if len(row) != len(frozen.SIDs) {
				return nil, nil, errors.New("runner: invalid fitted covariance dimensions")
			}
			for j, other := range assets {
				covariance[i][j] = row[indices[other]]
			}
		}
		result, err := research.OptimizePortfolio(research.OptimizationSpec{ExpectedReturns: expected, Covariance: covariance, RiskAversion: frozen.RiskAversion, Constraints: research.RiskConstraints{Gross: frozen.Gross, MaxWeight: frozen.MaxWeight, MaxNet: frozen.MaxNet, MaxBeta: frozen.MaxBeta, VolatilityTarget: frozen.VolatilityTarget, LongOnly: frozen.LongOnly, LimitNet: frozen.LimitNet, LimitBeta: frozen.LimitBeta, Groups: groups, GroupCaps: frozen.GroupCaps, Beta: betas}})
		if err != nil {
			return nil, nil, err
		}
		diagnostics := []factor.Diagnostic{{Code: "risk-optimizer-v1", Detail: fmt.Sprintf("config=%s status=%s gross=%g cash=%g volatility=%g iterations=%d", fingerprint, result.Status, result.Gross, result.Cash, result.Volatility, result.Iterations)}}
		if result.Status != "feasible" {
			return nil, diagnostics, fmt.Errorf("runner: risk optimizer constraints infeasible: %v", result.Violations)
		}
		targets := map[int32]float64{}
		for i, sid := range assets {
			targets[sid] = result.Weights[i]
		}
		spec.Diagnostics = append(spec.Diagnostics, diagnostics...)
		target, err := factor.NewTargetPortfolio(spec, targets)
		return target, diagnostics, err
	})
	if err != nil {
		return err
	}
	return RegisterPortfolioBuilderIdentity(name, fingerprint)
}
