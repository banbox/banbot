package runner

import (
	"context"
	"errors"
	"fmt"
	"github.com/banbox/banbot/factor/research"
)

type PortfolioTrial struct {
	Name      string
	Portfolio research.PortfolioDefinition
}
type PortfolioTrialResult struct {
	Name   string
	Result Result
}

// ScanPortfolioTrials runs a bounded parameter grid over one archived input.
// Compatible variants share one DAG, with private books and policy states.
// Selection of a winning variant belongs to a later, separate training window.
func ScanPortfolioTrials(ctx context.Context, base Config, trials []PortfolioTrial, maximum int) ([]PortfolioTrialResult, error) {
	if maximum <= 0 || len(trials) == 0 || len(trials) > maximum || maximum > 10000 {
		return nil, errors.New("runner: portfolio scan exceeds explicit trial budget")
	}
	if base.Mode != Weights {
		return nil, errors.New("runner: portfolio scan requires isolated weights simulations")
	}
	configs := make([]Config, len(trials))
	names := map[string]bool{}
	for i, trial := range trials {
		if trial.Name == "" || names[trial.Name] {
			return nil, errors.New("runner: portfolio trials require unique names")
		}
		names[trial.Name] = true
		config, err := CloneConfig(base)
		if err != nil {
			return nil, err
		}
		config.Manifest.Portfolio = research.ClonePortfolioDefinition(trial.Portfolio)
		if trial.Portfolio.Builder != base.Manifest.Portfolio.Builder {
			config.PortfolioBuilder = nil
		}
		config.StrategyID = fmt.Sprintf("%s-trial-%d", base.StrategyID, i+1)
		config.ArtifactPath = ""
		configs[i] = config
	}
	results, err := RunMany(ctx, configs, make([]Sink, len(trials)), make([]Output, len(trials)))
	if err != nil {
		return nil, err
	}
	output := make([]PortfolioTrialResult, len(results))
	for i, result := range results {
		output[i] = PortfolioTrialResult{Name: trials[i].Name, Result: result}
	}
	return output, nil
}
