package entry

import (
	"fmt"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/execution"
)

func executionTakeoverOwner(cfg *config.Config, bridge *biz.SharedOrderBridgeConfig) (execution.StrategyID, error) {
	if cfg == nil || cfg.TakeOverStrat == "" {
		return "", nil
	}
	if bridge == nil {
		return "", fmt.Errorf("execution: take_over_strat %s requires a configured time-series strategy", cfg.TakeOverStrat)
	}
	binding, ok := bridge.Strategies[cfg.TakeOverStrat]
	if !ok || binding.ID == "" {
		return "", fmt.Errorf("execution: take_over_strat %s is not a configured account strategy", cfg.TakeOverStrat)
	}
	return binding.ID, nil
}
