package biz

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// timeSeriesLossStop converts fatal_stop / fatal_stop_hours into the account
// loss circuit breaker owned by execution. active is false for backtests, which
// never applied fatal_stop. An empty fatal_stop yields an empty policy, which
// disables the breaker.
func timeSeriesLossStop(deps RuntimeDeps) (policy execution.LossStopPolicy, active bool, err error) {
	if deps.Core == nil || deps.Core.BackTestMode || deps.Config == nil || deps.Config.View() == nil {
		return policy, false, nil
	}
	cfg := deps.Config.View()
	for text, rate := range cfg.FatalStop {
		minutes, convErr := strconv.Atoi(strings.TrimSpace(text))
		if convErr != nil || minutes <= 0 || rate <= 0 || rate > 1 {
			return policy, false, fmt.Errorf("biz: invalid fatal_stop entry %q: %v", text, rate)
		}
		policy.Windows = append(policy.Windows, execution.LossStopWindow{
			WindowMS: int64(minutes) * 60_000, MaxLossRate: decimal.NewFromFloat(rate),
		})
	}
	if len(policy.Windows) == 0 {
		return policy, true, nil
	}
	sort.Slice(policy.Windows, func(a, b int) bool { return policy.Windows[a].WindowMS < policy.Windows[b].WindowMS })
	hours := cfg.FatalStopHours
	if hours <= 0 {
		hours = 8
	}
	policy.PauseMS = int64(hours) * 3600_000
	return policy, true, nil
}

// applyLossStopPause mirrors the owner's durable pause into this runtime's
// entry switch, so admission rejects new entries without sending a command the
// owner would have to refuse. It never shortens an existing switch.
func applyLossStopPause(deps RuntimeDeps, status execution.LossStopStatus) error {
	if status.PausedUntilMS == 0 {
		return nil
	}
	if deps.Core == nil || deps.Clock == nil {
		return errors.New("biz: loss stop requires runtime core and clock")
	}
	if status.PausedUntilMS <= deps.Clock.TimeMS() {
		return nil
	}
	if current, _ := deps.Core.NoEnterUntilFor(deps.DefaultAccount); current < status.PausedUntilMS {
		deps.Core.SetNoEnterUntil(deps.DefaultAccount, status.PausedUntilMS)
		deps.Logger().Error("loss stop: entries paused by account owner",
			zap.String("account", deps.DefaultAccount), zap.Int64("until_ms", status.PausedUntilMS),
			zap.Int64("window_ms", status.TrippedWindowMS), zap.String("loss_rate", status.TrippedLossRate.String()))
	}
	return nil
}
