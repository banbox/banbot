package statistics

import (
	"errors"
	"math"
	"math/rand"
)

type OUExitConfig struct {
	InitialPrice, Forecast, HalfLife, Sigma float64
	ProfitTaking, StopLoss                  []float64
	Trials, MaxHolding                      int
	Seed                                    int64
	MaxDraws                                int64
}

type OUExitResult struct {
	ProfitTaking, StopLoss, Mean, Std, Sharpe float64
	SharpeValid                               bool
}

// SimulateOUExitGrid implements AFML snippet 13.2's already-open LONG position
// exit-rule experiment, not an entry strategy or historical backtest. Prices
// follow p=(1-phi)*forecast+phi*p+sigma*N(0,1), phi=2^(-1/halfLife).
// Payout is p-initialPrice; crossings are strict. Printed hp>maxHP permits
// maxHolding+1 innovations. Std is population std (numpy default ddof=0).
// MaxDraws bounds the worst-case total innovations before any simulation runs.
func SimulateOUExitGrid(config OUExitConfig) ([]OUExitResult, error) {
	if !afmlFinite(config.InitialPrice) || !afmlFinite(config.Forecast) || !afmlFinite(config.HalfLife) || config.HalfLife <= 0 || !afmlFinite(config.Sigma) || config.Sigma <= 0 || config.Trials < 2 || config.MaxHolding < 1 || config.MaxHolding == math.MaxInt || config.MaxDraws < 1 || len(config.ProfitTaking) == 0 || len(config.StopLoss) == 0 {
		return nil, errors.New("statistics: invalid OU exit simulation configuration")
	}
	for _, thresholds := range [][]float64{config.ProfitTaking, config.StopLoss} {
		for _, v := range thresholds {
			if !afmlFinite(v) || v < 0 {
				return nil, errors.New("statistics: OU exit thresholds must be finite nonnegative")
			}
		}
	}
	worst := int64(config.MaxHolding) + 1
	for _, size := range []int{config.Trials, len(config.ProfitTaking), len(config.StopLoss)} {
		if int64(size) > config.MaxDraws/worst {
			return nil, errors.New("statistics: OU exit simulation exceeds draw budget")
		}
		worst *= int64(size)
	}
	phi := math.Exp2(-1 / config.HalfLife)
	rng := rand.New(rand.NewSource(config.Seed))
	var results []OUExitResult
	for _, profit := range config.ProfitTaking {
		for _, stop := range config.StopLoss {
			mean, m2 := 0., 0.
			for run := 0; run < config.Trials; run++ {
				p, payout := config.InitialPrice, 0.
				for hp := 1; hp <= config.MaxHolding+1; hp++ {
					p = (1-phi)*config.Forecast + phi*p + config.Sigma*rng.NormFloat64()
					payout = p - config.InitialPrice
					if !afmlFinite(payout) {
						return nil, errors.New("statistics: OU simulation price overflow")
					}
					if payout > profit || payout < -stop {
						break
					}
				}
				delta := payout - mean
				mean += delta / float64(run+1)
				m2 += delta * (payout - mean)
			}
			std := math.Sqrt(math.Max(0, m2/float64(config.Trials)))
			if !afmlFinite(mean) || !afmlFinite(std) {
				return nil, errors.New("statistics: OU simulation moments overflow")
			}
			result := OUExitResult{ProfitTaking: profit, StopLoss: stop, Mean: mean, Std: std}
			if std > 0 {
				result.Sharpe = mean / std
				result.SharpeValid = afmlFinite(result.Sharpe)
				if !result.SharpeValid {
					result.Sharpe = 0
				}
			}
			results = append(results, result)
		}
	}
	return results, nil
}
