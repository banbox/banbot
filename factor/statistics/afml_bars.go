package statistics

import (
	"errors"
	"math"
)

type ActivityTick struct {
	Timestamp     int64
	Price, Volume float64
	Side          int // +/-1 aggressor; 0 infers tick rule, carrying unchanged-price sign
}

type ActivityBar struct {
	Start, End                                         int
	Timestamp                                          int64
	Open, High, Low, Close, Volume, DollarVolume, VWAP float64
	Ticks                                              int
	VWAPValid, Complete                                bool
	Threshold                                          float64 // frozen activity/information threshold for this bar
}

func afmlTickActivity(tick ActivityTick, clock string) (float64, error) {
	switch clock {
	case "tick":
		return 1, nil
	case "volume":
		return tick.Volume, nil
	case "dollar":
		return tick.Price * tick.Volume, nil
	default:
		return 0, errors.New("statistics: unknown activity clock")
	}
}

func afmlValidateTicks(ticks []ActivityTick) error {
	if len(ticks) == 0 {
		return errors.New("statistics: empty tick stream")
	}
	for i, tick := range ticks {
		if !afmlFinite(tick.Price) || tick.Price <= 0 || !afmlFinite(tick.Volume) || tick.Volume < 0 || !afmlFinite(tick.Price*tick.Volume) || (tick.Side != 0 && tick.Side != 1 && tick.Side != -1) || (i > 0 && tick.Timestamp < ticks[i-1].Timestamp) {
			return errors.New("statistics: invalid activity tick")
		}
	}
	return nil
}

func afmlAddTick(bar *ActivityBar, tick ActivityTick, index int) error {
	if bar.Ticks == 0 {
		bar.Start, bar.Open, bar.High, bar.Low = index, tick.Price, tick.Price, tick.Price
	}
	bar.End, bar.Timestamp, bar.Close = index, tick.Timestamp, tick.Price
	bar.High, bar.Low = math.Max(bar.High, tick.Price), math.Min(bar.Low, tick.Price)
	bar.Ticks++
	bar.Volume += tick.Volume
	bar.DollarVolume += tick.Price * tick.Volume
	if !afmlFinite(bar.Volume) || !afmlFinite(bar.DollarVolume) {
		return errors.New("statistics: activity bar accumulation overflow")
	}
	if bar.Volume > 0 {
		bar.VWAP, bar.VWAPValid = bar.DollarVolume/bar.Volume, true
	}
	return nil
}

// StandardActivityBars forms AFML2.3.1 tick/volume/dollar bars at the first tick
// whose cumulative activity >=threshold. Ticks remain indivisible; overshoot
// belongs to this bar, never fabricated as a second fill. includePartial retains
// the final unfinished bar with Complete=false. Tick-count clocks are sensitive
// to fragmented fills/auction prints; volume and dollar clocks conserve volume.
func StandardActivityBars(ticks []ActivityTick, clock string, threshold float64, includePartial bool) ([]ActivityBar, error) {
	if err := afmlValidateTicks(ticks); err != nil {
		return nil, err
	}
	if !afmlFinite(threshold) || threshold <= 0 {
		return nil, errors.New("statistics: activity threshold must be positive finite")
	}
	var bars []ActivityBar
	bar, activity := ActivityBar{Threshold: threshold}, 0.
	for i, tick := range ticks {
		v, err := afmlTickActivity(tick, clock)
		if err != nil {
			return nil, err
		}
		if err = afmlAddTick(&bar, tick, i); err != nil {
			return nil, err
		}
		activity += v
		if !afmlFinite(activity) {
			return nil, errors.New("statistics: activity threshold accumulation overflow")
		}
		if activity >= threshold {
			bar.Complete = true
			bars = append(bars, bar)
			bar, activity = ActivityBar{Threshold: threshold}, 0
		}
	}
	if includePartial && bar.Ticks > 0 {
		bars = append(bars, bar)
	}
	return bars, nil
}

type InformationBarConfig struct {
	Clock, Kind                               string // tick/volume/dollar and imbalance/runs
	ExpectedTicks, ExpectedSignedActivity     float64
	BuyProbability, BuyActivity, SellActivity float64
	LengthAlpha, ActivityAlpha                float64 // explicit EW coefficients in (0,1]
	IncludePartial                            bool
}

// InformationActivityBars implements AFML2.3.2's >= information boundaries.
// Imbalance sums signed activity; runs count each side without offsetting or
// requiring consecutive same-side trades. Expectations are frozen at bar start.
// Caller supplies historical cold-start priors and EW coefficients (the book
// does not fix their values). Length and buy probability update from completed
// bar statistics; signed/conditional activity uses EW tick observations from
// prior completed bars. No future bar length is used in its own threshold.
func InformationActivityBars(ticks []ActivityTick, config InformationBarConfig) ([]ActivityBar, error) {
	if err := afmlValidateTicks(ticks); err != nil {
		return nil, err
	}
	if config.Kind != "imbalance" && config.Kind != "runs" {
		return nil, errors.New("statistics: unknown information bar kind")
	}
	if _, err := afmlTickActivity(ticks[0], config.Clock); err != nil {
		return nil, err
	}
	if !afmlFinite(config.ExpectedTicks) || config.ExpectedTicks <= 0 || !afmlFinite(config.ExpectedSignedActivity) || !afmlFinite(config.BuyProbability) || config.BuyProbability < 0 || config.BuyProbability > 1 || !afmlFinite(config.BuyActivity) || config.BuyActivity <= 0 || !afmlFinite(config.SellActivity) || config.SellActivity <= 0 || !afmlFinite(config.LengthAlpha) || config.LengthAlpha <= 0 || config.LengthAlpha > 1 || !afmlFinite(config.ActivityAlpha) || config.ActivityAlpha <= 0 || config.ActivityAlpha > 1 {
		return nil, errors.New("statistics: invalid information-bar priors/decay")
	}
	if config.Clock == "tick" && (config.BuyActivity != 1 || config.SellActivity != 1 || math.Abs(config.ExpectedSignedActivity) > 1) {
		return nil, errors.New("statistics: tick-clock activity is unit-valued")
	}
	expectedTicks, signed := config.ExpectedTicks, config.ExpectedSignedActivity
	pBuy, buyMean, sellMean := config.BuyProbability, config.BuyActivity, config.SellActivity
	threshold := func() float64 {
		if config.Kind == "imbalance" {
			return expectedTicks * math.Abs(signed)
		}
		return expectedTicks * math.Max(pBuy*buyMean, (1-pBuy)*sellMean)
	}
	var bars []ActivityBar
	bar := ActivityBar{Threshold: threshold()}
	buy, sell, buyTicks, side := 0., 0., 0, 1
	for i, tick := range ticks {
		if tick.Side != 0 {
			side = tick.Side
		} else if i > 0 {
			if tick.Price > ticks[i-1].Price {
				side = 1
			} else if tick.Price < ticks[i-1].Price {
				side = -1
			}
		}
		v, _ := afmlTickActivity(tick, config.Clock)
		if err := afmlAddTick(&bar, tick, i); err != nil {
			return nil, err
		}
		alpha := config.ActivityAlpha
		signed = (1-alpha)*signed + alpha*float64(side)*v
		if side > 0 {
			buy, buyTicks = buy+v, buyTicks+1
			buyMean = (1-alpha)*buyMean + alpha*v
		} else {
			sell += v
			sellMean = (1-alpha)*sellMean + alpha*v
		}
		information := math.Abs(buy - sell)
		if config.Kind == "runs" {
			information = math.Max(buy, sell)
		}
		if !afmlFinite(bar.Threshold) || !afmlFinite(information) {
			return nil, errors.New("statistics: information threshold overflow")
		}
		if information >= bar.Threshold {
			bar.Complete = true
			bars = append(bars, bar)
			expectedTicks = (1-config.LengthAlpha)*expectedTicks + config.LengthAlpha*float64(bar.Ticks)
			pBuy = (1-alpha)*pBuy + alpha*float64(buyTicks)/float64(bar.Ticks)
			bar = ActivityBar{Threshold: threshold()}
			buy, sell, buyTicks = 0, 0, 0
		}
	}
	if config.IncludePartial && bar.Ticks > 0 {
		bars = append(bars, bar)
	}
	return bars, nil
}
