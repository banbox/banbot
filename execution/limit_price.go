package execution

import (
	"errors"
	"math"
	"strconv"

	"github.com/shopspring/decimal"
)

// LimitPriceLevel is one normalized same-side depth level, in base-volume units.
type LimitPriceLevel struct {
	Price, Volume float64
}

// LimitPriceBook has bids in descending and asks in ascending price order.
// Callers provide an immutable snapshot; this utility does not own market I/O.
type LimitPriceBook struct {
	Bids, Asks []LimitPriceLevel
}

type LimitPriceVolume struct {
	AverageMinute, LastMinute float64
}

// SummarizeLimitPriceVolume summarizes chronological minute-volume observations.
func SummarizeLimitPriceVolume(volumes []float64) (LimitPriceVolume, error) {
	var result LimitPriceVolume
	for _, volume := range volumes {
		if !validLimitPriceNumber(volume) || volume < 0 {
			return result, errors.New("execution: invalid minute volume")
		}
		result.AverageMinute += volume
		result.LastMinute = volume
	}
	if len(volumes) > 0 {
		result.AverageMinute /= float64(len(volumes))
	}
	if !validLimitPriceNumber(result.AverageMinute) {
		return LimitPriceVolume{}, errors.New("execution: minute volume overflow")
	}
	return result, nil
}

// DefaultLimitPrice preserves the volume-based passive limit-price algorithm:
// buy prices use bids and sell prices use asks. Whole depth levels contribute to
// the average, including the level that first exceeds the requested volume.
// A zero priceTick leaves the result unrounded; a positive tick rounds nearest.
func DefaultLimitPrice(book LimitPriceBook, volume LimitPriceVolume, side OrderSide, waitSecs int, priceTick float64) (float64, error) {
	levels, err := limitPriceLevels(book, volume, side)
	if err != nil {
		return 0, err
	}
	if waitSecs < 0 || !validLimitPriceNumber(priceTick) || priceTick < 0 {
		return 0, errors.New("execution: invalid limit-price wait or tick")
	}
	depth := math.Min(volume.AverageMinute/30*float64(waitSecs), volume.LastMinute/60*float64(waitSecs))
	if !validLimitPriceNumber(depth) {
		return 0, errors.New("execution: limit-price depth overflow")
	}
	var total, cost, last float64
	for _, level := range levels {
		total += level.Volume
		cost += level.Volume * level.Price
		last = level.Price
		if total >= depth {
			break
		}
	}
	if total == 0 {
		return 0, nil
	}
	price := cost / total
	if total < depth {
		// Preserve the historical thin-book extrapolation rather than silently
		// pinning a large wait window to the last visible depth level.
		first := levels[0].Price
		last = first + (last-first)*depth/total
		price = first*0.3 + last*0.7
	}
	if !validLimitPriceNumber(price) || price <= 0 {
		return 0, errors.New("execution: invalid computed limit price")
	}
	if priceTick > 0 {
		tick := decimal.NewFromFloat(priceTick)
		price, _ = decimal.NewFromFloat(price).DivRound(tick, 0).Mul(tick).Float64()
		if price <= 0 {
			return 0, errors.New("execution: limit price rounds to zero")
		}
	}
	return price, nil
}

// LimitPriceWait estimates seconds and the visible-depth coverage for a passive
// target price. A target at a level excludes that level's volume, as before.
func LimitPriceWait(book LimitPriceBook, volume LimitPriceVolume, side OrderSide, price float64) (int, float64, error) {
	levels, err := limitPriceLevels(book, volume, side)
	if err != nil {
		return 0, 1, err
	}
	if !validLimitPriceNumber(price) || price <= 0 {
		return 0, 1, errors.New("execution: invalid limit-price target")
	}
	perSecond := math.Max(volume.AverageMinute, volume.LastMinute) / 60
	if perSecond == 0 || len(levels) == 0 {
		return 0, 1, nil
	}
	var total float64
	coverage := 1.0
	for i, level := range levels {
		if (side == Buy && level.Price <= price) || (side == Sell && level.Price >= price) {
			break
		}
		total += level.Volume
		if i == len(levels)-1 {
			coverage = math.Abs(level.Price-levels[0].Price) / math.Abs(price-levels[0].Price)
		}
	}
	seconds := math.Round(total / perSecond)
	if !validLimitPriceNumber(seconds) || seconds >= math.Ldexp(1, strconv.IntSize-1) {
		return 0, 1, errors.New("execution: limit-price wait overflow")
	}
	return int(seconds), coverage, nil
}

func limitPriceLevels(book LimitPriceBook, volume LimitPriceVolume, side OrderSide) ([]LimitPriceLevel, error) {
	if !validLimitPriceNumber(volume.AverageMinute) || !validLimitPriceNumber(volume.LastMinute) || volume.AverageMinute < 0 || volume.LastMinute < 0 {
		return nil, errors.New("execution: invalid limit-price volume")
	}
	levels := book.Bids
	if side == Sell {
		levels = book.Asks
	} else if side != Buy {
		return nil, errors.New("execution: invalid limit-price side")
	}
	for i, level := range levels {
		if !validLimitPriceNumber(level.Price) || !validLimitPriceNumber(level.Volume) || level.Price <= 0 || level.Volume < 0 {
			return nil, errors.New("execution: invalid limit-price depth level")
		}
		if i > 0 && ((side == Buy && level.Price > levels[i-1].Price) || (side == Sell && level.Price < levels[i-1].Price)) {
			return nil, errors.New("execution: unordered limit-price depth")
		}
	}
	return levels, nil
}

func validLimitPriceNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
