package execution

import (
	"strings"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
)

// OHLCProfile preserves the TS historical venue assumptions. It consumes the
// existing OHLC compatibility view only inside matching; DataSeries.Values
// remains the input and output model. Observable-quote execution has a separate
// profile and must not infer visible prices from this future bar range.
type OHLCProfile struct {
	LegacyIntrabar bool
}

func (p OHLCProfile) MarketPrice(bar *orm.SeriesOHLCV, rate float64) float64 {
	price, _, _ := p.PriceRange(bar, rate)
	return price
}

func (p OHLCProfile) StopEntryTriggered(isBuy bool, trigger, low, high float64) bool {
	if p.LegacyIntrabar {
		if isBuy {
			return trigger <= high
		}
		return trigger >= low
	}
	return trigger >= low && trigger <= high
}

// OHLCOrder is an order projection, not a second time-series transport.
type OHLCOrder struct {
	OrderType           string
	IsBuy, Enter, Short bool
	Price, Stop         float64
	CreateAt            int64
}

type OHLCFill struct {
	Price, Rate   float64
	TimeMS        int64
	StopTriggered bool
}

// MatchPending retains historical second-resolution delay and alignment,
// price improvement, and stop-limit ordering, including clearing a stop whose
// subsequent limit remains unfilled. The caller supplies an existing bar view.
func (p OHLCProfile) MatchPending(bar *orm.SeriesOHLCV, order OHLCOrder, tfSecs int, netCost float64) (fill OHLCFill, matched bool) {
	fill.Price = order.Price
	fill.TimeMS = order.CreateAt + int64(netCost*1000)
	barStartMS := utils.AlignTfMSecs(fill.TimeMS, int64(tfSecs*1000))
	var minRate float64
	if order.Enter && order.Stop > 0 {
		if !p.StopEntryTriggered(order.IsBuy, order.Stop, bar.Low, bar.High) {
			return fill, false
		}
		fill.Price = order.Stop
		fill.StopTriggered = true
		if strings.Contains(order.OrderType, "limit") && order.Price > 0 && (order.Price > order.Stop) == order.Short {
			fill.Price = order.Price
		}
		minRate = float64((order.CreateAt-barStartMS)/1000) / float64(tfSecs)
		minRate = p.MarketRate(bar, order.Stop, order.IsBuy, true, minRate)
		fill.Rate = minRate
		fill.TimeMS = bar.Time + int64(float64(tfSecs)*minRate)*1000
	}
	if strings.Contains(order.OrderType, "limit") && order.Price > 0 {
		if order.IsBuy {
			if fill.Price < bar.Low {
				return fill, false
			}
			if fill.Price > bar.Open {
				fill.Price = bar.Open
			}
		} else {
			if fill.Price > bar.High {
				return fill, false
			}
			if fill.Price < bar.Open {
				fill.Price = bar.Open
			}
		}
		if minRate == 0 {
			minRate = float64((order.CreateAt-barStartMS)/1000) / float64(tfSecs)
		}
		fill.Rate = p.MarketRate(bar, order.Price, order.IsBuy, false, minRate)
		fill.TimeMS = bar.Time + int64(float64(tfSecs)*fill.Rate)*1000
	} else if !fill.StopTriggered {
		fill.Rate = float64((fill.TimeMS-barStartMS)/1000) / float64(tfSecs)
		fill.Price = p.MarketPrice(bar, fill.Rate)
	}
	return fill, true
}

type OHLCProtection struct {
	Price, Limit float64
	Hit          bool
}

type OHLCProtectionFill struct {
	HitSL, HitTP, Ready                       bool
	TriggerPrice, CandidatePrice, Price, Rate float64
	TimeMS                                    int64
	OrderType                                 string
}

// ProtectionLimitPrice returns -1 for an unfillable limit, zero for market,
// or the historical limit price. Timing does not change maker classification.
func ProtectionLimitPrice(bar *orm.SeriesOHLCV, short bool, trigger, limit float64) float64 {
	if bar == nil {
		return -1
	}
	if limit > 0 {
		if short && limit < bar.Low || !short && limit > bar.High {
			return -1
		}
		if short && limit <= trigger || !short && limit >= trigger {
			return limit
		}
	}
	return 0
}

// MatchProtection preserves sticky hits, SL precedence on a dual hit, the
// take-profit limit override, and the distinct millisecond exit clock.
func (p OHLCProfile) MatchProtection(bar *orm.SeriesOHLCV, short bool, sl, tp *OHLCProtection, afterRate, tfSecs float64, nowMS int64) (fill OHLCProtectionFill) {
	if bar == nil {
		return fill
	}
	fill.HitSL = sl != nil && (sl.Hit || short && bar.High >= sl.Price || !short && bar.Low <= sl.Price)
	fill.HitTP = tp != nil && (tp.Hit || short && bar.Low <= tp.Price || !short && bar.High >= tp.Price)
	if fill.HitSL {
		fill.TriggerPrice = sl.Price
		fill.CandidatePrice = ProtectionLimitPrice(bar, short, sl.Price, sl.Limit)
	} else if fill.HitTP {
		fill.TriggerPrice = tp.Price
		fill.CandidatePrice = ProtectionLimitPrice(bar, short, tp.Price, tp.Limit)
		if fill.CandidatePrice == 0 && tp.Limit > 0 {
			fill.CandidatePrice = tp.Limit
		}
	} else {
		return fill
	}
	if fill.CandidatePrice < 0 {
		return fill
	}
	fill.Price = fill.CandidatePrice
	fill.OrderType = banexg.OdTypeMarket
	if fill.Price > 0 {
		fill.OrderType = banexg.OdTypeLimit
		fill.Rate = p.MarketRate(bar, fill.Price, short, true, afterRate)
	} else {
		fill.Rate = p.MarketRate(bar, fill.TriggerPrice, short, true, afterRate)
		fill.Price = p.MarketPrice(bar, fill.Rate)
	}
	fill.TimeMS = nowMS - int64(tfSecs*(1-fill.Rate)*1000)
	fill.Ready = true
	return fill
}

// LegacyOrderFee uses normalized SDK interfaces for every market type and
// normalizes the order type before SDK IO, including when fee calculation fails.
func LegacyOrderFee(exchange banexg.BanExchange, symbol string, orderType *string, side string, filled, price float64) (*banexg.Fee, *errs.Error) {
	maker := strings.Contains(*orderType, "limit")
	if *orderType == banexg.OdTypeLimit {
		if maker {
			*orderType = banexg.OdTypeLimitMaker
		} else {
			*orderType = "limit_taker"
		}
	}
	return exchange.CalculateFee(symbol, *orderType, side, filled, price, maker, nil)
}

// Entry helpers preserve SDK precision for spot/inverse/significant-digit
// markets without imposing the quote profile's linear-perpetual lattice.
func OHLCEntryPrice(exchange banexg.BanExchange, symbol string, price float64) (*banexg.Market, float64, *errs.Error) {
	market, err := exchange.GetMarket(symbol)
	if err != nil {
		return nil, 0, err
	}
	price, err = exchange.PrecPrice(market, price)
	return market, price, err
}

func OHLCEntryAmount(exchange banexg.BanExchange, market *banexg.Market, quoteCost, price float64) (raw, rounded float64, err *errs.Error) {
	raw = quoteCost / price
	rounded, err = exchange.PrecAmount(market, raw)
	return
}
func (p OHLCProfile) PriceRange(bar *orm.SeriesOHLCV, rate float64) (float64, float64, float64) {
	var (
		a, b, c, pa, totalLen float64
		aEndRate, bEndRate    float64
		start, end, posRate   float64
	)

	openP := bar.Open
	highP := bar.High
	lowP := bar.Low
	closeP := bar.Close
	preMoveFactor, closeLegFactor := 0.3, 1.3
	if p.LegacyIntrabar {
		preMoveFactor, closeLegFactor = 0, 1
	}

	if rate == 0 {
		return openP, highP, lowP
	}
	if rate >= 0.999 {
		return closeP, highP, lowP
	}
	newHigh, newLow := highP, lowP

	if openP <= closeP {
		// close > open, generally first moves down to the lower shadow line, then rises to the highest point, and finally retreats slightly to form the upper shadow line.
		// 阳线  一般是先下调走出下影线，然后上升到最高点，最后略微回撤，出现上影线
		pa = (openP - lowP) * preMoveFactor // a向下前的小幅向上回调，模拟震荡
		a = openP + pa - lowP
		b = highP - lowP
		c = (highP - closeP) * closeLegFactor // 多加些，模拟震荡
		totalLen = a + b + c + pa
		if totalLen == 0 {
			return closeP, highP, lowP
		}
		paEndRate := pa / totalLen
		aEndRate = (pa + a) / totalLen
		bEndRate = (pa + a + b) / totalLen
		if rate <= paEndRate {
			start, end, posRate = openP, openP+pa, rate/paEndRate
		} else if rate <= aEndRate {
			start, end, posRate = openP+pa, lowP, (rate-paEndRate)/(aEndRate-paEndRate)
		} else if rate <= bEndRate {
			start, end, posRate = lowP, highP, (rate-aEndRate)/(bEndRate-aEndRate)
			newLow = closeP
		} else {
			start, end, posRate = highP, closeP, (rate-bEndRate)/(1-bEndRate)
			newHigh, newLow = closeP, closeP
		}
	} else {
		// close < open. generally rises first and goes out of the upper shadow line, then drops to the lowest point, and finally pulls back slightly to form a lower shadow line.
		// 阴线  一般是先上升走出上影线，然后下降到最低点，最后略微回调，出现下影线
		pa = (highP - openP) * preMoveFactor // a向上前的小幅向下回调，模拟震荡
		a = highP - (openP - pa)
		b = highP - lowP
		c = (closeP - lowP) * closeLegFactor // 模拟震荡
		totalLen = a + b + c + pa
		if totalLen == 0 {
			return closeP, highP, lowP
		}
		paEndRate := pa / totalLen
		aEndRate = (pa + a) / totalLen
		bEndRate = (pa + a + b) / totalLen
		if rate <= paEndRate {
			start, end, posRate = openP, openP-pa, rate/paEndRate
		} else if rate <= aEndRate {
			start, end, posRate = openP-pa, highP, (rate-paEndRate)/(aEndRate-paEndRate)
		} else if rate <= bEndRate {
			start, end, posRate = highP, lowP, (rate-aEndRate)/(bEndRate-aEndRate)
			newHigh = closeP
		} else {
			start, end, posRate = lowP, closeP, (rate-bEndRate)/(1-bEndRate)
			newHigh, newLow = closeP, closeP
		}
	}

	newOpen := start*(1-posRate) + end*posRate
	newHigh = max(newOpen, newHigh)
	newLow = min(newOpen, newLow)
	return newOpen, newHigh, newLow
}
func (p OHLCProfile) CutBar(bar *orm.SeriesOHLCV, tfMSecs int64, rate float64) *orm.SeriesOHLCV {
	start, high, low := p.PriceRange(bar, rate)
	return &orm.SeriesOHLCV{
		Sid:       bar.Sid,
		ExSymbol:  bar.ExSymbol,
		Source:    bar.Source,
		Time:      bar.Time + int64(float64(tfMSecs)*rate),
		EndMS:     bar.EndMS,
		TimeFrame: bar.TimeFrame,
		Open:      start,
		High:      high,
		Low:       low,
		Close:     bar.Close,
		Volume:    bar.Volume * (1 - rate),
		Quote:     bar.Quote,
		BuyVolume: bar.BuyVolume,
		TradeNum:  bar.TradeNum,
		Adj:       bar.Adj,
		IsWarmUp:  bar.IsWarmUp,
		Closed:    bar.Closed,
	}
}
func (p OHLCProfile) MarketRate(bar *orm.SeriesOHLCV, price float64, isBuy, isTrigger bool, minRate float64) float64 {
	if bar == nil {
		return minRate
	}
	if isTrigger {
		// For the order that triggers the price, it is not a pending order. If it is judged that it is not within the bar range, it is considered to be completed immediately.
		// 对于触发价格的订单，不是挂单，判断如果未在bar范围内，则认为立刻成交
		if price < bar.Low || price > bar.High {
			return minRate
		}
	} else {
		// Non-trigger mode, directly compare with the opening price
		// 非触发模式，直接和开盘价对比
		if isBuy && price >= bar.Open || !isBuy && price <= bar.Open {
			// 开盘立刻成交。
			return minRate
		}
	}

	var (
		a, b, c, pa, totalLen float64
	)

	openP := bar.Open
	highP := bar.High
	lowP := bar.Low
	closeP := bar.Close
	preMoveFactor, closeLegFactor := 0.3, 1.3
	if p.LegacyIntrabar {
		preMoveFactor, closeLegFactor = 0, 1
	}

	if openP <= closeP {
		// close > open. generally first moves down to the lower shadow line, then rises to the highest point, and finally retreats slightly to form the upper shadow line.
		// 阳线  一般是先下调走出下影线，然后上升到最高点，最后略微回撤，出现上影线
		pa = (openP - lowP) * preMoveFactor   // a向下前的小幅向上回调，模拟震荡
		a = openP + pa - lowP                 // open~low. 开盘~最低
		b = highP - lowP                      // low~high. 最低~最高
		c = (highP - closeP) * closeLegFactor // high~close. 最高~收盘，模拟震荡
		totalLen = a + b + c + pa
		if totalLen == 0 {
			return 0.5
		}
		if isTrigger {
			// Trigger price, no need to consider buying and selling direction, direct comparison
			// 触发价格，无需考虑买卖方向，直接比较
			if !p.LegacyIntrabar && price >= openP && price <= openP+pa {
				// a向下前小幅向上回调时触发
				rate := (price - openP) / totalLen
				if rate >= minRate {
					return rate
				}
			} else if price < openP {
				// The trigger bid price is lower than the opening price, and it is triggered when the opening price is the lowest
				// 触发买价低于开盘，在开盘~最低时触发
				rate := (pa + openP + pa - price) / totalLen
				if rate >= minRate {
					return rate
				}
			}
			// Otherwise, it will be triggered from the lowest to the highest
			// 否则在最低~最高中触发
			rate := (pa + a + price - lowP) / totalLen
			if rate >= minRate {
				return rate
			} else {
				// Triggered during the highest to closing time
				// 在最高~收盘中触发
				return (pa + a + b + highP - price) / totalLen
			}
		} else {
			if isBuy {
				// Buy order, triggered at opening ~ lowest price
				// 买单，在开盘~最低时触发
				rate := (pa + openP + pa - price) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Trigger at minimum to maximum
					// 在最低~最高时触发
					return (pa + a + price - lowP) / totalLen
				}
			} else {
				// Sell order, triggered between the lowest and highest levels
				// 卖单，在最低~最高中触发
				rate := (pa + a + price - lowP) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Triggered during the highest to closing time
					// 在最高~收盘中触发
					return (pa + a + b + highP - price) / totalLen
				}
			}
		}
	} else {
		// close < open. generally rises first and goes out of the upper shadow line, then drops to the lowest point, and finally pulls back slightly to form a lower shadow line.
		// 阴线  一般是先上升走出上影线，然后下降到最低点，最后略微回调，出现下影线
		pa = (highP - openP) * preMoveFactor // a向上前的小幅回调向下，模拟震荡
		a = highP - (openP - pa)             // 开盘~最高
		b = highP - lowP                     // 最高~最低
		c = (closeP - lowP) * closeLegFactor // 最低~收盘，模拟震荡
		totalLen = a + b + c + pa
		if totalLen == 0 {
			return 0.5
		}
		if isTrigger {
			// Trigger price, no need to consider buying and selling direction, direct comparison
			// 触发价格，无需考虑买卖方向，直接比较
			if price < openP {
				if price >= openP-pa {
					// pa: 先小幅下降回调
					rate := (openP - price) / totalLen
					if rate >= minRate {
						return rate
					}
				}
				// If the trigger price is lower than the opening price, it must be triggered between the highest and lowest prices.
				// 触发价低于开盘，必然在最高~最低中触发
				rate := (pa + a + highP - price) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Triggered at the lowest price ~ closing price
					// 在最低~收盘中触发
					return (pa + a + b + price - lowP) / totalLen
				}
			} else {
				// The trigger price is higher than the opening price, and is triggered between the opening price and the highest price.
				// 触发价高于开盘，在开盘~最高中触发
				rate := (pa + price - openP + pa) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Trigger between highest and lowest
					// 在最高~最低中触发
					return (pa + a + highP - price) / totalLen
				}
			}
		} else {
			if isBuy {
				if price >= openP-pa {
					// 在向上前的小幅回调中触发
					rate := (openP - price) / totalLen
					if rate >= minRate {
						return rate
					}
				}
				// Buy orders must be triggered between the highest and lowest prices.
				// 买单，必然在最高~最低中触发
				rate := (pa + a + highP - price) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Triggered at the lowest price ~ closing price
					// 在最低~收盘中触发
					return (pa + a + b + price - lowP) / totalLen
				}
			} else {
				// Sell order, triggered from the opening to the highest price
				// 卖单，在开盘~最高中触发
				rate := (pa + price - openP + pa) / totalLen
				if rate >= minRate {
					return rate
				} else {
					// Trigger between highest and lowest
					// 在最高~最低中触发
					return (pa + a + highP - price) / totalLen
				}
			}
		}
	}
}
