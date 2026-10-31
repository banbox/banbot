package execution

import (
	"context"
	"errors"
	"time"

	"github.com/banbox/banexg"
)

// DefaultLimitPrice loads a complete closed-minute volume window and a current
// depth snapshot through the account's context-bound transport. No quote or
// strategy-price fallback is allowed when either input is unavailable.
func (a *BanexgAdapter) DefaultLimitPrice(ctx context.Context, id string, side OrderSide, waitSecs int) (float64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	entry, ok := a.byID[id]
	if !ok || (side != Buy && side != Sell) || waitSecs < 0 {
		return 0, errors.New("execution: invalid banexg limit-price request")
	}
	const minuteMS int64 = 60000
	closedUntil := time.Now().UnixMilli() / minuteMS * minuteMS
	since := closedUntil - 5*minuteMS
	var rows []*banexg.Kline
	if err := a.invoke(ctx, func() error {
		result, e := a.exchange.FetchOHLCV(entry.Symbol, "1m", since, 6, a.params(ctx))
		rows = result
		if e != nil {
			return e
		}
		return nil
	}); err != nil {
		return 0, err
	}
	volumes := make([]float64, 5)
	seen := make([]bool, 5)
	for _, row := range rows {
		if row == nil {
			return 0, errors.New("execution: missing banexg minute-volume row")
		}
		if row.Time < since || row.Time >= closedUntil {
			continue // An open minute is never a visible volume observation.
		}
		if row.Time%minuteMS != 0 {
			return 0, errors.New("execution: unaligned banexg minute-volume row")
		}
		i := (row.Time - since) / minuteMS
		if seen[i] {
			return 0, errors.New("execution: duplicate banexg minute-volume row")
		}
		seen[i], volumes[i] = true, row.Volume
	}
	for _, present := range seen {
		if !present {
			return 0, errors.New("execution: incomplete banexg closed-minute volume window")
		}
	}
	volume, err := SummarizeLimitPriceVolume(volumes)
	if err != nil {
		return 0, err
	}
	var book *banexg.OrderBook
	if err := a.invoke(ctx, func() error {
		result, e := a.exchange.FetchOrderBook(entry.Symbol, 0, a.params(ctx))
		book = result
		if e != nil {
			return e
		}
		return nil
	}); err != nil {
		return 0, err
	}
	received := time.Now().UnixMilli()
	if book == nil || book.Symbol != entry.Symbol || book.TimeStamp <= 0 || book.TimeStamp > received || received-book.TimeStamp >= a.config.QuoteTTL.Milliseconds() {
		return 0, errors.New("execution: missing or stale banexg limit-price book")
	}
	bids, err := normalizeLimitPriceSide(book.Bids)
	if err != nil {
		return 0, err
	}
	asks, err := normalizeLimitPriceSide(book.Asks)
	if err != nil {
		return 0, err
	}
	normalized := LimitPriceBook{Bids: bids, Asks: asks}
	if _, err := limitPriceLevels(normalized, volume, Buy); err != nil {
		return 0, err
	}
	if _, err := limitPriceLevels(normalized, volume, Sell); err != nil {
		return 0, err
	}
	if asks[0].Price < bids[0].Price {
		return 0, errors.New("execution: crossed banexg limit-price book")
	}
	tick, err := floatBoundary(entry.Instrument.PriceTick)
	if err != nil {
		return 0, err
	}
	price, err := DefaultLimitPrice(normalized, volume, side, waitSecs, tick)
	if err != nil {
		return 0, err
	}
	if price <= 0 {
		return 0, errors.New("execution: empty banexg limit-price liquidity")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return price, nil
}

func normalizeLimitPriceSide(side *banexg.OdBookSide) ([]LimitPriceLevel, error) {
	if side == nil {
		return nil, errors.New("execution: missing banexg limit-price depth side")
	}
	side.Lock.Lock()
	defer side.Lock.Unlock()
	if len(side.Price) == 0 || len(side.Price) != len(side.Size) {
		return nil, errors.New("execution: incomplete banexg limit-price depth side")
	}
	levels := make([]LimitPriceLevel, len(side.Price))
	for i, price := range side.Price {
		levels[i] = LimitPriceLevel{Price: price, Volume: side.Size[i]}
	}
	return levels, nil
}
