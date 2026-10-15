package exg

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

const dayMS = int64(24 * 60 * 60 * 1000)

// GetAlignOffForMarket derives a daily aggregation offset from the session
// metadata supplied by the exchange adapter. It is intentionally independent
// of exchange names: adapters own session semantics, while banbot only
// consumes the normalized Market representation.
func GetAlignOffForMarket(market *banexg.Market, tfSecs int) int {
	if market == nil || tfSecs < 24*60*60 || len(market.DayTimes) == 0 || len(market.NightTimes) == 0 {
		return 0
	}
	var dayEnd int64
	for _, session := range market.DayTimes {
		if session[1] > dayEnd {
			dayEnd = session[1]
		}
	}
	var nightStart int64
	hasNightStart := false
	for _, session := range market.NightTimes {
		start := session[0] % dayMS
		if start < 0 {
			start += dayMS
		}
		if !hasNightStart || start < nightStart {
			nightStart = start
			hasNightStart = true
		}
	}
	if dayEnd <= 0 || !hasNightStart {
		return 0
	}
	dayEnd %= dayMS
	boundary := (dayEnd + nightStart) / 2
	if boundary <= 0 || boundary >= dayMS {
		return 0
	}
	return int((dayMS - boundary) / 1000)
}

// GetAlignOffForExchangeChecked resolves market metadata while preserving
// adapter failures for callers that can handle them.
func GetAlignOffForExchangeChecked(exchange banexg.BanExchange, symbol string, tfSecs int) (offset int, err *errs.Error) {
	if tfSecs < 24*60*60 {
		return 0, nil
	}
	if exchange == nil {
		return 0, errs.NewMsg(core.ErrExgNotInit, "exchange is not initialized")
	}
	defer func() {
		if value := recover(); value != nil {
			offset = 0
			err = errs.NewMsg(core.ErrRunTime, "get alignment offset panicked for %q: %v", symbol, value)
		}
	}()
	market, err := exchange.GetMarket(symbol)
	if err != nil {
		return 0, err
	}
	if market == nil {
		market, err = exchange.MapMarket(symbol, 0)
		if err != nil {
			return 0, err
		}
	}
	return GetAlignOffForMarket(market, tfSecs), nil
}

// GetAlignOffForExchange resolves market metadata at a low-frequency
// composition/cache boundary. Feeders store the returned value in their
// PairTFCache, so no adapter call is made in the bar/tick path.
func GetAlignOffForExchange(exchange banexg.BanExchange, symbol string, tfSecs int) int {
	offset, err := GetAlignOffForExchangeChecked(exchange, symbol, tfSecs)
	if err != nil {
		log.Error("get alignment offset fail", zap.String("symbol", symbol), zap.Error(err))
		return 0
	}
	return offset
}

// GetAlignOffForSymbol is the compatibility bridge for legacy ORM/data tools
// that only carry exchange and market names. It still obtains the adapter's
// Market metadata before calculating the offset; no exchange-specific name is
// interpreted here.
func GetAlignOffForSymbol(exchangeName, marketType, symbol string, tfSecs int) int {
	exchange, err := GetWith(exchangeName, marketType, "")
	if err != nil {
		return 0
	}
	return GetAlignOffForExchange(exchange, symbol, tfSecs)
}

// GetAlignOff preserves the old name-only API for external callers. A name
// alone cannot identify a symbol-specific session profile, so it only returns
// an offset when the active adapter exposes one unambiguous profile across its
// current markets. New code should use GetAlignOffForMarket or
// GetAlignOffForExchange.
func GetAlignOff(exchangeName string, tfSecs int) int {
	if Default == nil || tfSecs < 24*60*60 {
		return 0
	}
	info := Default.Info()
	if info == nil || info.ID != exchangeName {
		return 0
	}
	var offset int
	for _, market := range Default.GetCurMarkets() {
		candidate := GetAlignOffForMarket(market, tfSecs)
		if candidate == 0 {
			continue
		}
		if offset != 0 && offset != candidate {
			return 0
		}
		offset = candidate
	}
	return offset
}
