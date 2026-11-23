package exg

import (
	"context"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type BotExchange struct {
	banexg.BanExchange
}

// UnderlyingExchange exposes SDK-owned optional capabilities to integrations.
func (e *BotExchange) UnderlyingExchange() banexg.BanExchange { return e.BanExchange }

// PutOrderRes is the record of one CreateOrder call as written to live dumps.
// Nothing produces it any more; the type stays registered with gob so dumps
// recorded by earlier versions still decode.
type PutOrderRes struct {
	Symbol    string
	OrderType string
	Side      string
	Amount    float64
	Price     float64
	Params    map[string]interface{}
	Order     *banexg.Order
	Err       *errs.Error
}

// getExchangeCapability resolves an optional capability from either the
// adapter itself or the local BotExchange wrapper. Keeping this boundary in
// one place prevents each capability facade from implementing subtly
// different nil and wrapper handling.
func getExchangeCapability[T any](exchange banexg.BanExchange) (T, bool) {
	var zero T
	if exchange == nil {
		return zero, false
	}
	if capability, ok := exchange.(T); ok {
		return capability, true
	}
	if wrapper, ok := exchange.(interface{ UnderlyingExchange() banexg.BanExchange }); ok {
		return getExchangeCapability[T](wrapper.UnderlyingExchange())
	}
	wrapper, ok := exchange.(*BotExchange)
	if !ok || wrapper == nil || wrapper.BanExchange == nil {
		return zero, false
	}
	capability, ok := wrapper.BanExchange.(T)
	if !ok {
		return zero, false
	}
	return capability, true
}

func (e *BotExchange) FetchOHLCVArchive(ctx context.Context, symbol, timeframe string, startMS, endMS int64) (
	[]*banexg.Kline, bool, *errs.Error) {
	fetcher, ok := e.BanExchange.(banexg.OHLCVArchiveFetcher)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchOHLCVArchive(ctx, symbol, timeframe, startMS, endMS)
}

func (e *BotExchange) FetchPriceOHLCVArchive(ctx context.Context, priceType, symbol, timeframe string, startMS, endMS int64) (
	[]*banexg.Kline, bool, *errs.Error) {
	fetcher, ok := e.BanExchange.(banexg.PriceOHLCVArchiveFetcher)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchPriceOHLCVArchive(ctx, priceType, symbol, timeframe, startMS, endMS)
}

// FetchPriceOHLCVArchive invokes the optional archive capability when the
// underlying exchange adapter supports immutable mark/index/premiumIndex files.
func FetchPriceOHLCVArchive(ctx context.Context, exchange banexg.BanExchange, priceType, symbol, timeframe string, startMS, endMS int64) (
	[]*banexg.Kline, bool, *errs.Error) {
	fetcher, ok := getExchangeCapability[banexg.PriceOHLCVArchiveFetcher](exchange)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchPriceOHLCVArchive(ctx, priceType, symbol, timeframe, startMS, endMS)
}

func (e *BotExchange) FetchFundingRateArchive(ctx context.Context, symbol string, startMS, endMS int64) (
	[]*banexg.FundingRate, bool, *errs.Error) {
	fetcher, ok := e.BanExchange.(banexg.FundingRateArchiveFetcher)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchFundingRateArchive(ctx, symbol, startMS, endMS)
}

// FetchFundingRateArchive invokes the optional archive capability when the
// underlying exchange adapter supports immutable historical funding files.
func FetchFundingRateArchive(ctx context.Context, exchange banexg.BanExchange, symbol string, startMS, endMS int64) (
	[]*banexg.FundingRate, bool, *errs.Error) {
	fetcher, ok := getExchangeCapability[banexg.FundingRateArchiveFetcher](exchange)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchFundingRateArchive(ctx, symbol, startMS, endMS)
}

func (e *BotExchange) FetchDerivativesMetricsArchive(ctx context.Context, symbol, timeframe string, startMS, endMS int64) (
	[]*banexg.DerivativesMetric, bool, *errs.Error) {
	fetcher, ok := e.BanExchange.(banexg.DerivativesMetricsArchiveFetcher)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchDerivativesMetricsArchive(ctx, symbol, timeframe, startMS, endMS)
}

// FetchDerivativesMetricsArchive invokes the optional archive capability when
// the underlying exchange adapter supports historical derivatives metrics files.
func FetchDerivativesMetricsArchive(ctx context.Context, exchange banexg.BanExchange, symbol, timeframe string, startMS, endMS int64) (
	[]*banexg.DerivativesMetric, bool, *errs.Error) {
	fetcher, ok := getExchangeCapability[banexg.DerivativesMetricsArchiveFetcher](exchange)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchDerivativesMetricsArchive(ctx, symbol, timeframe, startMS, endMS)
}

func (e *BotExchange) FetchDerivativesMetrics(ctx context.Context, symbol, timeframe string, startMS, endMS int64, limit int, params map[string]interface{}) (
	[]*banexg.DerivativesMetric, bool, *errs.Error) {
	fetcher, ok := e.BanExchange.(banexg.DerivativesMetricsFetcher)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchDerivativesMetrics(ctx, symbol, timeframe, startMS, endMS, limit, params)
}

// FetchDerivativesMetrics invokes the optional derivatives metrics capability
// (combining historical archives and recent REST endpoints when supported).
func FetchDerivativesMetrics(ctx context.Context, exchange banexg.BanExchange, symbol, timeframe string, startMS, endMS int64, limit int, params map[string]interface{}) (
	[]*banexg.DerivativesMetric, bool, *errs.Error) {
	fetcher, ok := getExchangeCapability[banexg.DerivativesMetricsFetcher](exchange)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchDerivativesMetrics(ctx, symbol, timeframe, startMS, endMS, limit, params)
}
