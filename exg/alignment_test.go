package exg

import (
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type alignmentExchangeStub struct {
	banexg.BanExchange
	market       *banexg.Market
	mappedMarket *banexg.Market
	getErr       *errs.Error
	mapErr       *errs.Error
	panicOnGet   bool
	panicOnMap   bool
	getCalls     atomic.Int32
	mapCalls     atomic.Int32
}

func (s *alignmentExchangeStub) GetMarket(string) (*banexg.Market, *errs.Error) {
	s.getCalls.Add(1)
	if s.panicOnGet {
		panic("stub GetMarket failure")
	}
	return s.market, s.getErr
}

func (s *alignmentExchangeStub) MapMarket(string, int) (*banexg.Market, *errs.Error) {
	s.mapCalls.Add(1)
	if s.panicOnMap {
		panic("stub MapMarket failure")
	}
	return s.mappedMarket, s.mapErr
}

func TestGetAlignOffForMarketUsesSessionMetadata(t *testing.T) {
	withNight := &banexg.Market{
		DayTimes:   [][2]int64{{60 * 60 * 1000, 2 * 60 * 60 * 1000}, {5 * 60 * 60 * 1000, 7 * 60 * 60 * 1000}},
		NightTimes: [][2]int64{{13 * 60 * 60 * 1000, 15 * 60 * 60 * 1000}},
	}
	if got := GetAlignOffForMarket(withNight, 24*60*60); got != 50400 {
		t.Fatalf("session-derived daily offset = %d, want 50400", got)
	}
	if got := GetAlignOffForMarket(withNight, 60*60); got != 0 {
		t.Fatalf("sub-daily offset = %d, want 0", got)
	}

	withoutNight := &banexg.Market{Contract: true, DayTimes: [][2]int64{{60 * 60 * 1000, 7 * 60 * 60 * 1000}}}
	if got := GetAlignOffForMarket(withoutNight, 24*60*60); got != 0 {
		t.Fatalf("day-only contract offset = %d, want 0", got)
	}
}

func TestGetAlignOffForMarketDoesNotDependOnExchangeName(t *testing.T) {
	market := &banexg.Market{
		DayTimes:   [][2]int64{{5 * 60 * 60 * 1000, 7 * 60 * 60 * 1000}},
		NightTimes: [][2]int64{{13 * 60 * 60 * 1000, 15 * 60 * 60 * 1000}},
	}
	if got := GetAlignOffForMarket(market, 24*60*60); got != 50400 {
		t.Fatalf("non-named adapter metadata offset = %d, want 50400", got)
	}
}

func TestGetAlignOffForExchangeCheckedShortTimeframe(t *testing.T) {
	offset, err := GetAlignOffForExchangeChecked(nil, "BTC/USDT", 60*60)
	if offset != 0 || err != nil {
		t.Fatalf("short timeframe = (%d, %v), want (0, nil)", offset, err)
	}
}

func TestGetAlignOffForExchangeCheckedNilExchange(t *testing.T) {
	offset, err := GetAlignOffForExchangeChecked(nil, "BTC/USDT", 24*60*60)
	if offset != 0 || err == nil || err.Code != core.ErrExgNotInit {
		t.Fatalf("nil exchange = (%d, %v), want code %d", offset, err, core.ErrExgNotInit)
	}
}

func TestGetAlignOffForExchangeCheckedPreservesAdapterErrors(t *testing.T) {
	getErr := errs.NewMsg(errs.CodeParamInvalid, "get failed")
	getExchange := &alignmentExchangeStub{getErr: getErr}
	if offset, err := GetAlignOffForExchangeChecked(getExchange, "raw", 24*60*60); offset != 0 || err != getErr {
		t.Fatalf("GetMarket error = (%d, %v), want original error %v", offset, err, getErr)
	}
	if got := getExchange.mapCalls.Load(); got != 0 {
		t.Fatalf("MapMarket calls after GetMarket error = %d, want 0", got)
	}

	mapErr := errs.NewMsg(errs.CodeParamInvalid, "map failed")
	mapExchange := &alignmentExchangeStub{mapErr: mapErr}
	if offset, err := GetAlignOffForExchangeChecked(mapExchange, "raw", 24*60*60); offset != 0 || err != mapErr {
		t.Fatalf("MapMarket error = (%d, %v), want original error %v", offset, err, mapErr)
	}
}

func TestGetAlignOffForExchangeCheckedRecoversPanics(t *testing.T) {
	for _, exchange := range []*alignmentExchangeStub{
		{panicOnGet: true},
		{panicOnMap: true},
	} {
		offset, err := GetAlignOffForExchangeChecked(exchange, "raw", 24*60*60)
		if offset != 0 || err == nil || err.Code != core.ErrRunTime {
			t.Fatalf("adapter panic = (%d, %v), want code %d", offset, err, core.ErrRunTime)
		}
	}
}

func TestGetAlignOffForExchangeCheckedSuccess(t *testing.T) {
	market := &banexg.Market{
		DayTimes:   [][2]int64{{5 * 60 * 60 * 1000, 7 * 60 * 60 * 1000}},
		NightTimes: [][2]int64{{13 * 60 * 60 * 1000, 15 * 60 * 60 * 1000}},
	}
	exchange := &alignmentExchangeStub{mappedMarket: market}
	offset, err := GetAlignOffForExchangeChecked(exchange, "raw", 24*60*60)
	if offset != 50400 || err != nil {
		t.Fatalf("mapped alignment offset = (%d, %v), want (50400, nil)", offset, err)
	}
	if got := exchange.getCalls.Load(); got != 1 {
		t.Fatalf("GetMarket calls = %d, want 1", got)
	}
	if got := exchange.mapCalls.Load(); got != 1 {
		t.Fatalf("MapMarket calls = %d, want 1", got)
	}

	exchange = &alignmentExchangeStub{market: market, panicOnMap: true}
	offset, err = GetAlignOffForExchangeChecked(exchange, "known", 24*60*60)
	if offset != 50400 || err != nil {
		t.Fatalf("known alignment offset = (%d, %v), want (50400, nil)", offset, err)
	}
	if got := exchange.mapCalls.Load(); got != 0 {
		t.Fatalf("MapMarket calls for known market = %d, want 0", got)
	}
}
