package execution

import (
	"math"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func TestOHLCProfileStopAndProtectionFixedFacts(t *testing.T) {
	const startMS = int64(1_700_000_040_000)
	bar := &orm.SeriesOHLCV{Time: startMS, Open: 100, High: 110, Low: 90, Close: 105}
	legacy := OHLCProfile{LegacyIntrabar: true}
	for _, test := range []struct {
		name             string
		profile          OHLCProfile
		order            OHLCOrder
		matched, cleared bool
		price            float64
		offset           int64
	}{
		{"stop long", legacy, OHLCOrder{OrderType: banexg.OdTypeMarket, IsBuy: true, Enter: true, Stop: 105, CreateAt: startMS}, true, true, 105, 42_000},
		{"stop short", legacy, OHLCOrder{OrderType: banexg.OdTypeMarket, Short: true, Enter: true, Stop: 95, CreateAt: startMS}, true, true, 95, 8_000},
		{"legacy gap stop", legacy, OHLCOrder{OrderType: banexg.OdTypeMarket, IsBuy: true, Enter: true, Stop: 85, CreateAt: startMS}, true, true, 85, 0},
		{"current gap stop", OHLCProfile{}, OHLCOrder{OrderType: banexg.OdTypeMarket, IsBuy: true, Enter: true, Stop: 85, CreateAt: startMS}, false, false, 0, 0},
		{"unreached stop", legacy, OHLCOrder{OrderType: banexg.OdTypeMarket, IsBuy: true, Enter: true, Stop: 115, CreateAt: startMS}, false, false, 0, 0},
		{"triggered long stop limit", legacy, OHLCOrder{OrderType: banexg.OdTypeLimit, IsBuy: true, Enter: true, Stop: 105, Price: 95, CreateAt: startMS}, true, true, 95, 25_000},
		{"stop clears while limit pending", legacy, OHLCOrder{OrderType: banexg.OdTypeLimit, IsBuy: true, Enter: true, Stop: 105, Price: 85, CreateAt: startMS}, false, true, 85, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			fill, matched := test.profile.MatchPending(bar, test.order, 60, 15)
			if matched != test.matched || fill.StopTriggered != test.cleared || matched && (fill.Price != test.price || fill.TimeMS != bar.Time+test.offset) {
				t.Fatalf("matched=%v fill=%+v", matched, fill)
			}
		})
	}
	for _, test := range []struct {
		name                string
		short               bool
		sl, tp              *OHLCProtection
		ready, hitSL, hitTP bool
		price               float64
		offset              int64
		orderType           string
	}{
		{"dual hit favors SL", false, &OHLCProtection{Price: 95}, &OHLCProtection{Price: 105}, true, true, true, 95, 8_572, banexg.OdTypeMarket},
		{"TP only", false, nil, &OHLCProtection{Price: 105}, true, false, true, 105, 42_858, banexg.OdTypeMarket},
		{"short SL", true, &OHLCProtection{Price: 105}, nil, true, true, false, 105, 42_858, banexg.OdTypeMarket},
		{"short TP", true, nil, &OHLCProtection{Price: 95}, true, false, true, 95, 8_572, banexg.OdTypeMarket},
		{"SL limit", false, &OHLCProtection{Price: 95, Limit: 100}, nil, true, true, false, 100, 34_286, banexg.OdTypeLimit},
		{"TP limit override", false, nil, &OHLCProtection{Price: 105, Limit: 100}, true, false, true, 100, 34_286, banexg.OdTypeLimit},
		{"SL blocked despite TP", false, &OHLCProtection{Price: 95, Limit: 115}, &OHLCProtection{Price: 105}, false, true, true, 0, 0, ""},
		{"sticky SL gap", false, &OHLCProtection{Price: 85, Hit: true}, nil, true, true, false, 100, 0, banexg.OdTypeMarket},
		{"unhit protection", false, &OHLCProtection{Price: 85}, &OHLCProtection{Price: 115}, false, false, false, 0, 0, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			fill := legacy.MatchProtection(bar, test.short, test.sl, test.tp, 0, 60, startMS+60_000)
			if fill.Ready != test.ready || fill.HitSL != test.hitSL || fill.HitTP != test.hitTP || fill.Ready && (math.Abs(fill.Price-test.price) > 1e-12 || fill.TimeMS != bar.Time+test.offset || fill.OrderType != test.orderType) {
				t.Fatalf("protection fill=%+v", fill)
			}
		})
	}
}

type ohlcSDKExchange struct {
	banexg.BanExchange
	market    *banexg.Market
	orderType *string
	feeErr    *errs.Error
}

func (e *ohlcSDKExchange) GetMarket(string) (*banexg.Market, *errs.Error) { return e.market, nil }
func (e *ohlcSDKExchange) PrecPrice(market *banexg.Market, price float64) (float64, *errs.Error) {
	if market != e.market {
		panic("market identity changed")
	}
	return math.Round(price*100) / 100, nil
}
func (e *ohlcSDKExchange) PrecAmount(market *banexg.Market, amount float64) (float64, *errs.Error) {
	if market != e.market {
		panic("market identity changed")
	}
	return math.Floor(amount*10) / 10, nil
}
func (e *ohlcSDKExchange) CalculateFee(symbol, orderType, side string, amount, price float64, maker bool, _ map[string]interface{}) (*banexg.Fee, *errs.Error) {
	if e.orderType != nil && *e.orderType != orderType {
		panic("type must normalize before SDK callback")
	}
	if symbol != e.market.Symbol || side != banexg.OdSideBuy {
		panic("SDK order identity changed")
	}
	rate := .002
	if maker {
		rate = .001
	}
	return &banexg.Fee{Currency: "USD", Cost: amount * price * rate, QuoteCost: amount * price * rate}, e.feeErr
}

func TestOHLCProfilePreservesSDKMarketsPrecisionAndFees(t *testing.T) {
	for _, market := range []*banexg.Market{
		{Symbol: "COIN/USD", Spot: true},
		{Symbol: "COIN/USD:COIN", Contract: true, Inverse: true, Swap: true},
		{Symbol: "COIN/USD:USD", Contract: true, Linear: true, Future: true},
	} {
		t.Run(market.Symbol, func(t *testing.T) {
			exchange := &ohlcSDKExchange{market: market}
			gotMarket, price, err := OHLCEntryPrice(exchange, market.Symbol, 91.257)
			if err != nil || gotMarket != market || price != 91.26 {
				t.Fatalf("price=%g err=%v", price, err)
			}
			raw, amount, err := OHLCEntryAmount(exchange, market, 200, price)
			if err != nil || raw != 200/91.26 || amount != 2.1 {
				t.Fatalf("quantity raw=%g amount=%g err=%v", raw, amount, err)
			}
			for _, orderType := range []string{banexg.OdTypeLimit, banexg.OdTypeMarket, "stop_limit"} {
				exchange.orderType = &orderType
				fee, err := LegacyOrderFee(exchange, market.Symbol, &orderType, banexg.OdSideBuy, amount, price)
				rate := .002
				if orderType != banexg.OdTypeMarket {
					rate = .001
				}
				if err != nil || fee.Currency != "USD" || fee.Cost != amount*price*rate || fee.QuoteCost != fee.Cost {
					t.Fatalf("fee=%+v err=%v", fee, err)
				}
			}
			orderType := banexg.OdTypeLimit
			exchange.orderType = &orderType
			exchange.feeErr = errs.NewMsg(errs.CodeRunTime, "venue fee failure")
			_, err = LegacyOrderFee(exchange, market.Symbol, &orderType, banexg.OdSideBuy, amount, price)
			if err != exchange.feeErr || orderType != banexg.OdTypeLimitMaker {
				t.Fatalf("fee failure lost normalization: type=%s err=%v", orderType, err)
			}
		})
	}
}
