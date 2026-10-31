package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type limitPriceSession struct {
	*fakeBanexgSession
	load       func(string, string, int64, int, map[string]any) ([]*banexg.Kline, *errs.Error)
	bookCalls  int
	bookParams map[string]any
}

func (s *limitPriceSession) FetchOHLCV(symbol, timeframe string, since int64, limit int, params map[string]any) ([]*banexg.Kline, *errs.Error) {
	return s.load(symbol, timeframe, since, limit, params)
}

func (s *limitPriceSession) FetchOrderBook(symbol string, limit int, params map[string]any) (*banexg.OrderBook, *errs.Error) {
	s.bookCalls++
	s.bookParams = params
	return s.fakeBanexgSession.FetchOrderBook(symbol, limit, params)
}

func TestBanexgDefaultLimitPriceClosedVolumeAndTick(t *testing.T) {
	_, _, base, transport, config := banexgFixture(t)
	config.Instruments[0].Instrument.PriceTick = intentPrice("0.25")
	base.book = &banexg.OrderBook{Symbol: "BTC/USDT:USDT", TimeStamp: time.Now().UnixMilli(),
		Bids: &banexg.OdBookSide{Price: []float64{100, 98, 96}, Size: []float64{2, 4, 4}},
		Asks: &banexg.OdBookSide{Price: []float64{102, 104, 106}, Size: []float64{2, 4, 4}}}
	session := &limitPriceSession{fakeBanexgSession: base}
	ctx := context.WithValue(context.Background(), struct{}{}, "owner")
	session.load = func(symbol, timeframe string, since int64, limit int, params map[string]any) ([]*banexg.Kline, *errs.Error) {
		if symbol != base.book.Symbol || timeframe != "1m" || limit != 6 || since%60000 != 0 || params[banexg.ParamContext] != ctx || params[banexg.ParamAccount] != config.Account.Account {
			t.Fatalf("invalid volume request: %s %s %d %d %+v", symbol, timeframe, since, limit, params)
		}
		var rows []*banexg.Kline
		// Reverse order proves the timestamp, not response ordering, selects last minute.
		for i := int64(4); i >= 0; i-- {
			rows = append(rows, &banexg.Kline{Time: since + i*60000, Volume: 60})
		}
		// A large still-open minute must not affect the calculation.
		return append(rows, &banexg.Kline{Time: since + 5*60000, Volume: 600000}), nil
	}
	invokes := 0
	transport.invoke = func(got context.Context, call func() error) error {
		if got != ctx {
			t.Fatal("transport lost context")
		}
		invokes++
		return call()
	}
	adapter, err := NewBanexgAdapter(ctx, session, config)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		side OrderSide
		want float64
	}{{Buy, 98.75}, {Sell, 103.25}} {
		price, err := adapter.DefaultLimitPrice(ctx, "BTC", tc.side, 3)
		if err != nil || price != tc.want {
			t.Fatalf("%s price = %v, %v; want %v", tc.side, price, err, tc.want)
		}
	}
	if invokes != 4 || session.bookCalls != 2 || session.bookParams[banexg.ParamContext] != ctx {
		t.Fatalf("requests bypassed transport/context: %d %+v", invokes, session.bookParams)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := adapter.DefaultLimitPrice(canceled, "BTC", Buy, 3); !errors.Is(err, context.Canceled) || invokes != 4 {
		t.Fatalf("canceled call = %v; invokes=%d", err, invokes)
	}
}

func TestBanexgDefaultLimitPriceFailsClosed(t *testing.T) {
	for _, scenario := range []string{"missing-volume", "open-only", "duplicate", "invalid-volume", "missing-book", "stale-book", "malformed-book", "cancel-after-volume"} {
		t.Run(scenario, func(t *testing.T) {
			_, _, base, _, config := banexgFixture(t)
			base.book = &banexg.OrderBook{Symbol: "BTC/USDT:USDT", TimeStamp: time.Now().UnixMilli(),
				Bids: &banexg.OdBookSide{Price: []float64{100}, Size: []float64{2}},
				Asks: &banexg.OdBookSide{Price: []float64{102}, Size: []float64{2}}}
			session := &limitPriceSession{fakeBanexgSession: base}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			session.load = func(_, _ string, since int64, _ int, _ map[string]any) ([]*banexg.Kline, *errs.Error) {
				var rows []*banexg.Kline
				for i := int64(0); i < 5; i++ {
					rows = append(rows, &banexg.Kline{Time: since + i*60000, Volume: 60})
				}
				switch scenario {
				case "missing-volume":
					rows = rows[1:]
				case "open-only":
					rows = []*banexg.Kline{{Time: since + 5*60000, Volume: 60}}
				case "duplicate":
					rows[4] = rows[0]
				case "invalid-volume":
					rows[0].Volume = -1
				case "missing-book":
					base.book = nil
				case "stale-book":
					base.book.TimeStamp -= 60000
				case "malformed-book":
					base.book.Bids.Size = nil
				case "cancel-after-volume":
					cancel()
				}
				return rows, nil
			}
			adapter, err := NewBanexgAdapter(ctx, session, config)
			if err != nil {
				t.Fatal(err)
			}
			if price, err := adapter.DefaultLimitPrice(ctx, "BTC", Buy, 3); err == nil || price != 0 {
				t.Fatalf("unsafe fallback: %v, %v", price, err)
			}
		})
	}
}
