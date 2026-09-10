package exg

import (
	"context"
	"sync"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type BotExchange struct {
	banexg.BanExchange
	orderCallbackMu  sync.RWMutex
	orderCallback    func(*PutOrderRes) *errs.Error
	orderCallbackSet bool
}

var (
	AfterCreateOrder func(*PutOrderRes) *errs.Error
)

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

func (e *BotExchange) CreateOrder(symbol, odType, side string, amount, price float64, params map[string]interface{}) (*banexg.Order, *errs.Error) {
	order, err := e.BanExchange.CreateOrder(symbol, odType, side, amount, price, params)
	e.orderCallbackMu.RLock()
	callback, callbackSet := e.orderCallback, e.orderCallbackSet
	e.orderCallbackMu.RUnlock()
	if !callbackSet {
		callback = AfterCreateOrder
	}
	if callback != nil {
		err2 := callback(&PutOrderRes{
			Symbol:    symbol,
			OrderType: odType,
			Side:      side,
			Amount:    amount,
			Price:     price,
			Params:    params,
			Order:     order,
			Err:       err,
		})
		if err2 != nil {
			return order, err2
		}
	}
	return order, err
}

// SetOrderCallback binds an application-owned order callback to this exchange
// instance. A callback set here takes precedence over the legacy package hook,
// including an explicit nil callback which disables the legacy fallback.
func (e *BotExchange) SetOrderCallback(callback func(*PutOrderRes) *errs.Error) {
	if e == nil {
		return
	}
	e.orderCallbackMu.Lock()
	e.orderCallback = callback
	e.orderCallbackSet = true
	e.orderCallbackMu.Unlock()
}

// SetOrderCallback binds an order callback when the supplied exchange exposes
// the optional instance-level capability. It returns false for foreign test or
// third-party adapters that do not implement the capability.
func SetOrderCallback(exchange banexg.BanExchange, callback func(*PutOrderRes) *errs.Error) bool {
	if setter, ok := exchange.(interface {
		SetOrderCallback(func(*PutOrderRes) *errs.Error)
	}); ok {
		setter.SetOrderCallback(callback)
		return true
	}
	return false
}

func (e *BotExchange) FetchOHLCVArchive(ctx context.Context, symbol, timeframe string, startMS, endMS int64) (
	[]*banexg.Kline, bool, *errs.Error) {
	fetcher, ok := e.BanExchange.(banexg.OHLCVArchiveFetcher)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchOHLCVArchive(ctx, symbol, timeframe, startMS, endMS)
}
