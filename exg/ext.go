package exg

import (
	"context"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type BotExchange struct {
	banexg.BanExchange
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
	if AfterCreateOrder != nil {
		err2 := AfterCreateOrder(&PutOrderRes{
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

func (e *BotExchange) FetchOHLCVArchive(ctx context.Context, symbol, timeframe string, startMS, endMS int64) (
	[]*banexg.Kline, bool, *errs.Error) {
	fetcher, ok := e.BanExchange.(banexg.OHLCVArchiveFetcher)
	if !ok {
		return nil, false, nil
	}
	return fetcher.FetchOHLCVArchive(ctx, symbol, timeframe, startMS, endMS)
}
