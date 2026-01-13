package exg

import (
	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banexg"
)

var Default banexg.BanExchange
var exgMap = map[string]banexg.BanExchange{}
var exgMapLock deadlock.Mutex
var AllowExgIds = map[string]bool{
	"binance": true,
	"okx":     true,
	"bybit":   true,
	"china":   true,
}
