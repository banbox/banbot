package exg

import (
	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banexg"
)

var Default banexg.BanExchange
var exgMap = map[string]banexg.BanExchange{}
var exgMapLock deadlock.Mutex
var allowedExgIDs = map[string]struct{}{
	"binance": {},
	"okx":     {},
	"bybit":   {},
	"china":   {},
}

// IsAllowedExgID reports whether an exchange identifier is supported by the
// process registry. The registry is immutable after initialization.
func IsAllowedExgID(id string) bool {
	_, ok := allowedExgIDs[id]
	return ok
}
