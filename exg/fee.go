package exg

import (
	"strings"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// OrderFee calculates the fee of a filled order through the normalized SDK
// interface for every market type. A plain limit order type is rewritten to
// its maker form before the SDK call, including when the calculation fails.
func OrderFee(exchange banexg.BanExchange, symbol string, orderType *string, side string, filled, price float64) (*banexg.Fee, *errs.Error) {
	maker := strings.Contains(*orderType, "limit")
	if *orderType == banexg.OdTypeLimit {
		if maker {
			*orderType = banexg.OdTypeLimitMaker
		} else {
			*orderType = "limit_taker"
		}
	}
	return exchange.CalculateFee(symbol, *orderType, side, filled, price, maker, nil)
}
