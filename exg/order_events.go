package exg

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// OrderEventCapability isolates adapter-owned order event semantics from the
// order manager. Implementations must use the stable, normalized banexg data
// model and must not expose exchange-specific details to callers.
type OrderEventCapability interface {
	ParseClientOrderID(namespace, clientOrderID string) int64
	GetOrderEventOrderID(trade *banexg.MyTrade) string
	NormalizeOrderTimestamp(order *banexg.Order, fallback int64) int64
}

// GetOrderEventCapability unwraps the BotExchange boundary and supplies the
// compatibility implementation required by older banexg adapters.
func GetOrderEventCapability(exchange banexg.BanExchange) OrderEventCapability {
	if capability := nativeOrderEventCapability(exchange); capability != nil {
		return capability
	}
	if exchange == nil {
		return nil
	}
	// The safe default is implemented by banexg. Banbot must not reconstruct
	// an exchange's client-order or algo-order format here.
	return banexg.GetOrderEventCapability(exchange)
}

// HasNativeOrderEventCapability reports whether the adapter itself supplies
// order-event semantics.
func HasNativeOrderEventCapability(exchange banexg.BanExchange) bool {
	return nativeOrderEventCapability(exchange) != nil
}

func nativeOrderEventCapability(exchange banexg.BanExchange) OrderEventCapability {
	capability, _ := getExchangeCapability[OrderEventCapability](exchange)
	return capability
}

// RequireOrderEventCapability makes the live-trading compatibility boundary
// explicit while allowing older adapters in non-trading contexts.
func RequireOrderEventCapability(exchange banexg.BanExchange, strict bool) (OrderEventCapability, *errs.Error) {
	capability := nativeOrderEventCapability(exchange)
	if capability != nil {
		return capability, nil
	}
	if strict {
		return nil, errs.NewMsg(core.ErrBadConfig,
			"exchange adapter is required to provide order-event capability")
	}
	if exchange == nil {
		return nil, nil
	}
	return banexg.GetOrderEventCapability(exchange), nil
}
