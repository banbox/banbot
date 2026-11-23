package exg

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// ClientOrderCapability is the exchange-boundary client-order ID contract.
// The type is owned by banexg so adapters, rather than banbot, own formats.
type ClientOrderCapability = banexg.ClientOrderCapability

// ClientOrderIDCapability is the descriptive alias used by callers that
// refer to the generated identifier rather than the operation.
type ClientOrderIDCapability = ClientOrderCapability

// BuildClientOrderID keeps client-order formatting at the exchange boundary:
// the adapter-owned capability when it is available and healthy, otherwise the
// banexg legacy format.
func BuildClientOrderID(exchange banexg.BanExchange, exchangeName, namespace string, orderID int64, clientID string, randomize bool) string {
	if capability := GetClientOrderCapability(exchange); capability != nil {
		if value, err := callClientOrderCapability(capability, namespace, orderID, clientID, randomize); err == nil {
			return value
		}
	}
	return buildLegacyClientOrderID(exchange, exchangeName, namespace, orderID, clientID, randomize)
}

func buildLegacyClientOrderID(_ banexg.BanExchange, exchangeName, namespace string, orderID int64, clientID string, randomize bool) string {
	return banexg.BuildLegacyClientOrderID(exchangeName, namespace, orderID, clientID, randomize)
}

// GetClientOrderCapability unwraps BotExchange and reports the optional
// adapter-owned client-order formatter, if present.
func GetClientOrderCapability(exchange banexg.BanExchange) ClientOrderCapability {
	capability, _ := getExchangeCapability[ClientOrderCapability](exchange)
	return capability
}

func callClientOrderCapability(capability ClientOrderCapability, namespace string, orderID int64, clientID string, randomize bool) (value string, err *errs.Error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			value = ""
			err = errs.NewMsg(core.ErrRunTime, "client order capability panicked: %v", recovered)
		}
	}()
	value = capability.BuildClientOrderID(namespace, orderID, clientID, randomize)
	if value == "" {
		return "", errs.NewMsg(core.ErrRunTime, "client order capability returned an empty identifier")
	}
	return value, nil
}
