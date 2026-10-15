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

// BuildClientOrderID keeps client-order formatting at the exchange boundary.
// It is the compatibility facade for callers that cannot return an error. New
// live paths should use BuildClientOrderIDChecked so an adapter failure cannot
// silently change the exchange contract.
func BuildClientOrderID(exchange banexg.BanExchange, exchangeName, namespace string, orderID int64, clientID string, randomize bool) string {
	value, err := BuildClientOrderIDChecked(exchange, exchangeName, namespace, orderID, clientID, randomize)
	if err == nil {
		return value
	}
	return buildLegacyClientOrderID(exchange, exchangeName, namespace, orderID, clientID, randomize)
}

// BuildClientOrderIDChecked returns an adapter-owned identifier or an explicit
// error when the adapter does not provide the capability or rejects it.
func BuildClientOrderIDChecked(exchange banexg.BanExchange, exchangeName, namespace string, orderID int64, clientID string, randomize bool) (string, *errs.Error) {
	if capability := GetClientOrderCapability(exchange); capability != nil {
		value, err := callClientOrderCapability(capability, namespace, orderID, clientID, randomize)
		if err != nil {
			return "", err
		}
		return value, nil
	}
	return "", errs.NewMsg(errs.CodeNotSupport,
		"exchange adapter does not provide client order capability")
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
