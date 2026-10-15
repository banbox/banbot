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

func callClientOrderCapability(capability ClientOrderCapability, namespace string, orderID int64, clientID string, randomize bool) (string, *errs.Error) {
	return guardClientOrderID(func() string {
		return capability.BuildClientOrderID(namespace, orderID, clientID, randomize)
	})
}

func guardClientOrderID(build func() string) (value string, err *errs.Error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			value = ""
			err = errs.NewMsg(core.ErrRunTime, "client order capability panicked: %v", recovered)
		}
	}()
	value = build()
	if value == "" {
		return "", errs.NewMsg(core.ErrRunTime, "client order capability returned an empty identifier")
	}
	return value, nil
}

// ClientOrderPurposeCapability is the optional adapter contract for stable
// per-purpose identifiers. Adapters without it keep the randomized identifier.
type ClientOrderPurposeCapability = banexg.ClientOrderPurposeCapability

// SubmitReconcileCapability is the optional adapter contract for resolving a
// submission whose response never reached the client.
type SubmitReconcileCapability = banexg.SubmitReconcileCapability

// GetClientOrderPurposeCapability unwraps BotExchange and reports whether the
// adapter can build a stable identifier for one purpose of a local order.
func GetClientOrderPurposeCapability(exchange banexg.BanExchange) ClientOrderPurposeCapability {
	capability, _ := getExchangeCapability[ClientOrderPurposeCapability](exchange)
	return capability
}

// GetSubmitReconcileCapability unwraps BotExchange and reports whether the
// adapter can confirm the exchange-side result of an uncertain submission.
func GetSubmitReconcileCapability(exchange banexg.BanExchange) SubmitReconcileCapability {
	capability, _ := getExchangeCapability[SubmitReconcileCapability](exchange)
	return capability
}

// BuildPurposeClientOrderIDChecked returns an identifier that is stable for one
// purpose of one local order, so entry and exit cannot collide and a resubmit
// reuses the same idempotency key. It fails when the adapter has no such
// contract, and callers then fall back to BuildClientOrderIDChecked.
func BuildPurposeClientOrderIDChecked(exchange banexg.BanExchange, namespace string, orderID int64,
	clientID string, purpose banexg.ClientOrderPurpose) (string, *errs.Error) {
	capability := GetClientOrderPurposeCapability(exchange)
	if capability == nil {
		return "", errs.NewMsg(errs.CodeNotSupport,
			"exchange adapter does not provide client order purpose capability")
	}
	return guardClientOrderID(func() string {
		return capability.BuildPurposeClientOrderID(namespace, orderID, clientID, purpose)
	})
}
