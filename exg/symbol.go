package exg

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// PriceSymbolCapability is an optional exchange-owned symbol parser. It is
// intentionally local to banbot so older banexg releases remain supported.
type PriceSymbolCapability interface {
	PriceSymbolParts(symbol string) ([4]string, *errs.Error)
}

// PriceSymbolParserWithError is the checked form used at runtime composition
// boundaries. It resolves one raw pair lazily so core's hot parser cache can
// still decide when the adapter is called.
type PriceSymbolParserWithError func(pair string) ([4]string, *errs.Error)

// GetPriceSymbolCapability unwraps the bot adapter and returns nil when the
// installed banexg version does not provide this optional capability.
func GetPriceSymbolCapability(exchange banexg.BanExchange) PriceSymbolCapability {
	capability, _ := getExchangeCapability[PriceSymbolCapability](exchange)
	return capability
}

// NewPriceSymbolParserWithError adapts exchange-owned symbol semantics while
// preserving adapter errors. A nil exchange intentionally uses the generic
// parser because no exchange-specific semantics are available.
func NewPriceSymbolParserWithError(exgName string, exchange banexg.BanExchange) PriceSymbolParserWithError {
	return func(pair string) ([4]string, *errs.Error) {
		return resolvePriceSymbol(exchange, exgName, pair)
	}
}

// ResolvePriceSymbol resolves one pair through the adapter capability first,
// then through MapMarket metadata. A capability error or panic is terminal for
// this resolution; it must not be replaced by a generic parse.
func ResolvePriceSymbol(exchange banexg.BanExchange, pair string) ([4]string, *errs.Error) {
	return resolvePriceSymbol(exchange, "", pair)
}

// ResolveRuntimePriceSymbol resolves a symbol that already belongs to an
// explicit Runtime. Runtime symbol catalogs store canonical exchange symbols,
// so a runtime without an exchange-owned parser must use the generic parser
// instead of calling MapMarket again. Legacy callers keep ResolvePriceSymbol's
// metadata lookup behavior.
func ResolveRuntimePriceSymbol(exchange banexg.BanExchange, pair string) ([4]string, *errs.Error) {
	if capability := GetPriceSymbolCapability(exchange); capability != nil {
		return callPriceSymbolCapability(capability, pair)
	}
	return priceParts(pair), nil
}

func resolvePriceSymbol(exchange banexg.BanExchange, exgName, pair string) ([4]string, *errs.Error) {
	if exchange == nil {
		return priceParts(pair), nil
	}
	if capability := GetPriceSymbolCapability(exchange); capability != nil {
		return callPriceSymbolCapability(capability, pair)
	}
	market, err := mapPriceMarket(exchange, pair)
	if err != nil {
		return [4]string{}, err
	}
	return priceSymbolParts(pair, market), nil
}

// ValidatePriceSymbolParser checks the pairs known at a runtime composition
// boundary before installing the no-error core strategy. It is deliberately
// separate from the hot parser cache because core.SymbolParserStrategy cannot
// represent an error.
func ValidatePriceSymbolParser(parser PriceSymbolParserWithError, pairs ...string) *errs.Error {
	if parser == nil {
		return errs.NewMsg(errs.CodeParamInvalid, "price symbol parser is nil")
	}
	for _, pair := range pairs {
		if _, err := parser(pair); err != nil {
			return err
		}
	}
	return nil
}

// NewPriceSymbolParser is the compatibility facade for core's historical
// no-error strategy. Checked runtime composition should use
// NewPriceSymbolParserWithError and ValidatePriceSymbolParser first. If a
// caller bypasses that boundary, an unresolved pair returns an empty tuple
// rather than a different generic or China interpretation.
func NewPriceSymbolParser(exgName string, exchange banexg.BanExchange) core.SymbolParserStrategy {
	parser := NewPriceSymbolParserWithError(exgName, exchange)
	return func(pair string) [4]string {
		parts, _ := parser(pair)
		return parts
	}
}

func callPriceSymbolCapability(capability PriceSymbolCapability, pair string) (parts [4]string, err *errs.Error) {
	defer func() {
		if value := recover(); value != nil {
			parts = [4]string{}
			err = errs.NewMsg(errs.CodeRunTime,
				"price symbol capability panicked for %q: %v", pair, value)
		}
	}()
	parts, err = capability.PriceSymbolParts(pair)
	if err != nil {
		parts = [4]string{}
	}
	return parts, err
}

// NewLegacyPriceSymbolParser preserves the symbol aliases used by the old
// package-level price facade. Runtime callers should prefer
// NewPriceSymbolParser with exchange metadata or an explicit strategy.
func NewLegacyPriceSymbolParser(exgName string) core.SymbolParserStrategy {
	return func(pair string) [4]string { return priceParts(pair) }
}

func mapPriceMarket(exchange banexg.BanExchange, pair string) (market *banexg.Market, err *errs.Error) {
	if exchange == nil {
		return nil, nil
	}
	defer func() {
		if value := recover(); value != nil {
			market = nil
			err = errs.NewMsg(errs.CodeRunTime, "MapMarket panicked for %q: %v", pair, value)
		}
	}()
	market, err = exchange.MapMarket(pair, 0)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, errs.NewMsg(errs.CodeNoMarketForPair, "MapMarket returned nil market for %q", pair)
	}
	return market, nil
}

func priceSymbolParts(pair string, market *banexg.Market) [4]string {
	symbol := pair
	if market != nil && market.Symbol != "" {
		symbol = market.Symbol
	}
	parts := priceParts(symbol)
	if market == nil {
		return parts
	}
	if market.Base != "" {
		parts[0] = market.Base
	}
	if market.Quote != "" {
		parts[1] = market.Quote
	}
	if market.Settle != "" {
		parts[2] = market.Settle
	}
	return parts
}

func priceParts(pair string) [4]string {
	base, quote, settle, ident := core.SplitSymbol(pair)
	return [4]string{base, quote, settle, ident}
}
