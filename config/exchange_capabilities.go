package config

import (
	"strings"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/bex"
)

// ExchangeUsesOpaqueSymbols asks the adapter whether a symbol cannot be
// normalized by the generic slash-delimited pair rules. The adapter owns this
// decision; config only consumes the capability at its low-frequency boundary.
func ExchangeUsesOpaqueSymbols(name string) bool {
	exchange, ok := newCapabilityExchange(name)
	if !ok {
		return false
	}
	defer exchange.Close()
	_, ok = exchange.(banexg.PriceSymbolCapability)
	return ok
}

// ExchangeDefaultLocation derives the display location from adapter metadata.
// A caller-supplied location still takes precedence; an adapter that does not
// publish a regional default leaves the existing UTC default unchanged.
func ExchangeDefaultLocation(name string) *time.Location {
	exchange, ok := newCapabilityExchange(name)
	if !ok {
		return nil
	}
	defer exchange.Close()
	info := exchange.Info()
	if info == nil {
		return nil
	}
	for _, country := range info.Countries {
		if strings.EqualFold(strings.TrimSpace(country), "CN") {
			location, err := time.LoadLocation("Asia/Shanghai")
			if err == nil {
				return location
			}
			return nil
		}
	}
	return nil
}

func newCapabilityExchange(name string) (banexg.BanExchange, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, false
	}
	exchange, err := bex.New(name, nil)
	if err != nil || exchange == nil {
		return nil, false
	}
	return exchange, true
}
