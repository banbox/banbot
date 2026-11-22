package config

import (
	"strings"
	"sync"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/bex"
)

type exchangeCapabilities struct {
	opaqueSymbols bool
	location      *time.Location
}

var (
	capabilityMu    sync.Mutex
	capabilityCache = make(map[string]exchangeCapabilities)
)

// ExchangeUsesOpaqueSymbols asks the adapter whether a symbol cannot be
// normalized by the generic slash-delimited pair rules. The adapter owns this
// decision; config only consumes the capability at its low-frequency boundary.
func ExchangeUsesOpaqueSymbols(name string) bool {
	caps, ok := loadExchangeCapabilities(name)
	return ok && caps.opaqueSymbols
}

// ExchangeDefaultLocation derives the display location from adapter metadata.
// A caller-supplied location still takes precedence; an adapter that does not
// publish a regional default leaves the existing UTC default unchanged.
func ExchangeDefaultLocation(name string) *time.Location {
	caps, ok := loadExchangeCapabilities(name)
	if !ok {
		return nil
	}
	return caps.location
}

// loadExchangeCapabilities creates the adapter once per exchange name and
// caches its answers. Config resolves these values several times per run, and
// every adapter creation used to repeat the proxy probe.
func loadExchangeCapabilities(name string) (exchangeCapabilities, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return exchangeCapabilities{}, false
	}
	capabilityMu.Lock()
	defer capabilityMu.Unlock()
	if caps, ok := capabilityCache[name]; ok {
		return caps, true
	}
	// Metadata only, no network: an explicit "no" proxy skips system detection.
	exchange, err := bex.New(name, map[string]interface{}{banexg.OptProxy: "no"})
	if err != nil || exchange == nil {
		return exchangeCapabilities{}, false
	}
	defer exchange.Close()
	var caps exchangeCapabilities
	_, caps.opaqueSymbols = exchange.(banexg.PriceSymbolCapability)
	if info := exchange.Info(); info != nil {
		caps.location = locationForCountries(info.Countries)
	}
	capabilityCache[name] = caps
	return caps, true
}

func locationForCountries(countries []string) *time.Location {
	for _, country := range countries {
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
