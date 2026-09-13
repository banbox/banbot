package biz

import "github.com/banbox/banbot/data"

// DataDeps projects the runtime-owned dependencies needed by data providers.
// The returned value is a copy, so a provider cannot replace the owner's
// catalog or callback tracker.
func (d RuntimeDeps) DataDeps() *data.RuntimeDeps {
	deps := &data.RuntimeDeps{
		Core:       d.Core,
		Clock:      d.Clock,
		Config:     d.Config,
		Market:     d.Market,
		Symbols:    d.Symbols,
		Storage:    d.Storage,
		Strategies: d.Strategies,
		Catalog:    d.Catalog,
		Dump:       d.Dump,
		Callbacks:  d.Callbacks,
		Exchange:   d.Exchange,
	}
	if d.Core != nil {
		deps.ExchangeName = d.Core.ExgName
		deps.MarketType = d.Core.Market
	}
	if d.Exchange != nil {
		name, market, err := deps.ResolveIdentity()
		if err != nil {
			deps.IdentityErr = err
		} else {
			if deps.ExchangeName == "" {
				deps.ExchangeName = name
			}
			if deps.MarketType == "" {
				deps.MarketType = market
			}
		}
	}
	return deps
}
