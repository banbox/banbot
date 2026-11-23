package entry

import (
	"context"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banexg"
)

// NewBanexgLiveBinding is the stock banexg account binding (the one used by
// the default live provider). Registering it under a custom name with
// RegisterFactorLiveBinding runs that account through the embedded execution
// path (env: prod, execution.live_provider: <name>) with the same SDK
// capabilities, instead of the default Hub.
func NewBanexgLiveBinding(ctx context.Context, exchange banexg.BanExchange, snapshot *config.Snapshot, c runner.Config) (FactorLiveBinding, error) {
	return newBanexgFactorLiveBinding(ctx, exchange, snapshot, c)
}
