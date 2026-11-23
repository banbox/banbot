package entry

import (
	"context"
	"errors"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/live"
)

// The embedded production time-series account always builds the Hub order
// manager; there is no configuration that selects another facade.
func TestTimeSeriesExecutionAccountAlwaysBuildsHubOrderMgr(t *testing.T) {
	session, snapshot, c, binding, _ := timeSeriesAccountStartupFixture(t)
	var manager biz.IOrderMgr
	stop := context.Canceled
	err := session.runTimeSeriesExecutionAccount(context.Background(), snapshot, c, binding, func(_ context.Context, trader *live.CryptoTrader) error {
		manager = trader.RuntimeDependencies().Trading.OrderManager(c.AccountID)
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("startup not reached: %v", err)
	}
	if _, ok := manager.(*biz.HubOrderMgr); !ok {
		t.Fatalf("built %T, want *biz.HubOrderMgr", manager)
	}
}
