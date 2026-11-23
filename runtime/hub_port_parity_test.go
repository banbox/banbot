package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// portFixture is newHubTSFixture over either transport of the same Hub API:
// the Unix-socket client or the in-process port. The wrapped hubTSFixture has
// no server for the local variant, so refusals are counted via the port.
type portFixture struct {
	*hubTSFixture
	local *execution.LocalHubPort
}

func newPortFixture(t *testing.T, local bool) *portFixture {
	t.Helper()
	if !local {
		return &portFixture{hubTSFixture: newHubTSFixture(t, nil)}
	}
	owner := newSharedTriggerFixtureWithAdapter(t, nil, true)
	hub, err := execution.NewHub(func(context.Context, execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		var service *execution.SharedAccount
		err := owner.rt.SharedExecution().WithState(func(s *execution.SharedAccount) error { service = s; return nil })
		return execution.HubAccountBinding{Account: service, Shutdown: func() error { return nil }}, err
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	dir, err := os.MkdirTemp("", "hub-port-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	port := execution.NewLocalHubPort(hub)
	t.Cleanup(port.Close)
	if err = port.Register(context.Background(), execution.HubAccountRegistration{Account: owner.key, Env: core.RunEnvDryRun, Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	bridge := &biz.HubOrderBridgeConfig{Client: port, Account: owner.key, Bridge: *owner.bridge, WriterID: "test-robot", StatePath: filepath.Join(dir, "robot.json")}
	rt, err := owner.process.NewRuntime(Options{Mode: core.RunModeLive, Env: core.RunEnvDryRun, HubOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := biz.NewHubOrderMgr(rt.BizDeps(), bridge, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.CleanUp() })
	rt.Trading.SetOrderManager("default", manager)
	return &portFixture{local: port, hubTSFixture: &hubTSFixture{owner: owner, rt: rt, client: port.HubClient, bridge: bridge, manager: manager, hub: hub}}
}

func (f *portFixture) countRefusals() *atomic.Int32 {
	if f.local == nil {
		return countOwnerRefusals(f.hubTSFixture)
	}
	var n atomic.Int32
	f.local.SetErrorObserver(func(string, error) { n.Add(1) })
	return &n
}

func forBothPorts(t *testing.T, run func(t *testing.T, f *portFixture)) {
	for _, local := range []bool{false, true} {
		name := map[bool]string{false: "socket", true: "local"}[local]
		t.Run(name, func(t *testing.T) { run(t, newPortFixture(t, local)) })
	}
}

func TestHubPortParityEntryExitRestart(t *testing.T) {
	forBothPorts(t, func(t *testing.T, f *portFixture) {
		symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
		req := &strat.EnterReq{CommandID: "open", Tag: "remote"}
		od, err := f.manager.EnterOrder(symbol, "ws", req)
		if err != nil || od == nil {
			t.Fatal(od, err)
		}
		if od.Enter.Amount != 1 || od.Enter.Filled != 1 || od.Enter.Average != 100 {
			t.Fatalf("entry projection: %+v", od.Enter)
		}
		replay, e := f.manager.EnterOrder(symbol, "ws", req)
		if e != nil || replay.ID != od.ID || f.owner.adapter.Metrics().Fills != 1 {
			t.Fatal("entry retry was not idempotent", replay, e)
		}
		if od, e = f.manager.ExitOrder(od, &strat.ExitReq{CommandID: "close", Tag: "done"}); e != nil {
			t.Fatal(e)
		}
		if od.Status != ormo.InOutStatusFullExit || od.Exit == nil || od.Exit.Filled != 1 || od.Exit.Average != 100 {
			t.Fatalf("exit projection: %+v", od)
		}
		if e = f.manager.CleanUp(); e != nil {
			t.Fatal(e)
		}
		restarted, e2 := biz.NewHubOrderMgr(f.rt.BizDeps(), f.bridge, nil)
		if e2 != nil {
			t.Fatal(e2)
		}
		defer restarted.CleanUp()
		restored, e := restarted.EnterOrder(symbol, "ws", req)
		if e != nil || restored.ID != od.ID || f.owner.adapter.Metrics().Fills != 2 {
			t.Fatal("restart lost frozen command identity", restored, e)
		}
	})
}

func TestHubPortParityLimitFillAndEdit(t *testing.T) {
	forBothPorts(t, func(t *testing.T, f *portFixture) {
		symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
		od, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "lim", Tag: "lim", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
		if err != nil || od == nil || od.Enter.Filled != 0 {
			t.Fatalf("resting limit: %+v %v", od, err)
		}
		hubTSObserve(t, f.hubTSFixture, 90)
		got := hubTSOrder(t, f.hubTSFixture, od.ID)
		if got.Enter.Filled != 1 || !decimal.NewFromFloat(got.Enter.Average).Equal(decimal.NewFromInt(90)) {
			t.Fatalf("limit fill: %+v", got.Enter)
		}
	})
}

func TestHubPortParityRejectionsDropAndRestart(t *testing.T) {
	t.Run("off tick", func(t *testing.T) {
		forBothPorts(t, func(t *testing.T, f *portFixture) {
			instrument := f.owner.bridge.Instruments["BTC"]
			instrument.PriceTick = decimal.RequireFromString("0.1")
			f.owner.bridge.Instruments["BTC"] = instrument
			refusals := f.countRefusals()
			symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
			req := &strat.EnterReq{CommandID: "off-tick", Tag: "tick", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 110.5}
			assertDroppedCleanly(t, f.hubTSFixture, symbol, req)
			if refusals.Load() != 1 {
				t.Fatalf("owner refusals: %d", refusals.Load())
			}
			assertSurvivesAndRestarts(t, f.hubTSFixture, symbol, req, refusals)
		})
	})
	t.Run("risk limit", func(t *testing.T) {
		forBothPorts(t, func(t *testing.T, f *portFixture) {
			refusals := f.countRefusals()
			symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
			first, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "big-1", Tag: "big", Amount: 9})
			if err != nil || first == nil {
				t.Fatalf("first entry: %v, %v", first, err)
			}
			req := &strat.EnterReq{CommandID: "big-2", Tag: "big", Amount: 9}
			assertDroppedCleanly(t, f.hubTSFixture, symbol, req)
			if refusals.Load() != 1 {
				t.Fatalf("owner refusals: %d", refusals.Load())
			}
			if _, e := f.manager.ExitOrder(first, &strat.ExitReq{CommandID: "close-1", Tag: "done"}); e != nil {
				t.Fatal(e)
			}
			assertSurvivesAndRestarts(t, f.hubTSFixture, symbol, req, refusals)
		})
	})
}

func TestHubPortParityVenueMinimumDrop(t *testing.T) {
	forBothPorts(t, func(t *testing.T, f *portFixture) {
		instrument := f.owner.bridge.Instruments["BTC"]
		instrument.MinSteps = 10
		f.owner.bridge.Instruments["BTC"] = instrument
		symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
		fills := f.owner.adapter.Metrics().Fills
		od, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "tiny", Tag: "tiny", Amount: 0.5})
		if err != nil || od != nil || f.owner.adapter.Metrics().Fills != fills || f.manager.LastError() != nil {
			t.Fatalf("below-minimum drop: %v %v %v", od, err, f.manager.LastError())
		}
		good, err := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: "ok", Tag: "ok", Amount: 1})
		if err != nil || good == nil || good.Enter.Filled != 1 {
			t.Fatalf("entry after drop: %v %v", good, err)
		}
	})
}

func TestHubPortParityUnclassifiedRefusalFailsClosed(t *testing.T) {
	forBothPorts(t, func(t *testing.T, f *portFixture) {
		symbol := &orm.ExSymbol{ID: 1, Symbol: "BTC"}
		lease, err := f.client.WriterLease(context.Background(), f.owner.key, "ts")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.client.ClaimStrategyWriter(context.Background(), f.owner.key, execution.StrategyWriterClaim{Strategy: "ts", WriterID: "usurper", ExpectedEpoch: lease.Epoch}); err != nil {
			t.Fatal(err)
		}
		fills := f.owner.adapter.Metrics().Fills
		for _, id := range []string{"stale", "stale", "next"} {
			if od, e := f.manager.EnterOrder(symbol, "ws", &strat.EnterReq{CommandID: id, Tag: id, Amount: 1}); e == nil || od != nil {
				t.Fatalf("%s: unclassified refusal must be an error, got %v, %v", id, od, e)
			}
		}
		if f.owner.adapter.Metrics().Fills != fills {
			t.Fatal("refused command reached the venue")
		}
	})
}

func TestHubPortRejectsTypedNilClient(t *testing.T) {
	var nilPort *execution.LocalHubPort
	var nilClient *execution.HubClient
	for _, c := range []execution.HubPort{nil, nilPort, nilClient} {
		cfg := &biz.HubOrderBridgeConfig{Client: c, Account: execution.AccountKey{}, WriterID: "w", StatePath: "/tmp/x.json"}
		if cfg.Validate() == nil {
			t.Fatalf("nil client %T accepted", c)
		}
	}
}
