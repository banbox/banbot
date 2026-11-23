package runtime

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func tsMetricsJob(rt *Runtime) *strat.StratJob {
	job := &strat.StratJob{Account: "default", Strat: &strat.TradeStrat{Name: "ts-metrics"}}
	job.BindRuntimeState(rt.Strategies, rt.Core, rt.Clock)
	return job
}

// Time-series Hub robots read account metrics through the cached reader.
func TestHubTSAccountMetricsCachedAndMatchFreshRead(t *testing.T) {
	f := newHubTSFixture(t, nil)
	if od, e := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "open", Tag: "x"}); e != nil || od == nil {
		t.Fatal(od, e)
	}
	deps := f.rt.BizDeps()
	reader := biz.BoundHubAccountReader(deps)
	if reader == nil || reader != biz.BoundHubAccountReader(deps) {
		t.Fatal("time-series Hub runtime has no stable cached reader")
	}
	job := tsMetricsJob(f.rt)
	// The manager's background poll may still apply the fill (it moves the
	// ledger cursor and legitimately refreshes the cache): wait for quiet.
	for quiet := 0; quiet < 5; {
		before := reader.RPCRounds()
		job.AccountEquity()
		if reader.RPCRounds() == before {
			quiet++
		} else {
			quiet = 0
			time.Sleep(50 * time.Millisecond)
		}
	}
	eq, av := job.AccountEquity(), job.AvailableBalance()
	if eq <= 0 || av >= eq {
		t.Fatalf("open position not reflected: equity=%v available=%v", eq, av)
	}
	// Same value an uncached Hub query produces.
	_, m, backed, remote := biz.ExecutionAccountBalance(deps, "default")
	if !backed || !remote || !m.Valid || m.Equity != eq || m.Available != av {
		t.Fatalf("cached read %+v differs from strategy view %v/%v", m, eq, av)
	}
	base := reader.RPCRounds()
	for i := 0; i < 200; i++ {
		job.AccountEquity()
		biz.ExecutionAccountBalance(deps, "default")
	}
	if n := reader.RPCRounds(); n != base {
		t.Fatalf("rounds under repeated reads = %d, want %d", n, base)
	}
	// A command of this robot expires the cache before the TTL.
	if od, e := f.manager.EnterOrder(&orm.ExSymbol{ID: 1, Symbol: "BTC"}, "ws", &strat.EnterReq{CommandID: "open2", Tag: "y"}); e != nil || od == nil {
		t.Fatal(od, e)
	}
	job.AccountEquity()
	if n := reader.RPCRounds(); n <= base {
		t.Fatalf("rounds after own order = %d, want > %d", n, base)
	}
	for quiet := 0; quiet < 5; {
		before := reader.RPCRounds()
		job.AccountEquity()
		if reader.RPCRounds() == before {
			quiet++
		} else {
			quiet = 0
			time.Sleep(50 * time.Millisecond)
		}
	}
	// After the TTL a new round happens even though nothing else changed.
	base = reader.RPCRounds()
	time.Sleep(1100 * time.Millisecond)
	job.AccountEquity()
	if n := reader.RPCRounds(); n != base+1 {
		t.Fatalf("rounds after TTL = %d, want %d", n, base+1)
	}
}

// A Hub that accepts connections but never answers must not block a read.
func TestHubTSAccountMetricsHungHubBounded(t *testing.T) {
	f := newHubTSFixture(t, nil)
	dir, err := os.MkdirTemp("", "hub-hung-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		for {
			c, e := ln.Accept()
			if e != nil {
				close(done)
				return
			}
			conns = append(conns, c) // never read, never answer
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		<-done
		for _, c := range conns {
			c.Close()
		}
	})
	hung := execution.NewHubClient(sock)
	t.Cleanup(func() { hung.Close() })
	bridge := &biz.HubOrderBridgeConfig{Client: hung, Account: f.owner.key, Bridge: *f.owner.bridge, WriterID: "hung", StatePath: filepath.Join(dir, "r.json")}
	rt2, err := f.owner.process.NewRuntime(Options{Mode: core.RunModeLive, Env: core.RunEnvDryRun, HubOrderBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { rt2.Close(); rt2.Join() }()
	deps := rt2.BizDeps()
	start := time.Now()
	_, m, _, _ := biz.ExecutionAccountBalance(deps, "default")
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("hung Hub blocked the read for %v", el)
	}
	if m.Valid {
		t.Fatalf("hung Hub produced valid metrics: %+v", m)
	}
	// The failure is cached for the TTL: no second 3s wait.
	start = time.Now()
	biz.ExecutionAccountBalance(deps, "default")
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("failed read was not cached: %v", el)
	}
	if n := biz.BoundHubAccountReader(deps).RPCRounds(); n != 1 {
		t.Fatalf("rounds = %d, want 1", n)
	}
}
