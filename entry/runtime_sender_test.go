package entry

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

type senderEntryExchange struct {
	banexg.BanExchange
	calls            atomic.Int32
	closed           atomic.Int32
	started, release chan struct{}
}

func (*senderEntryExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "senderentry", MarketType: banexg.MarketLinear}
}
func (*senderEntryExchange) GetCurMarkets() banexg.MarketMap {
	return banexg.MarketMap{"BTC/USDT": {Settle: "USDT"}}
}
func (e *senderEntryExchange) CreateOrder(_, _, _ string, _, _ float64, p map[string]any) (*banexg.Order, *errs.Error) {
	e.calls.Add(1)
	if e.started != nil {
		close(e.started)
		<-e.release
	}
	return &banexg.Order{ID: "ack"}, nil
}
func (e *senderEntryExchange) Close() *errs.Error { e.closed.Add(1); return nil }

func senderEntryFixture(t *testing.T, dir, env string, raw *senderEntryExchange) (*explicitEntrySession, *config.Snapshot) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	session := &explicitEntrySession{process: runtime.NewProcess(), ctx: ctx, cancel: cancel, exchange: raw}
	t.Cleanup(session.close)
	snapshot := config.NewSnapshotWithDirs(&config.Config{Env: env, MarketType: banexg.MarketLinear, Exchange: &config.ExchangeConfig{Name: "senderentry"}, Accounts: map[string]*config.AccountConfig{"trade": {}, "observe": {NoTrade: true}}, StakeCurrency: []string{"USDT"}}, dir, "", nil)
	return session, snapshot
}
func senderEntryKey() execution.AccountKey {
	return execution.AccountKey{VenueSessionIdentity: "senderentry:" + core.RunEnvProd, Account: "trade", SettlementDomain: "USDT"}
}

func TestEntryProductionConstructorFencesPhysicalSender(t *testing.T) {
	for _, mode := range []string{core.RunModeLive, core.RunModeOther} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			raw := &senderEntryExchange{}
			session, snapshot := senderEntryFixture(t, dir, core.RunEnvProd, raw)
			rt, err := session.newStorageRuntime(snapshot, mode, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !execution.IsLegacyExchange(rt.Exchange) || rt.BizDeps().Exchange != rt.Exchange {
				t.Fatal("entry passed an unfenced exchange")
			}
			if _, err := rt.Exchange.CreateOrder("BTC", "market", "buy", 1, 0, map[string]any{banexg.ParamAccount: "observe"}); err == nil {
				t.Fatal("no-trade account mutated venue")
			}
			key := senderEntryKey()
			key.SettlementDomain = "USD"
			if store, err := execution.OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "other.db"), key, filepath.Join(dir, "execution", "leases")); err == nil {
				store.Close()
				t.Fatal("shared sender bypassed actual entry lease")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEntrySenderLeaseChild$")
			child.Env = append(os.Environ(), "BAN_ENTRY_LEASE_CHILD=1", "BAN_ENTRY_LEASE_DIR="+filepath.Join(dir, "execution", "leases"), "BAN_ENTRY_LEASE_DB="+filepath.Join(t.TempDir(), "child.db"), "TMP="+t.TempDir())
			if output, err := child.CombinedOutput(); err != nil {
				t.Fatal(err, string(output))
			}
			rt.Close()
			rt.Join()
			if raw.closed.Load() != 0 {
				t.Fatal("runtime closed process SDK")
			}
			if store, err := execution.OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "retained.db"), key, filepath.Join(dir, "execution", "leases")); err == nil {
				store.Close()
				t.Fatal("borrower close released process sender")
			}
			session.process.Close()
			store, openErr := execution.OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "joined.db"), key, filepath.Join(dir, "execution", "leases"))
			if openErr != nil {
				t.Fatal("process did not release joined sender", openErr)
			}
			store.Close()
		})
	}
}
func TestEntrySenderLeaseChild(t *testing.T) {
	if os.Getenv("BAN_ENTRY_LEASE_CHILD") != "1" {
		return
	}
	key := senderEntryKey()
	key.SettlementDomain = "USD"
	if store, err := execution.OpenStoreWithLeaseDir(os.Getenv("BAN_ENTRY_LEASE_DB"), key, os.Getenv("BAN_ENTRY_LEASE_DIR")); err == nil {
		store.Close()
		t.Fatal("another process bypassed physical account lease")
	}
}

func TestEntryProductionConstructorRejectsExistingSharedSender(t *testing.T) {
	dir := t.TempDir()
	session, snapshot := senderEntryFixture(t, dir, core.RunEnvProd, &senderEntryExchange{})
	paper, err := execution.NewPaperAdapter(decimal.NewFromInt(100), decimal.NewFromInt(0), decimal.NewFromInt(0))
	if err != nil {
		t.Fatal(err)
	}
	_, err = session.process.BorrowAccount(senderEntryKey(), execution.SharedExecutionOptions{StorePath: filepath.Join(dir, "shared.db"), SenderLeaseDir: filepath.Join(dir, "execution", "leases"), Adapter: paper, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.newStorageRuntime(snapshot, core.RunModeLive, 0); err == nil {
		t.Fatal("legacy entry admitted beside shared sender")
	}
}

func TestEntrySenderRuntimeCloseWaitsActualSDKCall(t *testing.T) {
	dir := t.TempDir()
	raw := &senderEntryExchange{started: make(chan struct{}), release: make(chan struct{})}
	session, snapshot := senderEntryFixture(t, dir, core.RunEnvProd, raw)
	rt, err := session.newStorageRuntime(snapshot, core.RunModeLive, 0)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan *errs.Error, 1)
	go func() { _, err := rt.Exchange.CreateOrder("BTC", "market", "buy", 1, 0, nil); result <- err }()
	<-raw.started
	rt.Stop()
	if _, err := rt.Exchange.CreateOrder("BTC", "market", "buy", 1, 0, nil); err == nil {
		t.Fatal("stopped runtime admitted another mutation")
	}
	done := make(chan struct{})
	go func() { rt.Close(); rt.Join(); close(done) }()
	select {
	case <-done:
		t.Fatal("runtime reset before synchronous SDK returned")
	case <-time.After(20 * time.Millisecond):
	}
	close(raw.release)
	if err := <-result; err != nil {
		t.Fatal("late cancellation discarded venue acknowledgment", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runtime did not join call")
	}
	if raw.calls.Load() != 1 || raw.closed.Load() != 0 {
		t.Fatal("extra write or premature SDK close")
	}
}

func TestEntryHistoricalDataAndDryRunDoNotLeaseSender(t *testing.T) {
	for _, item := range []struct{ env, mode string }{{core.RunEnvProd, core.RunModeBackTest}, {core.RunEnvProd, core.RunModeData}, {core.RunEnvDryRun, core.RunModeLive}} {
		t.Run(item.env+"-"+item.mode, func(t *testing.T) {
			dir := t.TempDir()
			session, snapshot := senderEntryFixture(t, dir, item.env, &senderEntryExchange{})
			rt, err := session.newStorageRuntime(snapshot, item.mode, 0)
			if err != nil {
				t.Fatal(err)
			}
			if execution.IsLegacyExchange(rt.Exchange) {
				t.Fatal("nonproduction sender fenced")
			}
			if _, err := os.Stat(filepath.Join(dir, "execution", "leases")); !os.IsNotExist(err) {
				t.Fatal("read/paper path acquired lease", err)
			}
		})
	}
}
