package entry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/runtime"
)

type factoryFailureLiveTransport struct {
	liveEntryTransport
	invoked atomic.Int32
}

func (p *factoryFailureLiveTransport) Invoke(ctx context.Context, call func() error) error {
	p.invoked.Add(1)
	return p.liveEntryTransport.Invoke(ctx, call)
}

func TestFactorLiveFactoryFailurePrecedesAllExecutionResources(t *testing.T) {
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	c.Mode, c.Chunks = runner.Trade, nil
	c.Execution.StorePath = filepath.Join(dir, "ledger", "live.db")
	c.Execution.SenderLeaseDir = filepath.Join(dir, "leases")
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.Builder = fmt.Sprintf("live-factory-failure-%d-v1", time.Now().UnixNano())
	want := errors.New("invalid factory training window")
	var constructed, verifiedFunding atomic.Int32
	if err := runner.RegisterPortfolioFactory(c.Manifest.Portfolio.Builder, func(runner.Config) (runner.PortfolioBuilder, error) {
		constructed.Add(1)
		return nil, want
	}); err != nil {
		t.Fatal(err)
	}
	if err := validateFactorLiveConfig(c); err != nil {
		t.Fatalf("legacy factory preflight constructed state: %v", err)
	}
	key := execution.AccountKey{VenueSessionIdentity: dir, Account: c.AccountID, SettlementDomain: c.Manifest.Currency}
	transport := &factoryFailureLiveTransport{liveEntryTransport: liveEntryTransport{key: key}}
	exchange := &liveEntryExchange{}
	binding := FactorLiveBinding{
		Account: key, Transport: transport, Symbols: map[int32]*orm.ExSymbol{},
		VerifyFunding: func(context.Context, string) (string, error) {
			verifiedFunding.Add(1)
			return "verified", nil
		},
		Record: func(*orm.DataSeries, int64) (factor.VersionRecord, error) { return factor.VersionRecord{}, nil },
	}
	for sid, symbol := range c.Snapshot.SIDMap {
		binding.Symbols[sid] = &orm.ExSymbol{ID: sid, Exchange: "entrytest", Market: "linear", Symbol: symbol}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &explicitEntrySession{process: runtime.NewProcess(), ctx: ctx, cancel: cancel, exchange: exchange}
	defer session.close()
	snapshot := config.NewSnapshotWithDirs(&config.Config{Exchange: &config.ExchangeConfig{Name: "entrytest"}, MarketType: "linear"}, dir, "", nil)
	if err := session.runFactorsLive(ctx, snapshot, []runner.Config{c}, binding, io.Discard); !errors.Is(err, want) {
		t.Fatalf("live entry lost constructor failure: %v", err)
	}
	if constructed.Load() != 1 || c.PortfolioBuilder != nil {
		t.Fatalf("factory construction count=%d or caller config mutated", constructed.Load())
	}
	for _, path := range []string{filepath.Dir(c.Execution.StorePath), c.Execution.SenderLeaseDir} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("factory failure created execution directory %s: %v", path, err)
		}
	}
	if transport.verified.Load() != 0 || transport.invoked.Load() != 0 || exchange.snapshots.Load() != 0 || verifiedFunding.Load() != 0 {
		t.Fatal("factory failure reached transport, recovery or funding verification")
	}
}
