package execution

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/shopspring/decimal"
)

func bootstrapService(t *testing.T) (*SharedAccountBorrow, *SharedAccount) {
	t.Helper()
	key := AccountKey{"bootstrap-session", "bootstrap-account", "USDT"}
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewPaperAdapter(intentPrice("1000"), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewSharedAccount(owner, SharedExecutionOptions{StorePath: filepath.Join(t.TempDir(), "ledger.db"), SenderLeaseDir: t.TempDir(), Adapter: adapter, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	borrow := service.Borrow()
	t.Cleanup(func() { borrow.Release(); _ = service.Close(); registry.Close() })
	return borrow, service
}

func TestBootstrapCapitalAllocatesActualCashAndIsIdempotent(t *testing.T) {
	borrow, service := bootstrapService(t)
	capital := map[StrategyID]decimal.Decimal{"s1": decimal.NewFromInt(300), "s2": decimal.NewFromInt(200)}
	if err := borrow.BootstrapCapital(context.Background(), capital, 10); err != nil {
		t.Fatal(err)
	}
	var snapshot AccountSnapshot
	if err := borrow.WithState(func(s *SharedAccount) error {
		var err error
		snapshot, err = s.Store().Snapshot(context.Background())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !snapshot.AccountSettledCash.Equal(decimal.NewFromInt(1000)) || !snapshot.SyntheticStrategyCash["s1"].Equal(decimal.NewFromInt(300)) || !snapshot.SyntheticStrategyCash["s2"].Equal(decimal.NewFromInt(200)) {
		t.Fatalf("unexpected bootstrap snapshot: %+v", snapshot)
	}
	if err := borrow.BootstrapCapital(context.Background(), map[StrategyID]decimal.Decimal{"s1": decimal.NewFromInt(1)}, 20); err != nil {
		t.Fatal(err)
	}
	var after AccountSnapshot
	if err := borrow.WithState(func(s *SharedAccount) error {
		var err error
		after, err = s.Store().Snapshot(context.Background())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !after.SyntheticStrategyCash["s1"].Equal(decimal.NewFromInt(300)) {
		t.Fatalf("idempotent bootstrap changed capital: %+v", after)
	}
	_ = service
}

func TestBootstrapCapitalRejectsOverBudget(t *testing.T) {
	borrow, _ := bootstrapService(t)
	if err := borrow.BootstrapCapital(context.Background(), map[StrategyID]decimal.Decimal{"s1": decimal.NewFromInt(1001)}, 1); err == nil {
		t.Fatal("expected over-budget rejection")
	}
	var snapshot AccountSnapshot
	if err := borrow.WithState(func(s *SharedAccount) error {
		var err error
		snapshot, err = s.Store().Snapshot(context.Background())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if snapshot.Checkpoint != 0 {
		t.Fatalf("rejected bootstrap advanced checkpoint: %d", snapshot.Checkpoint)
	}
}

type bootstrapSnapshotAdapter struct {
	ExecutionAdapter
	venue VenueSnapshot
}

func (a *bootstrapSnapshotAdapter) Snapshot(context.Context) (VenueSnapshot, error) {
	return a.venue, nil
}

func TestBootstrapCapitalRejectsExistingExposure(t *testing.T) {
	key := AccountKey{"bootstrap-exposure", "account", "USDT"}
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewPaperAdapter(intentPrice("1000"), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &bootstrapSnapshotAdapter{ExecutionAdapter: base, venue: VenueSnapshot{Cash: "1000", Positions: map[string]int64{"BTC": 1}}}
	service, err := NewSharedAccount(owner, SharedExecutionOptions{StorePath: filepath.Join(t.TempDir(), "ledger.db"), SenderLeaseDir: t.TempDir(), Adapter: adapter, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	borrow := service.Borrow()
	defer borrow.Release()
	defer service.Close()
	defer registry.Close()
	if err := borrow.BootstrapCapital(context.Background(), map[StrategyID]decimal.Decimal{"s": decimal.NewFromInt(1)}, 1); err == nil {
		t.Fatal("expected exposure rejection")
	}
	var snap AccountSnapshot
	if err := borrow.WithState(func(s *SharedAccount) error {
		var e error
		snap, e = s.Store().Snapshot(context.Background())
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if snap.Checkpoint != 0 {
		t.Fatalf("rejected exposure advanced checkpoint: %d", snap.Checkpoint)
	}
}
