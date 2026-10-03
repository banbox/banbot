package execution

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

type fundingRecoverySession struct {
	*fakeBanexgSession
	fetch func(context.Context, string, string, int64, int64) ([]banexg.FundingCash, *errs.Error)
	calls int
}

func (e *fundingRecoverySession) FetchFundingCash(ctx context.Context, account, currency string, since, until int64) ([]banexg.FundingCash, *errs.Error) {
	e.calls++
	return e.fetch(ctx, account, currency, since, until)
}

func openFundingRecoveryAccount(t *testing.T, path string, exchange *fundingRecoverySession) (*SharedAccountBorrow, *BanexgAdapter, func()) {
	t.Helper()
	key := AccountKey{"funding-recovery-session", "funding-account", "USDT"}
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	proof := BanexgExecutionProof{Account: key, EvidenceID: "funding-recovery-proof", ContextBound: true, StableClientID: true, QueryClientID: true, CompleteCumulativeReports: true, CompleteAccountSnapshot: true, SettledCash: true, NetLinearPositions: true}
	adapter, err := NewBanexgAdapter(context.Background(), exchange, BanexgAdapterConfig{Account: key, Transport: &verifiedTestTransport{proof: proof}, Instruments: []BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: ledgerInstrument()}}})
	if err != nil {
		registry.Close()
		t.Fatal(err)
	}
	service, err := NewSharedAccount(owner, SharedExecutionOptions{StorePath: path, SenderLeaseDir: filepath.Join(filepath.Dir(path), "leases"), Adapter: adapter, AuthoritativeSnapshot: true})
	if err != nil {
		registry.Close()
		t.Fatal(err)
	}
	borrow := service.Borrow()
	closeAccount := func() {
		borrow.Release()
		if err := service.Close(); err != nil {
			t.Error(err)
		}
		registry.Close()
	}
	t.Cleanup(closeAccount)
	return borrow, adapter, closeAccount
}

func fundingRecoveryFixture(t *testing.T) (*SharedAccountBorrow, *BanexgAdapter, *fundingRecoverySession, string, int64, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.db")
	exchange := &fundingRecoverySession{fakeBanexgSession: &fakeBanexgSession{balances: &banexg.Balances{Total: map[string]float64{"USDT": 1000}}}}
	borrow, adapter, closeAccount := openFundingRecoveryAccount(t, path, exchange)
	floor := time.Now().UnixMilli() - 1000
	if err := borrow.BootstrapCapital(context.Background(), map[StrategyID]decimal.Decimal{"strategy": decimal.NewFromInt(100)}, floor); err != nil {
		t.Fatal(err)
	}
	return borrow, adapter, exchange, path, floor, closeAccount
}

func fundingRecoveryEvents(t *testing.T, borrow *SharedAccountBorrow) (AccountSnapshot, int, int64) {
	t.Helper()
	var snapshot AccountSnapshot
	count := 0
	watermark := int64(-1)
	err := borrow.WithState(func(s *SharedAccount) error {
		var err error
		snapshot, err = s.store.Snapshot(context.Background())
		if err != nil {
			return err
		}
		for cursor := int64(0); ; {
			page, err := s.store.EventsAfter(context.Background(), cursor, 512)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				return nil
			}
			for _, event := range page {
				if event.ID == "funding/recovery-1" {
					count++
				}
				if strings.HasPrefix(event.ID, "funding-watermark/") {
					var cash CashEvent
					if err := json.Unmarshal(event.Payload, &cash); err != nil {
						return err
					}
					watermark = max(watermark, cash.AtMS)
				}
			}
			cursor = page[len(page)-1].Checkpoint
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, count, watermark
}

func TestBanexgRecoverCashFetchFailureRetainsDurableWatermark(t *testing.T) {
	borrow, adapter, exchange, _, floor, _ := fundingRecoveryFixture(t)
	if err := borrow.CashEvent(CashEvent{ID: "funding-watermark/previous", Kind: Reconciliation, Postings: []CashPosting{{Amount: decimal.Zero}}, AtMS: floor + 1}); err != nil {
		t.Fatal(err)
	}
	before, _, watermark := fundingRecoveryEvents(t, borrow)
	exchange.fetch = func(ctx context.Context, account, currency string, since, until int64) ([]banexg.FundingCash, *errs.Error) {
		if account != "funding-account" || currency != "USDT" || since != floor || until < since {
			t.Fatal("funding recovery queried wrong account or overlap")
		}
		return nil, errs.NewMsg(errs.CodeNetFail, "funding connection interrupted")
	}
	if err := adapter.RecoverCash(context.Background(), borrow); err == nil {
		t.Fatal("funding fetch failure swallowed")
	}
	after, count, afterWatermark := fundingRecoveryEvents(t, borrow)
	if exchange.calls != 1 || after.Checkpoint != before.Checkpoint || afterWatermark != watermark || count != 0 || !after.AccountSettledCash.Equal(before.AccountSettledCash) {
		t.Fatalf("failed fetch advanced durable state: before=%+v after=%+v watermark=%d/%d", before, after, watermark, afterWatermark)
	}
}

func TestBanexgRecoverCashRestartAndRepeatedSettlementAreIdempotent(t *testing.T) {
	borrow, adapter, exchange, path, floor, closeAccount := fundingRecoveryFixture(t)
	row := banexg.FundingCash{ID: "funding/recovery-1", Symbol: "BTC/USDT:USDT", Currency: "USDT", Amount: "-0.125", Mark: "100", Rate: "0.001", AtMS: floor + 1}
	exchange.fetch = func(ctx context.Context, account, currency string, since, until int64) ([]banexg.FundingCash, *errs.Error) {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		if account != "funding-account" || currency != "USDT" || row.AtMS < since || row.AtMS > until {
			t.Fatal("restart did not preserve overlap/account identity")
		}
		return []banexg.FundingCash{row}, nil
	}
	for i := 0; i < 2; i++ {
		if err := adapter.RecoverCash(context.Background(), borrow); err != nil {
			t.Fatal(err)
		}
	}
	before, count, watermark := fundingRecoveryEvents(t, borrow)
	if count != 1 || watermark < floor || !before.AccountSettledCash.Equal(decimal.RequireFromString("999.875")) {
		t.Fatalf("funding was not committed exactly once: %+v count=%d watermark=%d", before, count, watermark)
	}
	closeAccount()
	borrow, adapter, _ = openFundingRecoveryAccount(t, path, exchange)
	if err := borrow.BootstrapCapital(context.Background(), map[StrategyID]decimal.Decimal{"strategy": decimal.NewFromInt(100)}, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := adapter.RecoverCash(context.Background(), borrow); err != nil {
		t.Fatal(err)
	}
	after, count, afterWatermark := fundingRecoveryEvents(t, borrow)
	if count != 1 || exchange.calls != 3 || afterWatermark < watermark || !after.AccountSettledCash.Equal(before.AccountSettledCash) || !after.SyntheticStrategyCash["strategy"].Equal(before.SyntheticStrategyCash["strategy"]) {
		t.Fatalf("restart duplicated cash or allocations: before=%+v after=%+v count=%d", before, after, count)
	}
}

func TestBanexgRecoverCashCanceledContextDoesNotRequestOrCommit(t *testing.T) {
	borrow, adapter, exchange, _, _, _ := fundingRecoveryFixture(t)
	exchange.fetch = func(context.Context, string, string, int64, int64) ([]banexg.FundingCash, *errs.Error) {
		t.Fatal("canceled recovery requested venue funding")
		return nil, nil
	}
	before, _, watermark := fundingRecoveryEvents(t, borrow)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := adapter.RecoverCash(ctx, borrow); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected caller cancellation, got %v", err)
	}
	after, count, afterWatermark := fundingRecoveryEvents(t, borrow)
	if exchange.calls != 0 || after.Checkpoint != before.Checkpoint || count != 0 || afterWatermark != watermark {
		t.Fatal("canceled recovery mutated durable state")
	}
}

func TestBanexgRecoverCashPartialCommitReplaysBeforeWatermark(t *testing.T) {
	borrow, adapter, exchange, _, floor, _ := fundingRecoveryFixture(t)
	row := banexg.FundingCash{ID: "funding/recovery-1", Symbol: "BTC/USDT:USDT", Currency: "USDT", Amount: "-0.125", Mark: "100", Rate: "0.001", AtMS: floor + 1}
	bad := row
	bad.ID, bad.Mark = "funding/recovery-2", "bad"
	exchange.fetch = func(context.Context, string, string, int64, int64) ([]banexg.FundingCash, *errs.Error) {
		return []banexg.FundingCash{row, bad}, nil
	}
	if err := adapter.RecoverCash(context.Background(), borrow); err == nil {
		t.Fatal("incomplete settlement page accepted")
	}
	partial, count, watermark := fundingRecoveryEvents(t, borrow)
	if count != 1 || watermark != -1 || !partial.AccountSettledCash.Equal(decimal.RequireFromString("999.875")) {
		t.Fatalf("partial page advanced watermark or lost first commit: %+v count=%d watermark=%d", partial, count, watermark)
	}
	exchange.fetch = func(context.Context, string, string, int64, int64) ([]banexg.FundingCash, *errs.Error) {
		return []banexg.FundingCash{row}, nil
	}
	if err := adapter.RecoverCash(context.Background(), borrow); err != nil {
		t.Fatal(err)
	}
	after, count, watermark := fundingRecoveryEvents(t, borrow)
	if count != 1 || watermark < floor || !after.AccountSettledCash.Equal(partial.AccountSettledCash) {
		t.Fatal("partial replay duplicated settlement or did not commit complete watermark")
	}
}
