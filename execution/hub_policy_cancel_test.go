package execution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func policyCancelHub(t *testing.T, memory bool) (*Hub, *SharedAccountBorrow, StrategyWriterLease) {
	t.Helper()
	account, _ := strategyService(t, memory)
	for id, market := range account.service.markets {
		market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
			return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000, Bar: now}, nil
		}
		account.service.markets[id] = market
	}
	h, err := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{Account: account.service, Shutdown: func() error { return nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if err := h.Register(context.Background(), HubAccountRegistration{Account: account.AccountKey(), Env: "dry_run", Configuration: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	lease, err := h.ClaimStrategyWriter(context.Background(), account.AccountKey(), StrategyWriterClaim{Strategy: "a", WriterID: "policy-cancel-test"})
	if err != nil {
		t.Fatal(err)
	}
	return h, account, lease
}

func TestHubPolicyCancellationLostACKDoesNotCancelNewGeneration(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(map[bool]string{false: "SQLite", true: "Memory"}[memory], func(t *testing.T) {
			ctx := context.Background()
			h, account, lease := policyCancelHub(t, memory)
			command := HubPolicyEntriesCancel{ID: "cancel-entries", Strategy: "a", StrategyEpoch: lease.Epoch, StrategySequence: 1, Instruments: []string{"BTC"}}
			if err := h.CancelPolicyEntries(ctx, account.AccountKey(), command); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UnixMilli()
			request := strategyRequest("a", "new-generation", now, StrategyTargetsPatch, ExecutableTarget{Lot: "new", SignedSteps: 2})
			request.ExpiresMS = now + 60000
			request.StrategyEpoch, request.StrategySequence = lease.Epoch, 2
			if err := h.RebalanceStrategy(ctx, account.AccountKey(), request, now); err != nil {
				t.Fatal(err)
			}
			before, _ := account.Snapshot(ctx)
			if err := h.CancelPolicyEntries(ctx, account.AccountKey(), command); err != nil {
				t.Fatal("completed retry did not replay original acceptance", err)
			}
			after, _ := account.Snapshot(ctx)
			if before.Checkpoint != after.Checkpoint || len(after.Lots) != 1 || after.Lots[0].SignedSteps != 2 {
				t.Fatalf("lost ACK retry changed a later target: %+v", after)
			}
			command.ID = "late-first-arrival"
			if err := h.CancelPolicyEntries(ctx, account.AccountKey(), command); err == nil {
				t.Fatal("late cancellation bypassed shared sequence")
			}
			command.ID, command.Instruments = "cancel-entries", []string{"other"}
			if err := h.CancelPolicyEntries(ctx, account.AccountKey(), command); err == nil {
				t.Fatal("different payload reused cancellation identity")
			}
		})
	}
}

func TestHubPolicyCancellationInterruptedOperationBlocksNewWriterCommands(t *testing.T) {
	ctx := context.Background()
	h, account, lease := policyCancelHub(t, false)
	now := time.Now().UnixMilli()
	request := strategyRequest("a", "initial", now, StrategyTargetsPatch, ExecutableTarget{Lot: "held", SignedSteps: 2})
	request.ExpiresMS = now + 60000
	request.StrategyEpoch, request.StrategySequence = lease.Epoch, 1
	if err := h.RebalanceStrategy(ctx, account.AccountKey(), request, now); err != nil {
		t.Fatal(err)
	}
	quoteFailure := errors.New("position cannot be safely marked")
	for id, market := range account.service.markets {
		market.quote = func(context.Context, string, int64) (VisibleQuote, error) { return VisibleQuote{}, quoteFailure }
		account.service.markets[id] = market
	}
	command := HubPolicyEntriesCancel{ID: "interrupted", Strategy: "a", StrategyEpoch: lease.Epoch, StrategySequence: 2, Instruments: []string{"BTC"}}
	if err := h.CancelPolicyEntries(ctx, account.AccountKey(), command); !errors.Is(err, ErrPolicyCancellationRecoveryRequired) || !errors.Is(err, quoteFailure) {
		t.Fatal("interrupted accepted action was not fenced", err)
	}
	state, err := account.service.store.strategyWriter(ctx, "a")
	if err != nil || state.Sequence != 2 {
		t.Fatal("accepted cancellation did not persist sequence before work", state, err)
	}
	if err := h.CancelPolicyEntries(ctx, account.AccountKey(), command); !errors.Is(err, ErrPolicyCancellationRecoveryRequired) {
		t.Fatal("interrupted retry blindly reran cancellation", err)
	}
	snapshot, err := account.Snapshot(ctx)
	if err != nil || !snapshot.RiskFrozen {
		t.Fatal("interrupted operation did not freeze account", snapshot, err)
	}
	// Even replacing the writer cannot bypass this durable incomplete command.
	next, err := h.ClaimStrategyWriter(ctx, account.AccountKey(), StrategyWriterClaim{Strategy: "a", WriterID: "replacement", ExpectedEpoch: lease.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	err = account.WithState(func(service *SharedAccount) error {
		_, err := service.strategyOrderingCheckpoints(ctx, []StrategyRebalance{{Strategy: "a", StrategyEpoch: next.Epoch, StrategySequence: 1}})
		return err
	})
	if !errors.Is(err, ErrPolicyCancellationRecoveryRequired) {
		t.Fatal("new writer bypassed unfinished cancellation", err)
	}
	store := account.service.store
	options := account.service.opts
	if err := account.service.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStoreWithLeaseDir(options.StorePath, store.Account(), options.SenderLeaseDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.requireNoPolicyCancellation(ctx, "a"); !errors.Is(err, ErrPolicyCancellationRecoveryRequired) {
		t.Fatal("restart lost incomplete-operation barrier", err)
	}
}

func TestHubPolicyCancellationSettlesSharedOrderAndRetainsOtherContributor(t *testing.T) {
	ctx := context.Background()
	h, account, lease := policyCancelHub(t, true)
	now := time.Now().UnixMilli()
	updates := []StrategyRebalance{
		strategyRequest("a", "shared-policy-entry", now, StrategyTargetsPatch, ExecutableTarget{Lot: "factor", SignedSteps: 3}),
		strategyRequest("b", "shared-policy-entry", now, StrategyTargetsPatch, ExecutableTarget{Lot: "other", SignedSteps: 2}),
	}
	for n := range updates {
		update := &updates[n]
		update.ExpiresMS = now + 60000
		input := &update.Requests[0]
		input.Quote = VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000, Bar: now}
		constraint := contributor(account.service.store, update.Strategy, input.Targets[0].Lot, Buy, input.Targets[0].SignedSteps, "99", "0")
		constraint.Conditions.PostOnly = true
		constraint.Conditions.ExpiresAtMS = now + 60000
		input.IntentConstraints = []EligibleIntent{constraint}
	}
	if err := account.WithState(func(service *SharedAccount) error {
		prepared, err := service.PrepareStrategiesWithCheckpoints(updates, ctx, nil)
		if err != nil {
			return err
		}
		return service.SendPrepared(prepared, now, ctx)
	}); err != nil {
		t.Fatal(err)
	}
	before, err := account.Snapshot(ctx)
	if err != nil || len(before.Orders) != 1 || len(before.Orders[0].Intent.Allocations) != 2 {
		t.Fatal("fixture is not a shared resting order", before, err)
	}
	command := HubPolicyEntriesCancel{ID: "settle-shared", Strategy: "a", StrategyEpoch: lease.Epoch, StrategySequence: 1, Instruments: []string{"BTC"}}
	if err := h.CancelPolicyEntries(ctx, account.AccountKey(), command); err != nil {
		t.Fatal(err)
	}
	after, err := account.Snapshot(ctx)
	if err != nil || len(after.Orders) != 1 || len(after.Orders[0].Intent.Allocations) != 1 || after.Orders[0].Intent.Allocations[0].Strategy != "b" {
		t.Fatal("cancellation lost the other contributor's pending allocation", after, err)
	}
}
