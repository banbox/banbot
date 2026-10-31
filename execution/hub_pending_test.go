package execution

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestHubPendingRPCIsolationReplayAndExecutionAfterDisconnect(t *testing.T) {
	b, venue := strategyService(t, true)
	market := b.service.markets["BTC"]
	var crossed atomic.Bool
	market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		price := "100"
		if crossed.Load() {
			price = "89"
		}
		return VisibleQuote{Bid: intentPrice(price), Ask: intentPrice(price), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000, Bar: 1}, nil
	}
	b.service.markets["BTC"] = market
	hub, _ := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{Account: b.service, Shutdown: func() error { return nil }}, nil
	})
	path := hubTestSocket(t)
	server, err := ListenHubUnix(path, hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := NewHubClient(path)
	ctx := context.Background()
	key := b.service.AccountKey()
	if err := client.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	command := pendingCreate("create-a", "a")
	for _, strategy := range []StrategyID{"a", "b"} {
		if _, err := client.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: strategy, WriterID: "session-" + string(strategy)}); err != nil {
			t.Fatal(err)
		}
	}
	command.StrategyEpoch, command.StrategySequence = 1, 1
	if revision, err := client.ApplyPendingCommand(ctx, key, command); err != nil || revision != 1 {
		t.Fatal(revision, err)
	}
	other := pendingCreate("create-b", "b")
	other.StrategyEpoch, other.StrategySequence = 1, 1
	if _, err := client.ApplyPendingCommand(ctx, key, other); err != nil {
		t.Fatal(err)
	}
	client.Close()
	crossed.Store(true)
	// The host, not a robot callback, advances accepted pending orders.
	if err := b.AdvancePendingExecution(ctx, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := b.Snapshot(ctx)
	if err != nil || len(snapshot.Lots) != 2 {
		t.Fatal("disconnected robot's orders did not fill", snapshot.Lots, err)
	}
	restarted := NewHubClient(path)
	defer restarted.Close()
	metrics := venue.Metrics()
	if revision, err := restarted.ApplyPendingCommand(ctx, key, command); err != nil || revision != 1 {
		t.Fatal(revision, err)
	}
	if got := venue.Metrics(); got != metrics {
		t.Fatal("lost ACK retry sent again", metrics, got)
	}
	book, err := restarted.PendingOrders(ctx, key)
	if err != nil || len(book) != 2 {
		t.Fatal(book, err)
	}
	command.Create.Conditions.Limit = intentPrice("80")
	if _, err := restarted.ApplyPendingCommand(ctx, key, command); err == nil {
		t.Fatal("conflicting retry crossed RPC")
	}
	exit := PendingCommand{ID: "exit-a", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingSetExit, Exit: &PendingExitSpec{}}
	exit.StrategyEpoch, exit.StrategySequence = 1, 2
	if revision, err := restarted.ApplyPendingCommand(ctx, key, exit); err != nil || revision != 2 {
		t.Fatal(revision, err)
	}
	metrics = venue.Metrics()
	if revision, err := restarted.ApplyPendingCommand(ctx, key, exit); err != nil || revision != 2 {
		t.Fatal(revision, err)
	}
	if venue.Metrics() != metrics {
		t.Fatal("exit retry sent again")
	}
	snapshot, err = restarted.Snapshot(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var otherHeld int64
	for _, lot := range snapshot.Lots {
		if lot.Strategy == "a" && lot.SignedSteps != 0 {
			t.Fatal("remote exit failed", snapshot.Lots)
		}
		if lot.Strategy == "b" {
			otherHeld += lot.SignedSteps
		}
	}
	if otherHeld != 10 {
		t.Fatal("remote exit changed other strategy", snapshot.Lots)
	}
}
