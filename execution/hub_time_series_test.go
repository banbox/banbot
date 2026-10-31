package execution

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
)

func TestHubTimeSeriesAtomicExitProjectionAndReplay(t *testing.T) {
	b, venue := strategyService(t, true)
	market := b.service.markets["BTC"]
	market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000, Bar: 1}, nil
	}
	b.service.markets["BTC"] = market
	hub, err := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{Account: b.service, Shutdown: func() error { return nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	path := hubTestSocket(t)
	server, err := ListenHubUnix(path, hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx := context.Background()
	key := b.AccountKey()
	client := NewHubClient(path)
	defer client.Close()
	if err := client.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	for _, strategy := range []StrategyID{"a", "b"} {
		if _, err := client.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: strategy, WriterID: "session-" + string(strategy)}); err != nil {
			t.Fatal(err)
		}
	}
	for i, lot := range []VirtualLotID{"first", "second"} {
		create := tsCreate(string(lot), lot, int64(i+1), true)
		create.StrategyEpoch, create.StrategySequence = 1, uint64(i+1)
		if _, err := client.ApplyPendingCommand(ctx, key, create); err != nil {
			t.Fatal(err)
		}
	}
	other := pendingCreate("other", "b")
	other.StrategyEpoch, other.StrategySequence = 1, 1
	other.Create.Conditions = IntentConditions{}
	if _, err := client.ApplyPendingCommand(ctx, key, other); err != nil {
		t.Fatal(err)
	}
	request := TimeSeriesProjectionRequest{Name: "robot-ts", Limit: 10000}
	page, err := client.TimeSeriesProjection(ctx, key, request)
	if err != nil || len(page.Records) != 2 || page.Cursor != 0 || !page.Reconciled || len(page.Events) == 0 {
		t.Fatal(page, err)
	}
	for _, record := range page.Records {
		if record.Strategy != "a" || record.Metadata.StrategyName != "ts" || string(record.Metadata.Payload) != `{"tag":"source"}` {
			t.Fatal("metadata lost or other contributor misattributed", record)
		}
	}
	// Concurrent retransmission must consume one shared budget, not one per
	// connection. This also exercises lost-ACK replay through JSON transport.
	command := TimeSeriesExitCommand{ID: "exit-budget", Strategy: "a", Force: true, Amount: intentPrice("1.3"), ExitTag: "remote-close"}
	command.StrategyEpoch, command.StrategySequence = 1, 3
	var results [2][]TimeSeriesExitResult
	var failures [2]error
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], failures[i] = client.ApplyTimeSeriesExit(ctx, key, command)
		}(i)
	}
	wg.Wait()
	if failures[0] != nil || failures[1] != nil || len(results[0]) != 2 || !reflect.DeepEqual(results[0], results[1]) {
		t.Fatal(results, failures)
	}
	var reduced int64
	for _, result := range results[0] {
		reduced += result.ReducedSteps
	}
	if reduced != 13 {
		t.Fatal("budget not consumed exactly once", results)
	}
	metrics := venue.Metrics()
	client.Close()
	reconnected := NewHubClient(path)
	defer reconnected.Close()
	if replay, err := reconnected.ApplyTimeSeriesExit(ctx, key, command); err != nil || !reflect.DeepEqual(replay, results[0]) || metrics != venue.Metrics() {
		t.Fatal("reconnect resent accepted exit", replay, err)
	}
	snapshot, err := reconnected.Snapshot(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var held, otherHeld int64
	for _, lot := range snapshot.Lots {
		if lot.Strategy == "a" {
			held += lot.SignedSteps
		} else if lot.Strategy == "b" {
			otherHeld += lot.SignedSteps
		}
	}
	if held != 7 || otherHeld != 10 {
		t.Fatal("remote selector crossed strategy boundary", snapshot.Lots)
	}
	updated, err := reconnected.TimeSeriesProjection(ctx, key, request)
	if err != nil || updated.Cursor != 0 || updated.Snapshot.Checkpoint <= page.Snapshot.Checkpoint {
		t.Fatal("reading projection acknowledged or lost new events", updated, err)
	}
	for _, record := range updated.Records {
		if record.Metadata.ExitTag != "remote-close" {
			t.Fatal("exit metadata not committed with exit", record)
		}
	}
	var historicalMetadata int
	for _, event := range updated.Events {
		if event.Kind != "StrategyAccepted" {
			continue
		}
		var accepted StrategyAcceptedEvent
		if err := json.Unmarshal(event.Payload, &accepted); err != nil {
			t.Fatal(err)
		}
		if accepted.Strategy != "a" {
			continue
		}
		if accepted.Metadata == nil || accepted.Kind == EntryIntent && accepted.Metadata.ExitTag != "" || accepted.Kind == ExitIntent && accepted.Metadata.ExitTag != "remote-close" {
			t.Fatal("historical acceptance metadata overwritten by latest state", accepted)
		}
		historicalMetadata++
	}
	if historicalMetadata != 4 {
		t.Fatal("missing atomic TS acceptance facts", historicalMetadata)
	}
	through := updated.Events[len(updated.Events)-1].Checkpoint
	if err := reconnected.AdvanceProjection(ctx, key, request.Name, through); err != nil {
		t.Fatal(err)
	}
	updated, err = reconnected.TimeSeriesProjection(ctx, key, request)
	if err != nil || updated.Cursor != through || len(updated.Events) != 0 {
		t.Fatal("acknowledged events replayed", updated, err)
	}
	command.Amount = intentPrice("1.4")
	if _, err := reconnected.ApplyTimeSeriesExit(ctx, key, command); err == nil || metrics != venue.Metrics() {
		t.Fatal("conflicting exit retry accepted", err)
	}
}

func TestHubTimeSeriesRPCRejectsMissingPayloads(t *testing.T) {
	hub, _ := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		t.Fatal("malformed request must not initialize account")
		return HubAccountBinding{}, nil
	})
	path := hubTestSocket(t)
	server, err := ListenHubUnix(path, hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := NewHubClient(path)
	defer client.Close()
	for _, route := range []string{"ts-exit", "ts-projection"} {
		if _, err := client.call(context.Background(), route, hubRPCRequest{}); err == nil {
			t.Fatal("missing payload accepted", route)
		}
	}
}
