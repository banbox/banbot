package execution

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestTimeSeriesProjectionLocalRemoteAtomicRecordsAndReplay(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	if _, err := b.ApplyPendingCommand(ctx, tsCreate("ts-create", "ts-lot", 1, true), 12); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ApplyPendingCommand(ctx, pendingCreate("factor-create", "b"), 13); err != nil {
		t.Fatal(err)
	}
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
	defer client.Close()
	key := b.AccountKey()
	if err := client.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	request := TimeSeriesProjectionRequest{Name: "ts-projection", Limit: 2}
	local, err := b.TimeSeriesProjection(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	port := HubTimeSeriesProjection{Client: client, Account: key}
	remote, err := port.TimeSeriesProjection(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(local, remote) {
		t.Fatalf("remote projection differs\nlocal=%+v\nremote=%+v", local, remote)
	}
	if len(remote.Records) != 1 || remote.Records[0].Metadata.OrderID != 1 || remote.Records[0].Lot != "ts-lot" {
		t.Fatal("metadata missing or factor mislabeled", remote.Records)
	}
	if len(remote.Events) != 2 || remote.Cursor != 0 {
		t.Fatal(remote)
	}
	metricsBefore, _ := b.Snapshot(ctx)
	if err := port.AdvanceProjection(ctx, request.Name, remote.Events[0].Checkpoint); err != nil {
		t.Fatal(err)
	}
	replay, err := port.TimeSeriesProjection(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Cursor != remote.Events[0].Checkpoint || len(replay.Events) == 0 || replay.Events[0].Checkpoint <= replay.Cursor {
		t.Fatal("incorrect resumed page", replay)
	}
	metricsAfter, _ := b.Snapshot(ctx)
	if !reflect.DeepEqual(metricsBefore, metricsAfter) {
		t.Fatal("projection changed trading state")
	}
}

func TestTimeSeriesProjectionIncludesExplicitTerminalMetadata(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	if _, err := b.ApplyPendingCommand(ctx, tsCreate("ts-create", "closed", 7, true), 12); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ApplyTimeSeriesExit(ctx, TimeSeriesExitCommand{ID: "close", Strategy: "a", Force: true}, 13); err != nil {
		t.Fatal(err)
	}
	snapshot, err := b.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.AdvanceProjection(ctx, "terminal", snapshot.Checkpoint); err != nil {
		t.Fatal(err)
	}
	page, err := b.TimeSeriesProjection(ctx, TimeSeriesProjectionRequest{Name: "terminal", Limit: 10, Lots: []TimeSeriesLotKey{{Strategy: "a", Lot: "closed"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 0 || len(page.Records) != 1 || page.Records[0].Metadata.OrderID != 7 || page.Records[0].Metadata.ExitTag != "" {
		t.Fatal(page)
	}
}
