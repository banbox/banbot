package execution

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Exit from inside an admitted operation skips all Store/owner cleanup. This
// validates process-crash WAL recovery and OS lease release, not power-loss or
// real venue durability.
func TestExecutionAbruptProcessRecovery(t *testing.T) {
	for _, boundary := range []string{"sending", "fill-before-cursor"} {
		t.Run(boundary, func(t *testing.T) {
			dir := t.TempDir()
			path, leases := filepath.Join(dir, "execution.db"), filepath.Join(dir, "leases")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionAbruptProcessChild$")
			command.Env = append(os.Environ(), "BANBOT_EXEC_CRASH_BOUNDARY="+boundary, "BANBOT_EXEC_CRASH_DB="+path, "BANBOT_EXEC_CRASH_LEASES="+leases)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 42 {
				t.Fatalf("child did not reach abrupt boundary: %v %s", err, output)
			}
			store, err := OpenStoreWithLeaseDir(path, testIntent(Buy).Account, leases)
			if err != nil {
				t.Fatal("crashed process retained lease or lost WAL", err)
			}
			defer store.Close()
			registry := &AccountRegistry{}
			defer registry.Close()
			owner, err := registry.Acquire(store.Account())
			if err != nil {
				t.Fatal(err)
			}
			order, err := store.Order(context.Background(), "crash-order")
			if err != nil || order.Attempt != 1 || order.ClientID == "" {
				t.Fatal("committed send identity lost", order, err)
			}
			before, err := store.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
			adapter.query = func(_ context.Context, client, exchange string) (QueryResult, error) {
				if client != order.ClientID || exchange != "" && exchange != "exchange-crash-order" {
					t.Fatal("recovery replaced stable identity", client, exchange)
				}
				return QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "exchange-crash-order", Fills: []FillReport{{EventID: "recovery-cumulative", OrderID: order.Intent.ID, Steps: 10, Price: intentPrice("100"), Cost: intentPrice("100"), Fee: intentPrice("0.05"), Cumulative: true, AtMS: 12}}}}, nil
			}
			executor := executorFor(store, owner, adapter)
			if err := executor.Send(order.Intent.ID, 13); err == nil {
				t.Fatal("crashed transmission blindly resent")
			}
			if boundary == "sending" && order.State != OrderSending || boundary == "fill-before-cursor" && order.State != OrderFilled {
				t.Fatal("committed boundary lost", boundary, order.State)
			}
			if err := executor.Recover(order.Intent.ID); err != nil {
				t.Fatal(err)
			}
			complete, err := store.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := executor.Recover(order.Intent.ID); err != nil {
				t.Fatal(err)
			}
			after, err := store.Snapshot(context.Background())
			if err != nil || !after.AccountSettledCash.Equal(complete.AccountSettledCash) || !after.UnassignedCash.Equal(complete.UnassignedCash) {
				t.Fatal("repeated recovery changed settled cash", after, complete, err)
			}
			// A recovered fill can advance the typed attempt result from ACK to
			// Filled on its next query; subsequent identical queries are stable.
			if err := executor.Recover(order.Intent.ID); err != nil {
				t.Fatal(err)
			}
			stable, err := store.Snapshot(context.Background())
			if err != nil || stable.Checkpoint != after.Checkpoint {
				t.Fatal("identical recovery kept advancing committed highwater", stable.Checkpoint, after.Checkpoint, err)
			}
			current, err := store.Order(context.Background(), order.Intent.ID)
			if err != nil || current.State != OrderFilled || current.FilledSteps != 10 || !current.ReportedFee.Equal(intentPrice("0.05")) || current.Attempt != 1 {
				t.Fatal("recovery lost fill attribution or attempt identity", current, err)
			}
			for _, call := range adapter.calls() {
				if len(call) < 6 || call[:6] != "Query:" {
					t.Fatal("recovery performed a new network write", call)
				}
			}
			if boundary == "fill-before-cursor" {
				cursor, err := store.ProjectionCursor(context.Background(), "strategy")
				if err != nil || cursor != 0 {
					t.Fatal("unacknowledged callback advanced cursor", cursor, err)
				}
				events, err := store.EventsAfter(context.Background(), cursor, 512)
				found := false
				for _, event := range events {
					if event.ID == "committed-fill" {
						found = true
					}
				}
				if err != nil || !found || before.Checkpoint == 0 {
					t.Fatal("committed callback unavailable for crash replay", found, err)
				}
				if err := store.AdvanceProjection(context.Background(), "strategy", after.Checkpoint); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExecutionAbruptProcessChild(t *testing.T) {
	boundary := os.Getenv("BANBOT_EXEC_CRASH_BOUNDARY")
	if boundary == "" {
		return
	}
	store, err := OpenStoreWithLeaseDir(os.Getenv("BANBOT_EXEC_CRASH_DB"), testIntent(Buy).Account, os.Getenv("BANBOT_EXEC_CRASH_LEASES"))
	if err != nil {
		t.Fatal(err)
	}
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(store.Account())
	if err != nil {
		t.Fatal(err)
	}
	order := planOrder(t, store, "crash-order", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	adapter := &fakeExecutionAdapter{}
	if boundary == "sending" {
		adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
			os.Exit(42)
			return SubmitReceipt{}, nil
		}
	}
	if err := executorFor(store, owner, adapter).Send(order.ID, 11); err != nil {
		t.Fatal(err)
	}
	if boundary != "fill-before-cursor" {
		t.Fatal("unknown crash boundary", boundary)
	}
	if _, err := store.ApplyFill(context.Background(), FillReport{EventID: "committed-fill", OrderID: order.ID, Steps: 10, Price: intentPrice("100"), Fee: intentPrice("0.02"), AtMS: 12}); err != nil {
		t.Fatal(err)
	}
	if events, err := store.EventsAfter(context.Background(), 0, 512); err != nil || len(events) == 0 {
		t.Fatal("projection callback could not read committed fill", err)
	}
	os.Exit(42)
}
