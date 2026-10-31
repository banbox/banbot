package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func contributorFixture(t *testing.T, memory bool) *SharedAccountBorrow {
	t.Helper()
	b, _ := strategyService(t, memory)
	_, err := b.service.store.ApplyCashEvent(context.Background(), CashEvent{ID: "release-unassigned-capital", Kind: CapitalTransfer, Postings: []CashPosting{{Strategy: "a", Amount: intentPrice("-200")}, {Amount: intentPrice("200")}}, AtMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func contributorRequest() HubContributorRegistration {
	return HubContributorRegistration{Strategy: "c", PolicyVersion: "v1", Capital: intentPrice("100"), GrossLimit: intentPrice("500"), Instruments: []string{"BTC"}}
}

func TestHubContributorBatchRollsBackEarlierAllocations(t *testing.T) {
	for _, memory := range []bool{false, true} {
		b := contributorFixture(t, memory)
		before, err := b.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		first, second := contributorRequest(), contributorRequest()
		second.Strategy, second.Capital = "d", intentPrice("101")
		if err := b.RegisterContributors(context.Background(), []HubContributorRegistration{first, second}, 10); err == nil {
			t.Fatal("overallocated batch accepted")
		}
		after, _ := b.Snapshot(context.Background())
		if !reflect.DeepEqual(before, after) {
			t.Fatal("failed batch left partial capital allocation")
		}
		policy, err := b.service.store.AccountPolicy(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, present := policy.StrategyGrossLimits["c"]; present {
			t.Fatal("failed batch left partial risk policy")
		}
		if _, err := b.service.store.StrategyCheckpoint(context.Background(), "c", hubContributorCheckpoint); !errors.Is(err, sql.ErrNoRows) {
			t.Fatal("failed batch left acceptance checkpoint", err)
		}
		second.Capital = intentPrice("100")
		if err := b.RegisterContributors(context.Background(), []HubContributorRegistration{first, second}, 10); err != nil {
			t.Fatal(err)
		}
		accepted, _ := b.Snapshot(context.Background())
		if !accepted.UnassignedCash.IsZero() || !accepted.SyntheticStrategyCash["c"].Equal(intentPrice("100")) || !accepted.SyntheticStrategyCash["d"].Equal(intentPrice("100")) {
			t.Fatal("batch did not conserve capital", accepted)
		}
		if err := b.RegisterContributors(context.Background(), []HubContributorRegistration{first, second}, 11); err != nil {
			t.Fatal(err)
		}
		retried, _ := b.Snapshot(context.Background())
		if !reflect.DeepEqual(accepted, retried) {
			t.Fatal("lost ACK repeated capital transfer")
		}
	}
}

func TestHubContributorIncrementalRegistrationIsConservingAndIdempotent(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(map[bool]string{false: "SQLite", true: "Memory"}[memory], func(t *testing.T) {
			ctx := context.Background()
			b := contributorFixture(t, memory)
			hub, _ := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
				return HubAccountBinding{Account: b.service, Shutdown: func() error { return nil }}, nil
			})
			defer hub.Close()
			server, err := ListenHubUnix(hubTestSocket(t), hub)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			first := NewHubClient(server.path)
			defer first.Close()
			second := NewHubClient(server.path)
			defer second.Close()
			key := b.AccountKey()
			if err := first.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{"first_robot":true}`)}); err != nil {
				t.Fatal(err)
			}
			before, _ := b.Snapshot(ctx)
			request := contributorRequest()
			if err := second.RegisterContributor(ctx, key, request); err != nil {
				t.Fatal("second robot must not need first robot's complete configuration", err)
			}
			after, err := b.Snapshot(ctx)
			if err != nil || !after.AccountSettledCash.Equal(before.AccountSettledCash) || !after.UnassignedCash.Equal(intentPrice("100")) || !after.SyntheticStrategyCash["c"].Equal(intentPrice("100")) || !after.SyntheticStrategyCash["a"].Equal(before.SyntheticStrategyCash["a"]) {
				t.Fatal("registration minted capital or changed another strategy", after, err)
			}
			policy, err := b.service.store.AccountPolicy(ctx)
			if err != nil || !policy.StrategyGrossLimits["c"].Equal(request.GrossLimit) || policy.Version != "v1" {
				t.Fatal(policy, err)
			}
			if err := second.RegisterContributor(ctx, key, request); err != nil {
				t.Fatal("lost ACK retry failed", err)
			}
			retried, _ := b.Snapshot(ctx)
			if !reflect.DeepEqual(after, retried) {
				t.Fatal("registration retry transferred capital twice")
			}
			outside := strategyRequest("c", "outside-declaration", 10, StrategyTargetsFull, ExecutableTarget{Lot: "other", SignedSteps: 1})
			outside.Requests[0].Instrument.ID = "ETH"
			if _, err := b.PrepareStrategy(outside); err == nil || !strings.Contains(err.Error(), "outside contributor declaration") {
				t.Fatal("target bypassed immutable instrument declaration", err)
			}
			pending := pendingCreate("outside-pending", "c")
			pending.Create.Instrument = "ETH"
			if _, err := b.ApplyPendingCommand(ctx, pending, 10); err == nil || !strings.Contains(err.Error(), "outside contributor declaration") {
				t.Fatal("pending command bypassed immutable instrument declaration", err)
			}
			request.Capital = intentPrice("99")
			if err := second.RegisterContributor(ctx, key, request); err == nil {
				t.Fatal("changed registration reused strategy identity")
			}
		})
	}
}

func TestHubContributorRejectedRegistrationAndCommitFailureLeaveNoAllocation(t *testing.T) {
	ctx := context.Background()
	b := contributorFixture(t, false)
	store := b.service.store
	for _, failure := range []string{"budget", "policy", "new-instrument", "receipt-write"} {
		t.Run(failure, func(t *testing.T) {
			request := contributorRequest()
			switch failure {
			case "budget":
				request.Capital = intentPrice("201")
			case "policy":
				request.PolicyVersion = "changed-account-baseline"
			case "new-instrument":
				request.Instruments = []string{"ETH"}
			case "receipt-write":
				if _, err := store.db.Exec(`CREATE TRIGGER reject_contributor BEFORE INSERT ON exec_checkpoint WHEN NEW.name='hub-contributor:v1' BEGIN SELECT RAISE(ABORT,'registration persistence failed'); END`); err != nil {
					t.Fatal(err)
				}
				defer store.db.Exec(`DROP TRIGGER reject_contributor`)
			}
			before, _ := b.Snapshot(ctx)
			if err := b.RegisterContributor(ctx, request, 10); err == nil {
				t.Fatal("unsafe/incomplete registration accepted")
			}
			after, _ := b.Snapshot(ctx)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("rejected registration changed account cash or ledger")
			}
			policy, _ := store.AccountPolicy(ctx)
			if _, ok := policy.StrategyGrossLimits["c"]; ok {
				t.Fatal("cap escaped failed registration transaction")
			}
			if _, err := store.StrategyCheckpoint(ctx, "c", hubContributorCheckpoint); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("receipt escaped failed registration", err)
			}
		})
	}
}

func TestHubContributorFirstTargetSettlesConflictingNativeReservation(t *testing.T) {
	ctx := context.Background()
	b := contributorFixture(t, true)
	pending := pendingTestOrder(b.service.store, "a")
	pending.Entry.Conditions.CreatedBar = 10
	pending.Entry.Conditions.Limit = intentPrice("90")
	saveTestPendingBook(t, b.service.store, []PendingDefinition{{Order: *pending, Instrument: ledgerInstrument(), Policy: OrderPolicy{Mode: ExchangeFirst}}})
	if err := b.AdvancePendingExecution(ctx, 12); err != nil {
		t.Fatal(err)
	}
	before, _ := b.Snapshot(ctx)
	if len(before.Orders) != 1 {
		t.Fatal(before.Orders)
	}
	order := before.Orders[0]
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	adapter.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "registration-cancel-fill", OrderID: order.Intent.ID, Steps: 4, Price: intentPrice("90"), Cost: intentPrice("36"), Cumulative: true, AtMS: 14}}}}, nil
	}
	b.service.executor.Adapter = adapter
	if err := b.RegisterContributor(ctx, contributorRequest(), 13); err != nil {
		t.Fatal(err)
	}
	if len(adapter.calls()) != 0 {
		t.Fatal("zero-exposure registration must retain economic-lot exclusivity", adapter.calls())
	}
	prepared, err := b.PrepareStrategy(strategyRequest("c", "new-contributor-target", 14, StrategyTargetsFull, ExecutableTarget{Lot: "c-lot", SignedSteps: 1}))
	if err != nil {
		t.Fatal(err)
	}
	calls := adapter.calls()
	if len(calls) != 2 || !strings.HasPrefix(calls[0], "Cancel:") || !strings.HasPrefix(calls[1], "Query:") {
		t.Fatal("first target bypassed authoritative cancellation settlement", calls)
	}
	settled, _ := b.service.store.Order(ctx, order.Intent.ID)
	if settled.State != OrderCanceled || settled.FilledSteps != 4 {
		t.Fatal("final cancellation fill was not settled", settled)
	}
	for _, target := range prepared.Plan.Targets {
		if target.Strategy == "a" && target.SignedSteps != 4 {
			t.Fatal("new contributor inherited stale reservation target", target)
		}
	}
	if err := b.ValidateAccountBindings(map[StrategyID]bool{"a": true, "b": true}); err != nil {
		t.Fatal("durably registered offline contributor prevented account recovery", err)
	}
	registration := contributorRequest()
	registration.GrossLimit = intentPrice("1")
	body, _ := json.Marshal(registration)
	if err := b.service.store.SaveStrategyCheckpoint(ctx, "c", hubContributorCheckpoint, body); err != nil {
		t.Fatal(err)
	}
	if err := b.ValidateAccountBindings(map[StrategyID]bool{"a": true, "b": true}); err == nil {
		t.Fatal("corrupt contributor declaration bypassed recovery validation")
	}
}
