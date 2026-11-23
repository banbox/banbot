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

type rejectionHub struct {
	b        *SharedAccountBorrow
	venue    *PaperAdapter
	client   *HubClient
	recorder *hubErrorRecorder
	key      AccountKey
}

// newRejectionHub serves a strategy account over the real Unix-socket RPC. The
// market's quote is made valid at the hub's wall clock; mutate may reshape the
// instrument before the account is registered.
func newRejectionHub(t *testing.T, mutate func(*Instrument)) *rejectionHub {
	t.Helper()
	b, venue := strategyService(t, true)
	market := b.service.markets["BTC"]
	if mutate != nil {
		mutate(&market.instrument)
	}
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
	t.Cleanup(func() { server.Close() })
	recorder := &hubErrorRecorder{}
	server.SetErrorObserver(recorder.observe)
	client := NewHubClient(path)
	t.Cleanup(client.Close)
	key := b.service.AccountKey()
	ctx := context.Background()
	if err := client.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "a", WriterID: "session-a"}); err != nil {
		t.Fatal(err)
	}
	return &rejectionHub{b: b, venue: venue, client: client, recorder: recorder, key: key}
}

// assertUntouched fails if a rejected command left any trace: account state,
// pending book, plans, command receipt/revision, writer sequence or venue.
type accountTrace struct {
	snapshot AccountSnapshot
	book     []PendingDefinition
	hasPlan  bool
	metrics  PaperMetrics
	writer   strategyWriterState
}

func (h *rejectionHub) trace(t *testing.T) accountTrace {
	t.Helper()
	ctx := context.Background()
	var out accountTrace
	var err error
	if out.snapshot, err = h.b.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if out.book, err = h.b.PendingOrders(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = h.b.service.store.LatestPlan(ctx); err == nil {
		out.hasPlan = true
	} else if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	out.metrics = h.venue.Metrics()
	if out.writer, err = h.b.service.store.strategyWriter(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	return out
}

func (h *rejectionHub) assertNoReceipt(t *testing.T, command PendingCommand) {
	t.Helper()
	ctx := context.Background()
	for _, name := range []string{"pending-command:" + command.ID, "pending-revision:" + string(command.Lot)} {
		if _, err := h.b.service.store.StrategyCheckpoint(ctx, command.Strategy, name); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("rejected command left checkpoint %s: %v", name, err)
		}
	}
}

func TestHubPendingCreateRejectionsAreTypedPreCommitAndSurviveRPC(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Instrument)
		edit   func(*PendingCommand)
		code   string
		text   string
	}{
		{"tick", nil, func(c *PendingCommand) { c.Create.Conditions.Limit = intentPrice("110.005") }, RejectLimitOffTick, "violates instrument tick"},
		{"risk gross", nil, func(c *PendingCommand) {
			c.Create.QuantitySteps = 1000000000
			c.Create.Conditions.Limit = intentPrice("100")
		}, RejectRiskLimit, "budget exceeded"},
		{"spot short", func(i *Instrument) { i.Valuation = ValuationSpot }, func(c *PendingCommand) {
			c.Create.Side = Sell
			c.Create.Conditions.Limit = intentPrice("90")
		}, RejectSpotShort, "spot"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRejectionHub(t, tc.mutate)
			ctx := context.Background()
			command := pendingCreate("create-"+tc.code, "a")
			command.StrategyEpoch, command.StrategySequence = 1, 1
			command.Create.Conditions.Limit = intentPrice("110") // marketable
			tc.edit(&command)
			before := h.trace(t)
			_, err := h.client.ApplyPendingCommand(ctx, h.key, command)
			if code, ok := RejectionCode(err); !ok || code != tc.code {
				observed := h.recorder.snapshot()
				t.Fatalf("want typed %s over RPC, got %v (host saw %v)", tc.code, err, observed)
			}
			if !errors.Is(err, &RejectedError{Code: tc.code}) || errors.Is(err, ErrVenueMinimum) {
				t.Fatalf("typed rejection must match its code only: %v", err)
			}
			if strings.Contains(err.Error(), "budget") || strings.Contains(err.Error(), "instrument tick") {
				t.Fatalf("owner error text crossed the RPC: %v", err)
			}
			seen := h.recorder.snapshot()
			if len(seen) == 0 || !strings.Contains(seen[len(seen)-1].err.Error(), tc.text) {
				t.Fatalf("host log lost the raw error: %+v", seen)
			}
			after := h.trace(t)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected command changed the account:\nbefore %+v\nafter  %+v", before, after)
			}
			h.assertNoReceipt(t, command)

			// The identical command is refused again (deterministic), and a
			// valid one with a newer sequence is accepted afterwards.
			if _, again := h.client.ApplyPendingCommand(ctx, h.key, command); !errors.Is(again, &RejectedError{Code: tc.code}) {
				t.Fatalf("resend not refused identically: %v", again)
			}
			good := pendingCreate("create-good", "a")
			good.Lot = "lot-good"
			good.StrategyEpoch, good.StrategySequence = 1, 2
			if revision, err := h.client.ApplyPendingCommand(ctx, h.key, good); err != nil || revision != 1 {
				t.Fatalf("valid command after a rejection: %d %v", revision, err)
			}
		})
	}
}

// Self-cross and venue-minimum style rejections are raised while the plan is
// prepared. Self-cross is reached directly (a same-strategy opposing delta).
func TestHubSelfCrossRejectionIsTypedAndPreCommit(t *testing.T) {
	h := newRejectionHub(t, nil)
	before := h.trace(t)
	request := strategyRequest("a", "cross", 10, StrategyTargetsFull, ExecutableTarget{Lot: "long", SignedSteps: 10}, ExecutableTarget{Lot: "short", SignedSteps: -10})
	_, err := h.b.PrepareStrategy(request)
	if code, ok := RejectionCode(err); !ok || code != RejectSelfCross {
		t.Fatalf("want self-cross rejection, got %v", err)
	}
	after := h.trace(t)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("self-cross left a trace")
	}
}

// Only a new entry may be dropped. The same refusals for any other command, on
// another endpoint, or with a code outside the closed set stay generic.
func TestHubRPCRejectionGate(t *testing.T) {
	risk := rejected(RejectRiskLimit, "secret detail")
	create := hubRPCRequest{Pending: &PendingCommand{Action: PendingCreate}}
	for _, tc := range []struct {
		name    string
		path    string
		request hubRPCRequest
		err     error
		want    string
	}{
		{"create", "/v1/pending-command", create, risk, RejectRiskLimit},
		{"wrapped", "/v1/pending-command", create, errors.Join(errors.New("x"), risk), RejectRiskLimit},
		{"edit", "/v1/pending-command", hubRPCRequest{Pending: &PendingCommand{Action: PendingEditLimit}}, risk, ""},
		{"set exit", "/v1/pending-command", hubRPCRequest{Pending: &PendingCommand{Action: PendingSetExit}}, risk, ""},
		{"protections", "/v1/pending-command", hubRPCRequest{Pending: &PendingCommand{Action: PendingSetProtections}}, risk, ""},
		{"ts exit", "/v1/ts-exit", hubRPCRequest{TSExit: &TimeSeriesExitCommand{}}, risk, ""},
		{"rebalance", "/v1/rebalance", hubRPCRequest{}, risk, ""},
		{"unknown code", "/v1/pending-command", create, &RejectedError{Code: "free text", Err: errors.New("x")}, ""},
		{"plain", "/v1/pending-command", create, errors.New("stale pending command revision"), ""},
		{"venue minimum any kind", "/v1/ts-exit", hubRPCRequest{TSExit: &TimeSeriesExitCommand{}}, ErrVenueMinimum, hubRejectedVenueMinimum},
		{"venue minimum edit", "/v1/pending-command", hubRPCRequest{Pending: &PendingCommand{Action: PendingEditLimit}}, ErrVenueMinimum, hubRejectedVenueMinimum},
	} {
		if got := hubRPCRejection(tc.path, tc.request, tc.err); got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}

// Anything that is not classified keeps the generic fail-closed answer.
func TestHubUnclassifiedPendingErrorsStayGeneric(t *testing.T) {
	h := newRejectionHub(t, nil)
	ctx := context.Background()
	stale := PendingCommand{ID: "stale", Strategy: "a", Lot: "missing", Action: PendingCancelEntry, StrategyEpoch: 1, StrategySequence: 1}
	_, err := h.client.ApplyPendingCommand(ctx, h.key, stale)
	if err == nil || !strings.Contains(err.Error(), "hub operation failed") {
		t.Fatalf("unclassified error must be the generic failure: %v", err)
	}
	if _, typed := RejectionCode(err); typed || errors.Is(err, ErrVenueMinimum) {
		t.Fatalf("unclassified error became typed: %v", err)
	}
	malformed := pendingCreate("bad", "a")
	malformed.StrategyEpoch, malformed.StrategySequence = 1, 2
	malformed.Create.Protections = []PositionProtection{{Kind: "invalid"}}
	if _, err = h.client.ApplyPendingCommand(ctx, h.key, malformed); err == nil || !strings.Contains(err.Error(), "hub operation failed") {
		t.Fatalf("malformed command must stay generic: %v", err)
	}
	// A stale writer sequence is never a droppable rejection.
	fresh := pendingCreate("fresh", "a")
	fresh.StrategyEpoch, fresh.StrategySequence = 1, 5
	if _, err = h.client.ApplyPendingCommand(ctx, h.key, fresh); err != nil {
		t.Fatal(err)
	}
	old := pendingCreate("old", "a")
	old.Lot = "lot-old"
	old.StrategyEpoch, old.StrategySequence = 1, 3
	if _, err = h.client.ApplyPendingCommand(ctx, h.key, old); err == nil {
		t.Fatal("stale sequence accepted")
	} else if _, typed := RejectionCode(err); typed {
		t.Fatalf("stale sequence became typed: %v", err)
	}
}
