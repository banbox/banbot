package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// portHub serves one account through BOTH transports of the same Hub.
type portHub struct {
	socket   *HubClient
	local    *LocalHubPort
	recorder *hubErrorRecorder
	key      AccountKey
}

func newPortHub(t *testing.T, mutate func(*Instrument)) *portHub {
	t.Helper()
	b, _ := strategyService(t, true)
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
	socket := NewHubClient(path)
	t.Cleanup(socket.Close)
	local := NewLocalHubPort(hub)
	t.Cleanup(local.Close)
	recorder := &hubErrorRecorder{}
	local.SetErrorObserver(recorder.observe)
	key := b.service.AccountKey()
	ctx := context.Background()
	if err := local.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := local.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "a", WriterID: "session-a"}); err != nil {
		t.Fatal(err)
	}
	return &portHub{socket: socket, local: local, recorder: recorder, key: key}
}

func (h *portHub) ports() map[string]HubPort {
	return map[string]HubPort{"socket": h.socket, "local": h.local}
}

// classifyPortError reduces an error to what robot code may branch on.
func classifyPortError(err error) string {
	switch {
	case err == nil:
		return "nil"
	case errors.Is(err, ErrVenueMinimum):
		return "venue_minimum"
	case errors.Is(err, ErrPolicyCancellationRecoveryRequired):
		return "cancellation_recovery"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	}
	if code, ok := RejectionCode(err); ok {
		return "rejected:" + code
	}
	var typed *RejectedError
	if errors.As(err, &typed) {
		return "rejected-unlisted"
	}
	return "other:" + err.Error()
}

const genericHubFailure = "other:execution: hub operation failed; inspect account recovery state before retrying"

// The same scenario against the socket client and the in-process port must end
// in the same classification, and unclassified failures must be the fixed
// generic text (never the owner's raw message).
func TestLocalHubPortMatchesSocketClassification(t *testing.T) {
	create := func(h *portHub, p HubPort, mutate func(*PendingCommand)) error {
		command := pendingCreate("create-"+t.Name(), "a")
		command.StrategyEpoch, command.StrategySequence = 1, 1
		command.Create.Conditions.Limit = intentPrice("110")
		mutate(&command)
		_, err := p.ApplyPendingCommand(context.Background(), h.key, command)
		return err
	}
	scenarios := []struct {
		name   string
		mutate func(*Instrument)
		run    func(h *portHub, p HubPort) error
		want   string
	}{
		{"venue minimum on create", func(i *Instrument) { i.MinSteps = 1000 }, func(h *portHub, p HubPort) error {
			return create(h, p, func(*PendingCommand) {})
		}, "venue_minimum"},
		{"off tick on create", nil, func(h *portHub, p HubPort) error {
			return create(h, p, func(c *PendingCommand) { c.Create.Conditions.Limit = intentPrice("110.005") })
		}, "rejected:" + RejectLimitOffTick},
		{"risk limit on create", nil, func(h *portHub, p HubPort) error {
			return create(h, p, func(c *PendingCommand) {
				c.Create.QuantitySteps = 1000000000
				c.Create.Conditions.Limit = intentPrice("100")
			})
		}, "rejected:" + RejectRiskLimit},
		{"spot short on create", func(i *Instrument) { i.Valuation = ValuationSpot }, func(h *portHub, p HubPort) error {
			return create(h, p, func(c *PendingCommand) {
				c.Create.Side = Sell
				c.Create.Conditions.Limit = intentPrice("90")
			})
		}, "rejected:" + RejectSpotShort},
		{"allowlisted refusal on an edit stays generic", nil, func(h *portHub, p HubPort) error {
			ctx := context.Background()
			first := pendingCreate("edit-base", "a")
			first.StrategyEpoch, first.StrategySequence = 1, 1
			if _, err := h.socket.ApplyPendingCommand(ctx, h.key, first); err != nil {
				return fmt.Errorf("setup: %w", err)
			}
			edit := PendingCommand{ID: "edit-off-tick", Strategy: "a", Lot: first.Lot, ExpectedRevision: 1, Action: PendingEditLimit, Limit: intentPrice("90.005"), StrategyEpoch: 1, StrategySequence: 2}
			_, err := p.ApplyPendingCommand(ctx, h.key, edit)
			return err
		}, genericHubFailure},
		{"stale pending command", nil, func(h *portHub, p HubPort) error {
			_, err := p.ApplyPendingCommand(context.Background(), h.key, PendingCommand{ID: "stale", Strategy: "a", Lot: "missing", Action: PendingCancelEntry, StrategyEpoch: 1, StrategySequence: 1})
			return err
		}, genericHubFailure},
		{"time-series exit refusal", nil, func(h *portHub, p HubPort) error {
			_, err := p.ApplyTimeSeriesExit(context.Background(), h.key, TimeSeriesExitCommand{ID: "x", Strategy: "a"})
			return err
		}, genericHubFailure},
		{"unregistered account", nil, func(h *portHub, p HubPort) error {
			other := h.key
			other.Account = other.Account + "-unknown"
			_, err := p.Snapshot(context.Background(), other)
			return err
		}, genericHubFailure},
		{"canceled context", nil, func(h *portHub, p HubPort) error {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := p.Snapshot(ctx, h.key)
			return err
		}, "canceled"},
		{"success", nil, func(h *portHub, p HubPort) error {
			_, err := p.Snapshot(context.Background(), h.key)
			return err
		}, "nil"},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			got := map[string]string{}
			for _, name := range []string{"socket", "local"} {
				h := newPortHub(t, sc.mutate) // fresh account per transport
				got[name] = classifyPortError(sc.run(h, h.ports()[name]))
			}
			if got["socket"] != sc.want || got["local"] != sc.want {
				t.Fatalf("want %q on both, got %+v", sc.want, got)
			}
		})
	}
}

// Classes that need an owner failure the test Hub cannot produce are checked
// through the shared failure encoding and client decoding (the transport-free
// core of both paths).
func TestHubReplyMappingClasses(t *testing.T) {
	risk := rejected(RejectRiskLimit, "secret detail")
	create := hubRPCRequest{Pending: &PendingCommand{Action: PendingCreate}}
	exit := hubRPCRequest{TSExit: &TimeSeriesExitCommand{}}
	cases := []struct {
		name    string
		path    string
		request hubRPCRequest
		result  hubRPCResponse
		err     error
		want    string
	}{
		{"venue minimum on exit", "/v1/ts-exit", exit, hubRPCResponse{}, &venueMinimumError{Instrument: "BTC"}, "venue_minimum"},
		{"venue minimum on edit", "/v1/pending-command", hubRPCRequest{Pending: &PendingCommand{Action: PendingEditLimit}}, hubRPCResponse{}, ErrVenueMinimum, "venue_minimum"},
		{"rejection on create", "/v1/pending-command", create, hubRPCResponse{}, risk, "rejected:" + RejectRiskLimit},
		{"rejection on exit kind", "/v1/ts-exit", exit, hubRPCResponse{}, risk, genericHubFailure},
		{"wrapped rejection on exit kind", "/v1/pending-command", hubRPCRequest{Pending: &PendingCommand{Action: PendingSetExit}}, hubRPCResponse{}, errors.Join(errors.New("x"), risk), genericHubFailure},
		{"policy cancellation recovery", "/v1/policy-cancel-entries", hubRPCRequest{}, hubRPCResponse{}, fmt.Errorf("wrap: %w", ErrPolicyCancellationRecoveryRequired), "cancellation_recovery"},
		{"target send failed", "/v1/target-resume", hubRPCRequest{}, hubRPCResponse{TargetRecovery: &HubTargetRecovery{Found: true}}, errors.New("send failed: secret"), "other:execution: accepted hub target send requires account recovery"},
		{"arbitrary raw error", "/v1/snapshot", hubRPCRequest{}, hubRPCResponse{}, errors.New("secret token abc"), genericHubFailure},
		{"context canceled in owner", "/v1/snapshot", hubRPCRequest{}, hubRPCResponse{}, context.Canceled, genericHubFailure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var observed error
			observe := func(_ string, err error) { observed = err }
			reply, ok := hubFailureReply(tc.path, tc.request, tc.result, tc.err, &observe)
			var err error
			if ok {
				body, e := json.Marshal(reply)
				if e != nil {
					t.Fatal(e)
				}
				var decoded hubRPCResponse
				if e = json.Unmarshal(body, &decoded); e != nil {
					t.Fatal(e)
				}
				err = hubReplyError(decoded)
			} else {
				err = errors.New("execution: hub operation failed; inspect account recovery state before retrying")
			}
			if got := classifyPortError(err); got != tc.want {
				t.Fatalf("want %q, got %q", tc.want, got)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatalf("raw text leaked: %v", err)
			}
			_ = observed
		})
	}
}

// The in-process port feeds the host observer the raw error, like the server.
func TestLocalHubPortObserverReceivesRawError(t *testing.T) {
	h := newPortHub(t, nil)
	_, err := h.local.ApplyPendingCommand(context.Background(), h.key, PendingCommand{ID: "stale", Strategy: "a", Lot: "missing", Action: PendingCancelEntry, StrategyEpoch: 1, StrategySequence: 1})
	if classifyPortError(err) != genericHubFailure {
		t.Fatal(err)
	}
	seen := h.recorder.snapshot()
	if len(seen) != 1 || seen[0].path != "/v1/pending-command" || seen[0].err == nil || seen[0].err.Error() == err.Error() {
		t.Fatalf("observer must get the raw error, not the client one: %+v", seen)
	}
	h.local.SetErrorObserver(nil)
	_, _ = h.local.ApplyPendingCommand(context.Background(), h.key, PendingCommand{ID: "stale2", Strategy: "a", Lot: "missing", Action: PendingCancelEntry, StrategyEpoch: 1, StrategySequence: 2})
	if len(h.recorder.snapshot()) != 1 {
		t.Fatal("observer called after removal")
	}
}

// Mutating anything the port returned must not change the owner's later
// answers (the in-process path must not share memory with the owner).
func TestLocalHubPortResponsesAreIsolatedFromOwner(t *testing.T) {
	h := newPortHub(t, nil)
	ctx := context.Background()
	command := pendingCreate("alias", "a")
	command.StrategyEpoch, command.StrategySequence = 1, 1
	command.Create.Protections = []PositionProtection{{Kind: StopLossProtection, Level: intentPrice("80")}}
	if _, err := h.local.ApplyPendingCommand(ctx, h.key, command); err != nil {
		t.Fatal(err)
	}
	// The request is isolated too: reshaping it after the call changes nothing.
	command.Create.Protections[0].Level = intentPrice("1")
	command.Create.Instrument = "ZZZ"

	wantPending, err := h.socket.PendingOrders(ctx, h.key)
	if err != nil || len(wantPending) != 1 {
		t.Fatal(wantPending, err)
	}
	wantSnap, err := h.socket.Snapshot(ctx, h.key)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := h.local.PendingOrders(ctx, h.key)
	if err != nil || !reflect.DeepEqual(pending, wantPending) {
		t.Fatalf("local view differs from socket view: %v\n%+v\n%+v", err, pending, wantPending)
	}
	snap, err := h.local.Snapshot(ctx, h.key)
	if err != nil {
		t.Fatal(err)
	}
	// Vandalise every returned value.
	pending[0].Instrument.ID = "mutated"
	if len(pending[0].Specs) == 0 {
		t.Fatal("test needs a protection spec")
	}
	pending[0].Specs[0].Level = intentPrice("1")
	pending[0].Specs = append(pending[0].Specs, PositionProtection{Kind: TakeProfitProtection})
	pending = append(pending[:0], PendingDefinition{})
	for k := range snap.SyntheticStrategyCash {
		snap.SyntheticStrategyCash[k] = intentPrice("-1")
	}
	snap.SyntheticStrategyCash["ghost"] = intentPrice("5")
	for i := range snap.Orders {
		snap.Orders[i] = StoredOrder{}
	}
	snap.Lots = append(snap.Lots, VirtualLot{})
	_ = pending

	gotPending, err := h.local.PendingOrders(ctx, h.key)
	if err != nil || !reflect.DeepEqual(gotPending, wantPending) {
		t.Fatalf("pending answer changed after the caller mutated its copy: %v\n%+v\n%+v", err, gotPending, wantPending)
	}
	gotSnap, err := h.local.Snapshot(ctx, h.key)
	if err != nil || !reflect.DeepEqual(gotSnap, wantSnap) {
		t.Fatalf("snapshot answer changed after the caller mutated its copy: %v", err)
	}
	if again, err := h.socket.PendingOrders(ctx, h.key); err != nil || !reflect.DeepEqual(again, wantPending) {
		t.Fatalf("owner state changed: %v", err)
	}
}

func TestNewLocalHubPortNilHub(t *testing.T) {
	if NewLocalHubPort(nil) != nil {
		t.Fatal("nil hub must give a nil port")
	}
}
