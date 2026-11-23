package execution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type observedHubError struct {
	path string
	err  error
}

type hubErrorRecorder struct {
	mu   sync.Mutex
	seen []observedHubError
}

func (r *hubErrorRecorder) observe(path string, err error) {
	r.mu.Lock()
	r.seen = append(r.seen, observedHubError{path, err})
	r.mu.Unlock()
}

func (r *hubErrorRecorder) snapshot() []observedHubError {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]observedHubError(nil), r.seen...)
}

// The RPC reply never carries error text (factory/SDK errors may embed
// credentials), so the host-side observer is the only place an operator can see
// why an operation failed.
func TestHubServerErrorObserverReceivesRawErrorWhileClientGetsGenericText(t *testing.T) {
	var closed atomic.Int32
	const secret = "secret-token-from-factory"
	hub, err := NewHub(func(ctx context.Context, request HubAccountRegistration) (HubAccountBinding, error) {
		if request.Account.Account == "broken" {
			return HubAccountBinding{}, errors.New("factory failed with " + secret)
		}
		return hubTestBinding(t, request.Account, &closed), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	server, err := ListenHubUnix(hubTestSocket(t), hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	recorder := &hubErrorRecorder{}
	server.SetErrorObserver(recorder.observe)
	client := NewHubClient(server.path)
	defer client.Close()
	ctx := context.Background()

	broken := hubTestRegistration()
	broken.Account.Account = "broken"
	broken.Configuration = json.RawMessage(`{}`)
	regErr := client.Register(ctx, broken)
	if regErr == nil {
		t.Fatal("register with failing factory succeeded")
	}
	if strings.Contains(regErr.Error(), secret) || strings.Contains(regErr.Error(), "factory failed") {
		t.Fatalf("client received raw error text: %v", regErr)
	}
	seen := recorder.snapshot()
	if len(seen) != 1 || seen[0].path != "/v1/register" || seen[0].err == nil || !strings.Contains(seen[0].err.Error(), secret) {
		t.Fatalf("observer did not receive path and raw error: %+v", seen)
	}

	good := hubTestRegistration()
	if err := client.Register(ctx, good); err != nil {
		t.Fatal(err)
	}
	if n := len(recorder.snapshot()); n != 1 {
		t.Fatalf("successful operation reached the error observer: %d calls", n)
	}
	// Invalid loss-stop policy fails inside the hub operation.
	if err := client.SetLossStopPolicy(ctx, good.Account, LossStopPolicy{PauseMS: 1}); err == nil {
		t.Fatal("invalid policy accepted")
	} else if !strings.Contains(err.Error(), "hub operation failed") {
		t.Fatalf("client should only see generic text: %v", err)
	}
	seen = recorder.snapshot()
	if len(seen) != 2 || !strings.HasPrefix(seen[1].path, "/v1/") || seen[1].path == "/v1/register" || seen[1].err == nil {
		t.Fatalf("observer missed policy failure: %+v", seen)
	}

	// Removing the observer stops notifications.
	server.SetErrorObserver(nil)
	_ = client.SetLossStopPolicy(ctx, good.Account, LossStopPolicy{PauseMS: 1})
	if n := len(recorder.snapshot()); n != 2 {
		t.Fatalf("observer still called after removal: %d", n)
	}
}

// A below-minimum command is refused with a fixed code over RPC and must come
// back to the client as the typed ErrVenueMinimum so the robot can drop (not
// retry) it.
func TestHubClientApplyPendingCommandVenueMinimumSurvivesRPC(t *testing.T) {
	b, venue := strategyService(t, true)
	market := b.service.markets["BTC"]
	market.instrument.MinSteps = 1000 // far above the 10-step command below
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
	recorder := &hubErrorRecorder{}
	server.SetErrorObserver(recorder.observe)
	client := NewHubClient(path)
	defer client.Close()
	ctx := context.Background()
	key := b.service.AccountKey()
	if err := client.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "a", WriterID: "session-a"}); err != nil {
		t.Fatal(err)
	}
	command := pendingCreate("create-min", "a")
	command.StrategyEpoch, command.StrategySequence = 1, 1
	command.Create.Conditions.Limit = intentPrice("110") // marketable against the 100 quote
	before := venue.Metrics()
	_, err = client.ApplyPendingCommand(ctx, key, command)
	if !errors.Is(err, ErrVenueMinimum) {
		t.Fatalf("want ErrVenueMinimum over RPC, got %v (observed %v)", err, recorder.snapshot()[len(recorder.snapshot())-1].err)
	}
	if venue.Metrics() != before {
		t.Fatal("rejected command reached the venue")
	}
}
