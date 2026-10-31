package execution

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func hubTestRegistration() HubAccountRegistration {
	return HubAccountRegistration{Account: AccountKey{"hub-session", "account", "USDT"}, Env: "dry_run", Configuration: json.RawMessage(`{"capital":1000,"api_key":"never-echo-this"}`)}
}

func hubTestBinding(t *testing.T, key AccountKey, closed *atomic.Int32) HubAccountBinding {
	t.Helper()
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	paper, err := NewPaperAdapter(decimal.NewFromInt(1000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	account, err := NewSharedAccount(owner, SharedExecutionOptions{Memory: true, Adapter: paper, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	return HubAccountBinding{Account: account, Shutdown: func() error {
		closed.Add(1)
		registry.Close()
		return account.Close()
	}}
}

func TestHubRegistrationIsDynamicIdempotentAndConfigurationFenced(t *testing.T) {
	var created, closed atomic.Int32
	hub, err := NewHub(func(ctx context.Context, request HubAccountRegistration) (HubAccountBinding, error) {
		created.Add(1)
		return hubTestBinding(t, request.Account, &closed), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	if created.Load() != 0 {
		t.Fatal("hub created an account before a robot requested it")
	}
	request := hubTestRegistration()
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if err := hub.Register(context.Background(), request); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	request.Configuration = json.RawMessage(`{ "api_key":"never-echo-this", "capital":1000 }`)
	if err := hub.Register(context.Background(), request); err != nil {
		t.Fatal("format-only change was not idempotent", err)
	}
	request.Configuration = json.RawMessage(`{"capital":2000}`)
	if err := hub.Register(context.Background(), request); err == nil {
		t.Fatal("conflicting account configuration was accepted")
	}
	request = hubTestRegistration()
	request.Env = "prod"
	if err := hub.Register(context.Background(), request); err == nil {
		t.Fatal("environment change for the same account was accepted")
	}
	if created.Load() != 1 {
		t.Fatalf("created %d owners", created.Load())
	}
	if err := hub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := hub.Close(); err != nil || closed.Load() != 1 {
		t.Fatalf("shutdown is not once-only: %d, %v", closed.Load(), err)
	}
	if err := hub.Register(context.Background(), hubTestRegistration()); !errors.Is(err, ErrOwnerStopped) {
		t.Fatal("registration after shutdown", err)
	}
}

func TestHubRejectsNonLiveEnvironmentsBeforeFactory(t *testing.T) {
	hub, _ := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		t.Error("invalid environment reached account factory")
		return HubAccountBinding{}, nil
	})
	defer hub.Close()
	for _, env := range []string{"", "test", "backtest", "back_test", "research"} {
		request := hubTestRegistration()
		request.Env = env
		if err := hub.Register(context.Background(), request); err == nil {
			t.Fatalf("accepted environment %q", env)
		}
	}
}

func TestHubShutdownCancelsInitializationAndJoinsBinding(t *testing.T) {
	started := make(chan struct{})
	var closed atomic.Int32
	hub, _ := NewHub(func(ctx context.Context, request HubAccountRegistration) (HubAccountBinding, error) {
		binding := hubTestBinding(t, request.Account, &closed)
		close(started)
		<-ctx.Done()
		return binding, nil
	})
	registered := make(chan error, 1)
	go func() { registered <- hub.Register(context.Background(), hubTestRegistration()) }()
	<-started
	if err := hub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-registered; !errors.Is(err, ErrOwnerStopped) {
		t.Fatal("shutdown admitted account registration", err)
	}
	if closed.Load() != 1 {
		t.Fatal("registration racing shutdown leaked or double-closed its binding", closed.Load())
	}
}

func hubTestSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "banhub-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "hub.sock")
}

func TestHubUnixClientDisconnectKeepsAccountAndWorkerAlive(t *testing.T) {
	var closed atomic.Int32
	advance := make(chan struct{})
	advanced := make(chan struct{})
	hub, _ := NewHub(func(ctx context.Context, request HubAccountRegistration) (HubAccountBinding, error) {
		binding := hubTestBinding(t, request.Account, &closed)
		borrow := binding.Account.Borrow()
		if err := borrow.BootstrapCapital(ctx, map[StrategyID]decimal.Decimal{"strategy": decimal.NewFromInt(100)}, 1); err != nil {
			t.Fatal(err)
		}
		borrow.Release()
		workerCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			select {
			case <-workerCtx.Done():
				return
			case <-advance:
			}
			account := binding.Account.Borrow()
			defer account.Release()
			if err := account.CashEvent(CashEvent{ID: "hub-worker", Kind: Funding, AccountDelta: decimal.NewFromInt(2), Postings: []CashPosting{{Strategy: "strategy", Amount: decimal.NewFromInt(2)}}, AtMS: 2}); err != nil {
				t.Error(err)
			}
			close(advanced)
		}()
		shutdown := binding.Shutdown
		binding.Shutdown = func() error { cancel(); <-done; return shutdown() }
		return binding, nil
	})
	path := hubTestSocket(t)
	server, err := ListenHubUnix(path, hub)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("socket is not private: %v, %v", info, err)
	}
	client := NewHubClient(path)
	request := hubTestRegistration()
	if err := client.Register(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	client.Close()
	close(advance)
	select {
	case <-advanced:
	case <-time.After(5 * time.Second):
		t.Fatal("hub account worker did not survive robot disconnect")
	}
	client = NewHubClient(path)
	defer client.Close()
	snapshot, err := client.Snapshot(context.Background(), request.Account)
	if err != nil || !snapshot.SyntheticStrategyCash["strategy"].Equal(decimal.NewFromInt(102)) {
		t.Fatalf("snapshot did not reflect independent hub worker: %+v, %v", snapshot, err)
	}
	accepted, err := client.ObservePendingFeed(context.Background(), request.Account, PendingFeedObservation{Feed: "1/1m", Closed: true, EndMS: 60000})
	if err != nil || !accepted {
		t.Fatal("feed observation", accepted, err)
	}
	accepted, err = client.ObservePendingFeed(context.Background(), request.Account, PendingFeedObservation{Feed: "1/1m", Closed: true, EndMS: 60000})
	if err != nil || accepted {
		t.Fatal("duplicate feed observation", accepted, err)
	}
	events, err := client.ProjectionEvents(context.Background(), request.Account, "robot", 10)
	if err != nil || len(events) == 0 {
		t.Fatal("projection events", events, err)
	}
	if err := client.AdvanceProjection(context.Background(), request.Account, "robot", events[len(events)-1].Checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatal("server did not close account", closed.Load())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("owned socket was not removed", err)
	}
}

func TestHubUnixDoesNotStealOrDeleteAnotherSocket(t *testing.T) {
	hub, _ := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{}, nil
	})
	path := hubTestSocket(t)
	server, err := ListenHubUnix(path, hub)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if _, err := ListenHubUnix(path, hub); err == nil {
		t.Fatal("second server stole an existing socket")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(path); err != nil || string(body) != "replacement" {
		t.Fatal("server deleted a replacement path", string(body), err)
	}
}

func TestHubRPCDoesNotEchoFactorySecrets(t *testing.T) {
	hub, _ := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{}, errors.New("api_key=never-echo-this")
	})
	server, err := ListenHubUnix(hubTestSocket(t), hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := NewHubClient(server.path)
	defer client.Close()
	if err := client.Register(context.Background(), hubTestRegistration()); err == nil || strings.Contains(err.Error(), "never-echo-this") {
		t.Fatal("factory error was absent or exposed credentials", err)
	}
	response, err := client.client.Post("http://hub/v1/register", "application/json", strings.NewReader(`{"Registration":{"unknown":"never-echo-this"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatal("unknown request field was accepted", response.StatusCode)
	}
}

func TestHubTargetsUseOwnerQuotesAndDurableRetryIdentity(t *testing.T) {
	var closed atomic.Int32
	var quotes atomic.Int32
	var service *SharedAccount
	hub, _ := NewHub(func(ctx context.Context, request HubAccountRegistration) (HubAccountBinding, error) {
		binding := hubTestBinding(t, request.Account, &closed)
		service = binding.Account
		borrow := service.Borrow()
		defer borrow.Release()
		if err := borrow.BootstrapCapital(ctx, map[StrategyID]decimal.Decimal{"a": intentPrice("500"), "b": intentPrice("500")}, time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		if err := borrow.RegisterRiskPolicy("v1", domainRisk()); err != nil {
			t.Fatal(err)
		}
		if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
			quotes.Add(1)
			return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000, Bar: now}, nil
		}, func() int64 { return time.Now().UnixMilli() }); err != nil {
			t.Fatal(err)
		}
		if err := borrow.Reconcile("hub-ready", time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		return binding, nil
	})
	defer hub.Close()
	registration := hubTestRegistration()
	if err := hub.Register(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	request := strategyRequest("a", "robot-target-1", now, StrategyTargetsFull, ExecutableTarget{Lot: "one", SignedSteps: 2})
	lease, err := hub.ClaimStrategyWriter(context.Background(), registration.Account, StrategyWriterClaim{Strategy: "a", WriterID: "robot-session-1"})
	if err != nil {
		t.Fatal(err)
	}
	request.StrategyEpoch, request.StrategySequence = lease.Epoch, 1
	request.ExpiresMS = now + 60000
	request.Requests[0].Quote.Bid = intentPrice("1")
	request.Requests[0].Quote.Ask = intentPrice("1")
	if err := hub.RebalanceStrategy(context.Background(), registration.Account, request, 0); err != nil {
		t.Fatal(err)
	}
	snapshot, err := hub.Snapshot(context.Background(), registration.Account)
	if err != nil || len(snapshot.Lots) != 1 || !snapshot.Lots[0].CostBasis.Equal(intentPrice("20")) {
		t.Fatalf("target did not use owner quote: %+v, %v", snapshot, err)
	}
	firstQuotes := quotes.Load()
	if err := hub.RebalanceStrategy(context.Background(), registration.Account, request, now+999999); err != nil {
		t.Fatal("retry changed accepted target hash", err)
	}
	after, err := hub.Snapshot(context.Background(), registration.Account)
	if err != nil || after.Checkpoint != snapshot.Checkpoint || quotes.Load() != firstQuotes {
		t.Fatalf("retry repeated quote collection or changed ledger: %d/%d, %d/%d, %v", snapshot.Checkpoint, after.Checkpoint, firstQuotes, quotes.Load(), err)
	}
	if body, err := service.store.StrategyCheckpoint(context.Background(), "a", "hub-target:robot-target-1"); err != nil || len(body) == 0 {
		t.Fatal("owner quote retry identity was not durable", err)
	}
	request.Requests[0].Targets[0].SignedSteps = 3
	if err := hub.RebalanceStrategy(context.Background(), registration.Account, request, now); err == nil {
		t.Fatal("accepted target identity was reused for a different target")
	}
	request.PlanID = "robot-flat-3"
	request.StrategySequence = 3
	request.Requests[0].Targets[0].SignedSteps = 0
	if err := hub.RebalanceStrategy(context.Background(), registration.Account, request, now); err != nil {
		t.Fatal(err)
	}
	request.PlanID = "robot-delayed-2"
	request.StrategySequence = 2
	request.Requests[0].Targets[0].SignedSteps = 10
	if err := hub.RebalanceStrategy(context.Background(), registration.Account, request, now); err == nil {
		t.Fatal("hub arrival time overwrote strategy command ordering")
	}
	request.StrategyEpoch, request.StrategySequence = 0, 0
	if err := hub.RebalanceStrategy(context.Background(), registration.Account, request, now); err == nil {
		t.Fatal("remote writer bypassed ordering by omitting its epoch")
	}
	final, err := hub.Snapshot(context.Background(), registration.Account)
	if err != nil {
		t.Fatal(err)
	}
	for _, lot := range final.Lots {
		if lot.Strategy == "a" && lot.SignedSteps != 0 {
			t.Fatal("delayed command reopened the strategy", lot)
		}
	}
}
