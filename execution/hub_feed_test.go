package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestHubPublicFeedExpiresAfterRobotDisconnect(t *testing.T) {
	ctx := context.Background()
	robot, paper := strategyService(t, true)
	service := robot.service
	unavailable := false
	quoteFailure := errors.New("temporary quote failure")
	market := service.markets["BTC"]
	market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		if unavailable {
			return VisibleQuote{}, quoteFailure
		}
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000, Bar: 0}, nil
	}
	service.markets["BTC"] = market
	order := pendingTestOrder(service.store, "a")
	order.Entry.Conditions.CreatedBar = 0
	order.Entry.Conditions.Limit = intentPrice("90")
	if err := robot.WithState(func(account *SharedAccount) error { return account.BindPendingFeedExpiry(ctx, order, "17/1m", 1) }); err != nil {
		t.Fatal(err)
	}
	saveTestPendingBook(t, service.store, []PendingDefinition{{Order: *order, Instrument: ledgerInstrument(), Policy: OrderPolicy{Mode: ExchangeFirst}}})
	if err := robot.AdvancePendingExecution(ctx, 12); err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.store.Snapshot(ctx)
	if err != nil || len(snapshot.Orders) != 1 {
		t.Fatal("entry did not rest", snapshot.Orders, err)
	}
	orderID := snapshot.Orders[0].Intent.ID
	hub, err := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{Account: service, Shutdown: func() error { return nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	if err := hub.Register(ctx, HubAccountRegistration{Account: service.AccountKey(), Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	robot.Release()
	feeds, err := hub.PendingFeeds(ctx)
	if err != nil || len(feeds) != 1 || feeds[0].Feed != "17/1m" {
		t.Fatal(feeds, err)
	}
	wrong := feeds[0]
	wrong.Instrument = "ETH"
	if err := hub.AuthorizePublicFeed(wrong); err != nil {
		t.Fatal(err)
	}
	if err := hub.ObservePublicFeed(ctx, PendingFeedObservation{Feed: "17/1m", EndMS: 13, Closed: true}); err != nil {
		t.Fatal(err)
	}
	untrusted, err := service.store.LoadPendingFeeds(ctx)
	if err != nil || len(untrusted) != 0 {
		t.Fatal("unverified account/instrument/feed binding advanced", untrusted, err)
	}
	if err := hub.AuthorizePublicFeed(feeds[0]); err != nil {
		t.Fatal(err)
	}
	if err := hub.ObservePublicFeed(ctx, PendingFeedObservation{Feed: "other/1m", EndMS: 13, Closed: true}); err != nil {
		t.Fatal(err)
	}
	unavailable = true
	if err := hub.ObservePublicFeed(ctx, PendingFeedObservation{Feed: "17/1m", EndMS: 13, Closed: true}); !errors.Is(err, quoteFailure) {
		t.Fatal("quote failure lost", err)
	}
	progress, err := service.store.LoadPendingFeeds(ctx)
	if err != nil || progress["17/1m"].Bars != 1 {
		t.Fatal("accepted fact was not durable before advancement failure", progress, err)
	}
	unavailable = false
	if err := hub.ObservePublicFeed(ctx, PendingFeedObservation{Feed: "17/1m", EndMS: 13, Closed: true}); err != nil {
		t.Fatal(err)
	}
	book, err := service.store.LoadPendingBook(ctx)
	if err != nil || len(book) != 1 || !book[0].Order.Canceled {
		t.Fatal("public feed did not expire robot-owned entry", book, err)
	}
	settled, err := service.store.Order(ctx, orderID)
	if err != nil || settled.State != OrderCanceled || paper.Metrics().Fills != 0 {
		t.Fatal("public expiry did not cancel resting venue order", settled, err)
	}
	before, err := service.store.LoadPendingFeeds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := before["other/1m"]; exists {
		t.Fatal("unrelated public feeds polluted account state")
	}
	if err := hub.ObservePublicFeed(ctx, PendingFeedObservation{Feed: "17/1m", EndMS: 13, Closed: true}); err != nil {
		t.Fatal(err)
	}
	after, err := service.store.LoadPendingFeeds(ctx)
	if err != nil || after["17/1m"] != before["17/1m"] {
		t.Fatal("duplicate public fact advanced clock", after, err)
	}
}

func TestHubPublicFeedRejectsConflictingAccountInstrumentIdentity(t *testing.T) {
	robot, _ := strategyService(t, true)
	ctx := context.Background()
	first := PendingDefinition{Order: *pendingTestOrder(robot.service.store, "a"), Instrument: ledgerInstrument()}
	second := PendingDefinition{Order: *pendingTestOrder(robot.service.store, "b"), Instrument: ledgerInstrument()}
	second.Instrument.ID = "ETH"
	second.Order.Entry.Instrument = "ETH"
	first.Order.EntryFeedExpiry = &PendingFeedExpiry{Feed: "17/1m", ExpiresAtBar: 1}
	second.Order.EntryFeedExpiry = &PendingFeedExpiry{Feed: "17/1m", ExpiresAtBar: 1}
	saveTestPendingBook(t, robot.service.store, []PendingDefinition{first, second})
	hub, err := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{Account: robot.service, Shutdown: func() error { return nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	if err := hub.Register(ctx, HubAccountRegistration{Account: robot.AccountKey(), Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if feeds, err := hub.PendingFeeds(ctx); err == nil || len(feeds) != 0 {
		t.Fatal("feed-only dedup hid a conflicting instrument identity", feeds, err)
	}
}

func TestHubPendingFeedAdmissionPrecedesCommitButNotReceiptReplay(t *testing.T) {
	ctx := context.Background()
	hub, account, lease := policyCancelHub(t, true)
	command := pendingCreate("feed-admission", "a")
	command.StrategyEpoch, command.StrategySequence = lease.Epoch, 1
	command.Create.Feed, command.Create.StopBars = "custom-feed", 1
	if _, err := hub.ApplyPendingCommand(ctx, account.AccountKey(), command); err == nil {
		t.Fatal("unconfigured feed host accepted an unobservable deadline")
	}
	unavailable := errors.New("public feed not ready")
	calls := 0
	hub.SetPendingFeedAdmission(func(context.Context, HubPendingFeed) error { calls++; return unavailable })
	if _, err := hub.ApplyPendingCommand(ctx, account.AccountKey(), command); !errors.Is(err, unavailable) {
		t.Fatal(err)
	}
	book, err := account.PendingOrders(ctx)
	if err != nil || len(book) != 0 {
		t.Fatal("failed feed admission committed an order", book, err)
	}
	if _, err := account.service.store.StrategyCheckpoint(ctx, "a", "pending-command:"+command.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("failed feed admission committed a receipt", err)
	}
	var publicReadyAt int64
	hub.SetPendingFeedAdmission(func(_ context.Context, feed HubPendingFeed) error {
		calls++
		time.Sleep(3 * time.Millisecond)
		publicReadyAt = time.Now().UnixMilli()
		if feed.Account != account.AccountKey() || feed.Instrument != "BTC" || feed.Feed != "custom-feed" {
			t.Fatal("host validator received a different feed", feed)
		}
		return nil
	})
	if revision, err := hub.ApplyPendingCommand(ctx, account.AccountKey(), command); err != nil || revision != 1 {
		t.Fatal(revision, err)
	}
	book, err = account.PendingOrders(ctx)
	if err != nil || len(book) != 1 || book[0].Order.EntryFeedExpiry == nil || book[0].Order.EntryFeedExpiry.NotBeforeEndMS < publicReadyAt {
		t.Fatal("feed deadline used the pre-pin request clock", book, publicReadyAt, err)
	}
	callsBefore := calls
	hub.SetPendingFeedAdmission(func(context.Context, HubPendingFeed) error { calls++; return unavailable })
	if revision, err := hub.ApplyPendingCommand(ctx, account.AccountKey(), command); err != nil || revision != 1 {
		t.Fatal("accepted receipt depended on host availability", revision, err)
	}
	if calls != callsBefore {
		t.Fatal("lost ACK re-entered first-admission validator")
	}
	command.Create.QuantitySteps++
	if _, err := hub.ApplyPendingCommand(ctx, account.AccountKey(), command); err == nil {
		t.Fatal("mismatching retry bypassed exact command identity")
	}
}

func TestLocalPendingCustomFeedDoesNotRequireHubPublicGrammar(t *testing.T) {
	account, _ := strategyService(t, true)
	command := pendingCreate("trusted-custom-feed", "a")
	command.Create.Feed, command.Create.StopBars = "custom-domain-clock", 2
	if revision, err := account.ApplyPendingCommand(context.Background(), command, 12); err != nil || revision != 1 {
		t.Fatal("hub public grammar narrowed the local contract", revision, err)
	}
}

func TestHubPendingFeedAdmissionDoesNotLockAccountOrLoseConcurrentReceipt(t *testing.T) {
	ctx := context.Background()
	hub, account, lease := policyCancelHub(t, true)
	command := pendingCreate("concurrent-feed-ack", "a")
	command.StrategyEpoch, command.StrategySequence = lease.Epoch, 1
	command.Create.Feed, command.Create.StopBars = "trusted-clock", 2
	entered, release := make(chan struct{}), make(chan struct{})
	unavailable := errors.New("public source unavailable during concurrent retry")
	hub.SetPendingFeedAdmission(func(context.Context, HubPendingFeed) error { close(entered); <-release; return unavailable })
	type result struct {
		revision int64
		err      error
	}
	completed := make(chan result, 1)
	go func() {
		revision, err := hub.ApplyPendingCommand(ctx, account.AccountKey(), command)
		completed <- result{revision, err}
	}()
	<-entered
	// A trusted in-process writer commits the same fact while the public host
	// is unavailable. Neither this acceptance nor protection queries may wait
	// for the host admission network operation to finish.
	localDone := make(chan result, 1)
	go func() {
		revision, err := account.ApplyPendingCommand(ctx, command, time.Now().UnixMilli())
		localDone <- result{revision, err}
	}()
	select {
	case local := <-localDone:
		if local.err != nil || local.revision != 1 {
			close(release)
			<-completed
			t.Fatal(local)
		}
	case <-time.After(time.Second):
		close(release)
		<-completed
		t.Fatal("public feed admission held the account lock")
	}
	close(release)
	remote := <-completed
	if remote.err != nil || remote.revision != 1 {
		t.Fatal("concurrent durable receipt was lost behind admission failure", remote)
	}
}
