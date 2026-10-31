package execution

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type hubPolicyRoundTrip func(*http.Request) (*http.Response, error)

func (f hubPolicyRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHubPolicyAtomicLostACKRestartAndEvidence(t *testing.T) {
	ctx := context.Background()
	venue, err := NewPaperAdapter(intentPrice("1000"), intentPrice("0"), intentPrice("0"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	key := testIntent(Buy).Account
	var quotes atomic.Int64
	factory := func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		registry := &AccountRegistry{}
		owner, err := registry.Acquire(key)
		if err != nil {
			return HubAccountBinding{}, err
		}
		service, err := NewSharedAccount(owner, SharedExecutionOptions{StorePath: filepath.Join(root, "ledger.db"), SenderLeaseDir: root, Adapter: venue, AuthoritativeSnapshot: true})
		if err != nil {
			registry.Close()
			return HubAccountBinding{}, err
		}
		fundStrategies(t, service.store)
		borrow := service.Borrow()
		defer borrow.Release()
		if err := borrow.RecoverPersisted(ctx); err != nil {
			t.Fatal(err)
		}
		if err := borrow.Reconcile("startup", time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		if err := borrow.RegisterRiskPolicy("v1", domainRisk()); err != nil {
			t.Fatal(err)
		}
		if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
			quotes.Add(1)
			return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000, Bar: now}, nil
		}, nil); err != nil {
			t.Fatal(err)
		}
		return HubAccountBinding{Account: service, Shutdown: func() error { registry.Close(); return service.Close() }}, nil
	}
	start := func() (*HubClient, *HubServer) {
		t.Helper()
		hub, err := NewHub(factory)
		if err != nil {
			t.Fatal(err)
		}
		path := hubTestSocket(t)
		server, err := ListenHubUnix(path, hub)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = server.Close() })
		client := NewHubClient(path)
		t.Cleanup(client.Close)
		if err := client.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		return client, server
	}
	client, server := start()
	lease, err := client.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "a", WriterID: "factor-bot"})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := client.PolicyEvidence(ctx, key, "a")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	update := strategyRequest("a", "remote-policy", now, StrategyTargetsFull, ExecutableTarget{Lot: "a", SignedSteps: 2})
	update.StrategyEpoch, update.StrategySequence = lease.Epoch, 1
	update.ExpiresMS = now + 60000
	// The proposal cannot authorize its own executable price.
	update.Requests[0].Quote.Bid, update.Requests[0].Quote.Ask = intentPrice("1"), intentPrice("1")
	request := PolicyAcceptance{ID: update.PlanID, ExpectedLedgerCursor: evidence.Snapshot.Checkpoint, DecisionMS: now, ExpiresMS: now + 60000, Updates: []StrategyRebalance{update}, Checkpoints: []PolicyCheckpoint{{Strategy: "a", Payload: json.RawMessage(`{"step":1}`), ProposalHash: strings.Repeat("a", 64), PlanSequence: 1}}}
	client.client.Transport = hubPolicyRoundTrip(func(r *http.Request) (*http.Response, error) {
		response, err := client.transport.RoundTrip(r)
		if err == nil {
			_ = response.Body.Close()
		}
		return nil, errors.New("lost response after commit")
	})
	if _, err := client.AcceptPolicyBatch(ctx, key, request); err == nil {
		t.Fatal("lost ACK did not fail the client")
	}
	client.client.Transport = client.transport
	metrics := venue.Metrics()
	if metrics.Fills != 1 {
		t.Fatal("lost ACK request not committed", metrics)
	}
	quoteReads := quotes.Load()
	for range 2 {
		receipt, err := client.AcceptPolicyBatch(ctx, key, request)
		if err != nil || !receipt.Accepted || receipt.SendError != nil || receipt.Versions["a"] != 1 {
			t.Fatal("lost ACK retry", receipt, err)
		}
	}
	if quotes.Load() != quoteReads || venue.Metrics() != metrics {
		t.Fatal("retry refreshed frozen quotes or sent again", quotes.Load(), quoteReads, venue.Metrics(), metrics)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	client, server = start()
	evidence, err = client.PolicyEvidence(ctx, key, "a")
	if err != nil || evidence.State.Version != 1 || string(evidence.State.Payload) != `{"step":1}` || len(evidence.Snapshot.Lots) != 1 || evidence.Snapshot.Lots[0].SignedSteps != 2 || !evidence.Snapshot.Lots[0].CostBasis.Equal(ledgerInstrument().Notional(2, intentPrice("100"))) {
		t.Fatal("restart split checkpoint and targets", evidence, err)
	}
	metrics = venue.Metrics()
	receipt, err := client.AcceptPolicyBatch(ctx, key, request)
	if err != nil || !receipt.Accepted || receipt.Versions["a"] != 1 || venue.Metrics() != metrics {
		t.Fatal("restart replay", receipt, err)
	}
	resume := HubPolicyResume{Strategy: "a", ID: request.ID, ProposalHash: request.Checkpoints[0].ProposalHash}
	if receipt, found, err := client.ResumePolicy(ctx, key, resume); err != nil || !found || !receipt.Accepted || venue.Metrics() != metrics {
		t.Fatal("resume after restart", receipt, found, err)
	}
	request.Checkpoints[0].Payload = json.RawMessage(`{"step":2}`)
	if _, err := client.AcceptPolicyBatch(ctx, key, request); err == nil {
		t.Fatal("changed state accepted under same command identity")
	}
	request.ID = "stale-evidence"
	request.Updates = nil
	request.Checkpoints[0].StrategyEpoch, request.Checkpoints[0].StrategySequence = lease.Epoch, 2
	request.ExpectedLedgerCursor = evidence.Snapshot.Checkpoint
	if _, err := client.AcceptPolicyBatch(ctx, key, request); !errors.Is(err, ErrPolicyEvidenceChanged) {
		t.Fatal("RPC lost evidence conflict classification", err)
	}
	evidence, err = client.PolicyEvidence(ctx, key, "a")
	if err != nil || evidence.State.Version != 1 || string(evidence.State.Payload) != `{"step":1}` {
		t.Fatal("rejected policy partially committed", evidence, err)
	}
	now = time.Now().UnixMilli()
	plain := strategyRequest("a", "durable-source-target", now, StrategyTargetsFull, ExecutableTarget{Lot: "a", SignedSteps: 3})
	plain.StrategyEpoch, plain.StrategySequence, plain.ExpiresMS = lease.Epoch, 2, now+60000
	plain.ProposalHash, plain.ProposalState = strings.Repeat("c", 64), json.RawMessage(`{"version":1,"scope":{"BTC":3}}`)
	if err := client.RebalanceStrategy(ctx, key, plain, 0); err != nil {
		t.Fatal("target source acceptance", err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	client, _ = start()
	metrics = venue.Metrics()
	query := HubTargetResume{Strategy: "a", ID: plain.PlanID, ProposalHash: plain.ProposalHash}
	if recovered, err := client.ResumeStrategyTarget(ctx, key, query); err != nil || !recovered.Found || string(recovered.State) != string(plain.ProposalState) || venue.Metrics() != metrics {
		t.Fatal("Hub restart lost frozen target source or resent", recovered, err)
	}
	query.ProposalHash = strings.Repeat("d", 64)
	if _, err := client.ResumeStrategyTarget(ctx, key, query); err == nil {
		t.Fatal("target resume accepted a changed proposal fingerprint")
	}
}

func TestHubPolicyBatchRejectsAtomicallyAndPreservesUnknownSend(t *testing.T) {
	ctx := context.Background()
	borrow, _ := strategyService(t, true)
	adapter := &fakeExecutionAdapter{submit: func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
		return SubmitReceipt{}, errors.New("private credential must not cross RPC")
	}}
	if err := borrow.WithState(func(service *SharedAccount) error {
		service.executor.Adapter = adapter
		market := service.markets["BTC"]
		market.quote = func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
			return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000, Bar: now}, nil
		}
		service.markets["BTC"] = market
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	hub, _ := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{Account: borrow.service, Shutdown: func() error { return nil }}, nil
	})
	path := hubTestSocket(t)
	server, err := ListenHubUnix(path, hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := NewHubClient(path)
	defer client.Close()
	key := borrow.service.AccountKey()
	if err := client.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	lease, err := client.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "a", WriterID: "batch-bot"})
	if err != nil {
		t.Fatal(err)
	}
	otherLease, err := client.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "b", WriterID: "other-bot"})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := client.PolicyEvidence(ctx, key, "a")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	update := strategyRequest("a", "unknown-batch", now, StrategyTargetsFull, ExecutableTarget{Lot: "a", SignedSteps: 2})
	update.ExpiresMS, update.StrategyEpoch, update.StrategySequence = now+60000, lease.Epoch, 1
	request := PolicyAcceptance{ID: update.PlanID, ExpectedLedgerCursor: evidence.Snapshot.Checkpoint, DecisionMS: now, ExpiresMS: now + 60000, Updates: []StrategyRebalance{update}, Checkpoints: []PolicyCheckpoint{
		{Strategy: "a", Payload: json.RawMessage(`{"step":1}`), ProposalHash: strings.Repeat("a", 64)},
		{Strategy: "b", ExpectedVersion: 1, Payload: json.RawMessage(`{"step":2}`), ProposalHash: strings.Repeat("b", 64), StrategyEpoch: otherLease.Epoch, StrategySequence: 1},
	}}
	if _, err := client.AcceptPolicyBatch(ctx, key, request); !errors.Is(err, ErrPolicyEvidenceChanged) {
		t.Fatal("partial batch version was accepted", err)
	}
	after, err := client.PolicyEvidence(ctx, key, "a")
	if err != nil || after.State.Version != 0 || len(after.Snapshot.Orders) != 0 || len(adapter.calls()) != 0 {
		t.Fatal("rejected batch partially committed", after, err)
	}
	request.Checkpoints[1].ExpectedVersion = 0
	receipt, err := client.AcceptPolicyBatch(ctx, key, request)
	if err != nil || !receipt.Accepted || receipt.Versions["a"] != 1 || receipt.Versions["b"] != 1 || receipt.SendError == nil || strings.Contains(receipt.SendError.Error(), "private credential") {
		t.Fatal("unknown send lost acceptance or leaked adapter error", receipt, err)
	}
	if receipt, err = client.AcceptPolicyBatch(ctx, key, request); err != nil || !receipt.Accepted || receipt.SendError == nil || len(adapter.calls()) != 1 {
		t.Fatal("retry resubmitted uncertain order", receipt, err, adapter.calls())
	}
	resume := HubPolicyResume{Strategy: "b", ID: request.ID, ProposalHash: request.Checkpoints[1].ProposalHash}
	if receipt, found, err := client.ResumePolicy(ctx, key, resume); err != nil || !found || !receipt.Accepted || receipt.SendError == nil || len(adapter.calls()) != 1 {
		t.Fatal("batch member could not recover shared acceptance", receipt, found, err)
	}
	resume.ID = "not-accepted"
	if receipt, found, err := client.ResumePolicy(ctx, key, resume); err != nil || found || receipt.Accepted {
		t.Fatal("missing acceptance invented a receipt", receipt, found, err)
	}
}

func TestHubPolicyStateOnlyFencesOldWriterAndReservedNames(t *testing.T) {
	ctx := context.Background()
	borrow, venue := strategyService(t, true)
	hub, _ := NewHub(func(context.Context, HubAccountRegistration) (HubAccountBinding, error) {
		return HubAccountBinding{Account: borrow.service, Shutdown: func() error { return nil }}, nil
	})
	defer hub.Close()
	key := borrow.service.AccountKey()
	if err := hub.Register(ctx, HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	lease, err := hub.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "a", WriterID: "old-bot"})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := hub.PolicyEvidence(ctx, key, "a")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	request := PolicyAcceptance{ID: "state-only", ExpectedLedgerCursor: evidence.Snapshot.Checkpoint, DecisionMS: now, ExpiresMS: now + 60000, Checkpoints: []PolicyCheckpoint{{Strategy: "a", Payload: json.RawMessage(`{"step":1}`), StrategyEpoch: lease.Epoch, StrategySequence: 1}}}
	newLease, err := hub.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "a", WriterID: "new-bot", ExpectedEpoch: lease.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.AcceptPolicyBatch(ctx, key, request); err == nil {
		t.Fatal("old writer changed policy state without a target")
	}
	request.Checkpoints[0].StrategyEpoch = newLease.Epoch
	receipt, err := hub.AcceptPolicyBatch(ctx, key, request)
	if err != nil || !receipt.Accepted || receipt.Versions["a"] != 1 || venue.Metrics().Fills != 0 {
		t.Fatal("state-only acceptance traded or lost receipt", receipt, err)
	}
	if receipt, err := hub.AcceptPolicyBatch(ctx, key, request); err != nil || !receipt.Accepted || receipt.Versions["a"] != 1 {
		t.Fatal("state-only retry lost original acceptance", receipt, err)
	}
	request.ID = "state-next"
	request.Checkpoints[0].ExpectedVersion = 1
	request.Checkpoints[0].Payload = json.RawMessage(`{"step":2}`)
	if _, err := hub.AcceptPolicyBatch(ctx, key, request); err == nil {
		t.Fatal("same writer sequence accepted a new command")
	}
	request.Checkpoints[0].StrategySequence = 2
	request.Checkpoints[0].Name = strategyWriterCheckpoint
	if _, err := hub.AcceptPolicyBatch(ctx, key, request); err == nil {
		t.Fatal("policy overwrote reserved writer checkpoint")
	}
	request.Checkpoints[0].Name = ""
	if receipt, err := hub.AcceptPolicyBatch(ctx, key, request); err != nil || !receipt.Accepted || receipt.Versions["a"] != 2 {
		t.Fatal("failed state update consumed writer highwater", receipt, err)
	}
}

func TestHubWriterLeaseReadOnlyRPC(t *testing.T) {
	hub, account, _ := policyCancelHub(t, true)
	server, err := ListenHubUnix(hubTestSocket(t), hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := NewHubClient(server.path)
	defer client.Close()
	ctx := context.Background()
	key := account.AccountKey()
	before, err := client.WriterLease(ctx, key, "a")
	if err != nil {
		t.Fatal(err)
	}
	again, err := client.WriterLease(ctx, key, "a")
	if err != nil || before != again {
		t.Fatalf("read mutated writer: %+v %+v %v", before, again, err)
	}
	lease, err := client.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "a", WriterID: "fresh-query-cas", ExpectedEpoch: before.Epoch})
	if err != nil {
		t.Fatal(err)
	}
	after, err := client.WriterLease(ctx, key, "a")
	if err != nil || after.Epoch != lease.Epoch || after.Sequence != 0 || after.Revision != before.Revision {
		t.Fatalf("lease %+v %v", after, err)
	}
	if _, err := client.ClaimStrategyWriter(ctx, key, StrategyWriterClaim{Strategy: "a", WriterID: "stale-query-cas", ExpectedEpoch: before.Epoch}); err == nil {
		t.Fatal("stale query allowed new writer")
	}
}
