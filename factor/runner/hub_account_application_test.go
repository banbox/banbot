package runner

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/shopspring/decimal"
)

func factorHubSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "factor-hub-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "hub.sock")
}

// The upstream finishes acceptance; the first response is lost before the
// robot can read it. Subsequent requests use the same real Unix RPC contract.
func factorHubLostACKProxy(t *testing.T, upstream string) string {
	t.Helper()
	path := factorHubSocket(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", upstream)
	}}
	var dropped atomic.Bool
	var droppedTarget atomic.Bool
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := r.Clone(r.Context())
		request.RequestURI = ""
		request.URL.Scheme, request.URL.Host = "http", "hub"
		response, err := transport.RoundTrip(request)
		if err != nil {
			http.Error(w, "upstream failed", http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		if r.URL.Path == "/v1/policy-accept" && dropped.CompareAndSwap(false, true) || r.URL.Path == "/v1/rebalance" && droppedTarget.CompareAndSwap(false, true) {
			_, _ = io.Copy(io.Discard, response.Body)
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	})}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close(); <-done; transport.CloseIdleConnections() })
	return path
}

func TestHubAccountSinkPolicyLostACKRestorePatchAndNetting(t *testing.T) {
	ctx := context.Background()
	c := paperConfig(t, archiveConfig(t, false))
	key := execution.AccountKey{VenueSessionIdentity: "factor-hub", Account: c.AccountID, SettlementDomain: c.Manifest.Currency}
	venue, err := execution.NewPaperAdapter(decimal.NewFromInt(10000), decimal.Zero, decimal.Zero)
	if err != nil {
		t.Fatal(err)
	}
	hub, err := execution.NewHub(func(context.Context, execution.HubAccountRegistration) (execution.HubAccountBinding, error) {
		registry := &execution.AccountRegistry{}
		owner, err := registry.Acquire(key)
		if err != nil {
			return execution.HubAccountBinding{}, err
		}
		service, err := execution.NewSharedAccount(owner, execution.SharedExecutionOptions{Memory: true, Adapter: venue, AuthoritativeSnapshot: true})
		if err != nil {
			registry.Close()
			return execution.HubAccountBinding{}, err
		}
		borrow := service.Borrow()
		defer borrow.Release()
		capital := map[execution.StrategyID]decimal.Decimal{execution.StrategyID(c.StrategyID): decimal.NewFromInt(5000), "other": decimal.NewFromInt(5000)}
		if err := borrow.BootstrapCapital(ctx, capital, time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		if err := borrow.Reconcile("startup", time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		if err := borrow.RegisterRiskPolicy("hub-v1", execution.PortfolioRisk{MarginRate: decimal.RequireFromString("0.1"), MaxAccountMargin: decimal.NewFromInt(10000), MaxVirtualGross: decimal.NewFromInt(20000), StrategyGrossLimits: capital}); err != nil {
			t.Fatal(err)
		}
		var instruments []execution.Instrument
		for _, instrument := range c.Execution.Instruments {
			instruments = append(instruments, instrument)
		}
		if err := borrow.RegisterAccountQuotes(instruments, func(_ context.Context, _ string, now int64) (execution.VisibleQuote, error) {
			return execution.VisibleQuote{Bid: decimal.NewFromInt(100), Ask: decimal.NewFromInt(100), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 60000, Bar: now}, nil
		}, nil); err != nil {
			t.Fatal(err)
		}
		return execution.HubAccountBinding{Account: service, Shutdown: func() error { registry.Close(); return service.Close() }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	path := factorHubSocket(t)
	server, err := execution.ListenHubUnix(path, hub)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := execution.NewHubClient(factorHubLostACKProxy(t, path))
	defer client.Close()
	if err := client.Register(ctx, execution.HubAccountRegistration{Account: key, Env: "dry_run", Configuration: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	sink, err := NewHubAccountSink(ctx, client, key, execution.StrategyWriterClaim{Strategy: execution.StrategyID(c.StrategyID), WriterID: "robot-one"}, c)
	if err != nil {
		t.Fatal(err)
	}
	if sink.Account != nil || sink.Paper != nil || sink.Application == nil {
		t.Fatal("remote sink retained a process-owned sender")
	}
	if err := sink.RegisterExecution(); err != nil {
		t.Fatal(err)
	}
	makeProposal := func(sink *AccountSink, seq uint64, mode factor.PortfolioMode, allocations map[int32]factor.Allocation) (factor.PortfolioProposal, map[int32]backtest.Quote, int64) {
		t.Helper()
		now := time.Now().UnixMilli()
		sp := patchPortfolio(t, seq, mode, 5000, nil).Spec()
		sp.AccountID, sp.StrategyID = sink.AccountID, sink.StrategyID
		sp.DecisionTime, sp.ExecutableAt, sp.ExpireAt = now-100, now-50, now+60000
		sp.Budget.Currency = sink.Currency
		target, err := factor.NewPortfolioTarget(sp, allocations)
		if err != nil {
			t.Fatal(err)
		}
		quotes := map[int32]backtest.Quote{}
		for sid := range allocations {
			quotes[sid] = backtest.Quote{AtMS: now, AvailableAt: now, Price: 1, Bid: 1, Ask: 1}
		}
		return factor.PortfolioProposal{Target: target, NextState: json.RawMessage(`{"step":1}`), PlanSequence: seq}, quotes, now
	}
	evidence, err := sink.PolicyEvidence(ctx, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	proposal, quotes, now := makeProposal(sink, 1, factor.Full, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "4"}})
	if _, err := sink.AcceptProposal(ctx, proposal, evidence.StateVersion, evidence.LedgerCursor, quotes, now); err == nil {
		t.Fatal("proxy did not lose the committed ACK")
	}
	fills := venue.Metrics().Fills
	if fills != 1 {
		t.Fatal("policy and state were not committed before lost ACK", venue.Metrics())
	}
	if receipt, err := sink.AcceptProposal(ctx, proposal, evidence.StateVersion, evidence.LedgerCursor, quotes, now); err != nil || !receipt.Accepted || receipt.SendError != nil || venue.Metrics().Fills != fills || sink.previousAllocation == nil {
		t.Fatal("resume did not restore target scope without resending", receipt, err)
	}
	proposal, quotes, now = makeProposal(sink, 2, factor.Patch, map[int32]factor.Allocation{2: {Basis: factor.AbsoluteQuantity, Value: "2"}})
	evidence, err = sink.PolicyEvidence(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := sink.AcceptProposal(ctx, proposal, evidence.StateVersion, evidence.LedgerCursor, quotes, now); err != nil || !receipt.Accepted || receipt.SendError != nil {
		t.Fatal("remote patch", receipt, err)
	}
	state, err := sink.StrategyState(ctx, time.Now().UnixMilli())
	if err != nil || state.Quantities[1] != 4 || state.Quantities[2] != 2 || state.NAV != 5000 {
		t.Fatal("remote account state lost quantity/marks", state, err)
	}
	otherConfig := c
	otherConfig.StrategyID = "other"
	other, err := NewHubAccountSink(ctx, client, key, execution.StrategyWriterClaim{Strategy: "other", WriterID: "robot-two"}, otherConfig)
	if err != nil {
		t.Fatal(err)
	}
	proposal, quotes, now = makeProposal(other, 1, factor.Full, map[int32]factor.Allocation{1: {Basis: factor.AbsoluteQuantity, Value: "-2"}})
	evidence, err = other.PolicyEvidence(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := other.AcceptProposal(ctx, proposal, evidence.StateVersion, evidence.LedgerCursor, quotes, now); err != nil || !receipt.Accepted || receipt.SendError != nil {
		t.Fatal("second robot", receipt, err)
	}
	snapshot, err := client.Snapshot(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, position := range snapshot.ActualPositions {
		if position.Instrument.ID == c.Execution.Instruments[1].ID && !decimal.NewFromInt(position.SignedSteps).Mul(position.Instrument.QuantityStep).Mul(position.Instrument.ContractSize).Equal(decimal.NewFromInt(2)) {
			t.Fatal("hub did not net independent robot positions", snapshot.ActualPositions)
		}
	}
	// A fresh robot session restores checkpoint and full/patch membership; it
	// never obtains the in-process owner or quote callback.
	restored, err := NewHubAccountSink(ctx, client, key, execution.StrategyWriterClaim{Strategy: execution.StrategyID(c.StrategyID), WriterID: "robot-restarted", ExpectedEpoch: 1}, c)
	if err != nil {
		t.Fatal(err)
	}
	raw, version, sequence, err := restored.RestorePolicy(ctx, "")
	if err != nil || version != 2 || sequence != 2 || string(raw) != `{"step":1}` || restored.previousAllocation.Allocations()[1].Value != "4" {
		t.Fatal("remote checkpoint restoration", string(raw), version, sequence, err)
	}
	evidence, err = restored.PolicyEvidence(ctx, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	now = time.Now().UnixMilli()
	stateOnly := factor.PortfolioProposal{AcceptanceID: "remote-state-only", NextState: json.RawMessage(`{"step":3}`), PlanSequence: 3, DecisionTime: now, ExpireAt: now + 60000}
	fills = venue.Metrics().Fills
	if receipt, err := restored.AcceptProposal(ctx, stateOnly, evidence.StateVersion, evidence.LedgerCursor, nil, now); err != nil || !receipt.Accepted || venue.Metrics().Fills != fills {
		t.Fatal("remote state-only policy traded", receipt, err)
	}
	if err := restored.ObserveFunding(ctx, backtest.Funding{}, now); err == nil {
		t.Fatal("robot injected hub-owned funding")
	}
	if err := sink.CancelIncreasingPending(ctx, []int32{1}, now); err == nil {
		t.Fatal("fenced old robot canceled entries")
	}
	if err := restored.CancelIncreasingPending(ctx, []int32{1}, now); err != nil {
		t.Fatal("remote cancellation contract", err)
	}
	// Plain weight strategies use the same durable target recovery contract,
	// rather than recomputing a lost-ACK command using newer prices.
	now = time.Now().UnixMilli()
	sp := patchPortfolio(t, 1, factor.Full, 5000, nil).Spec()
	sp.AccountID, sp.StrategyID, sp.Budget.Currency = restored.AccountID, restored.StrategyID, restored.Currency
	sp.DecisionTime, sp.ExecutableAt, sp.ExpireAt = now-100, now-50, now+60000
	plain, err := factor.NewTargetPortfolio(sp, map[int32]float64{1: .1})
	if err != nil {
		t.Fatal(err)
	}
	quotes = map[int32]backtest.Quote{1: {AtMS: now, AvailableAt: now, Price: 1, Bid: 1, Ask: 1}}
	if err := restored.ProcessSnapshot(ctx, plain, quotes, now); err == nil {
		t.Fatal("plain target ACK was not lost")
	}
	fills = venue.Metrics().Fills
	if err := restored.ProcessSnapshot(ctx, plain, nil, sp.ExpireAt+1); err != nil || venue.Metrics().Fills != fills {
		t.Fatal("accepted plain target was resized or rejected on retry", err, fills, venue.Metrics())
	}
	plainRestarted, err := NewHubAccountSink(ctx, client, key, execution.StrategyWriterClaim{Strategy: execution.StrategyID(c.StrategyID), WriterID: "weights-restarted", ExpectedEpoch: 2}, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := plainRestarted.ProcessSnapshot(ctx, plain, nil, sp.ExpireAt+1); err != nil || venue.Metrics().Fills != fills || plainRestarted.previous == nil {
		t.Fatal("restarted weight robot did not recover accepted command", err)
	}
	now = time.Now().UnixMilli()
	sp.PlanSequence, sp.Mode, sp.DecisionTime, sp.ExecutableAt, sp.ExpireAt = 2, factor.Patch, now-100, now-50, now+60000
	patch, err := factor.NewTargetPortfolio(sp, map[int32]float64{2: .02})
	if err != nil {
		t.Fatal(err)
	}
	quotes = map[int32]backtest.Quote{2: {AtMS: now, AvailableAt: now, Price: 1, Bid: 1, Ask: 1}}
	if err := plainRestarted.ProcessSnapshot(ctx, patch, quotes, now); err != nil {
		t.Fatal("restarted weights Patch", err)
	}
	state, err = plainRestarted.StrategyState(ctx, time.Now().UnixMilli())
	if err != nil || state.Quantities[1] != 5 || state.Quantities[2] != 1 || plainRestarted.previous.Targets()[1] != .1 {
		t.Fatal("weight source checkpoint lost full scope", state, err)
	}
	fills = venue.Metrics().Fills
	if err := plainRestarted.ProcessSnapshot(ctx, plain, nil, sp.ExpireAt+1); err != nil || venue.Metrics().Fills != fills || plainRestarted.previous.Spec().PlanSequence != 2 {
		t.Fatal("historical weight ACK restored stale scope", err)
	}
}
