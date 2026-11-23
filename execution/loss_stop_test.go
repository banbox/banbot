package execution

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/shopspring/decimal"
)

func TestLossStopPolicyValidation(t *testing.T) {
	rate := decimal.RequireFromString("0.1")
	for name, policy := range map[string]LossStopPolicy{
		"pause without window": {PauseMS: 1},
		"no pause":             {Windows: []LossStopWindow{{WindowMS: 60_000, MaxLossRate: rate}}},
		"window below minute":  {Windows: []LossStopWindow{{WindowMS: 59_999, MaxLossRate: rate}}, PauseMS: 1},
		"zero rate":            {Windows: []LossStopWindow{{WindowMS: 60_000}}, PauseMS: 1},
		"rate above one":       {Windows: []LossStopWindow{{WindowMS: 60_000, MaxLossRate: decimal.NewFromInt(2)}}, PauseMS: 1},
		"duplicate window":     {Windows: []LossStopWindow{{WindowMS: 60_000, MaxLossRate: rate}, {WindowMS: 60_000, MaxLossRate: rate}}, PauseMS: 1},
	} {
		if policy.validate() == nil {
			t.Fatalf("%s must be rejected", name)
		}
	}
	if err := (LossStopPolicy{}).validate(); err != nil {
		t.Fatalf("empty policy disables the breaker: %v", err)
	}
}

func TestLossStopObserveWindows(t *testing.T) {
	minute := int64(60_000)
	policy := LossStopPolicy{PauseMS: 8 * 60 * minute, Windows: []LossStopWindow{
		{WindowMS: 30 * minute, MaxLossRate: decimal.RequireFromString("0.3")},
		{WindowMS: 180 * minute, MaxLossRate: decimal.RequireFromString("0.2")},
	}}
	eq := func(v int64) decimal.Decimal { return decimal.NewFromInt(v) }
	t0 := int64(1_700_000_000_000)

	t.Run("slow loss trips the long window only", func(t *testing.T) {
		state := lossStopState{LossStopStatus: LossStopStatus{Policy: policy}}
		// Lose 1.1% of the start equity every 10 minutes: never 30% in 30
		// minutes, and 20% only once a full 180 minutes have passed.
		for i := int64(0); i <= 190; i += 10 {
			state.observe(t0+i*minute, eq(1000-i*11/10))
			if state.PausedUntilMS != 0 && i < 180 {
				t.Fatalf("tripped after %d minutes with %v", i, state.TrippedLossRate)
			}
		}
		if state.PausedUntilMS == 0 || state.TrippedWindowMS != 180*minute {
			t.Fatalf("expected the 180m window to trip, got %+v", state.LossStopStatus)
		}
	})

	t.Run("fast loss trips the short window and pauses", func(t *testing.T) {
		state := lossStopState{LossStopStatus: LossStopStatus{Policy: policy}}
		state.observe(t0, eq(1000))
		state.observe(t0+10*minute, eq(1000))
		if !state.observe(t0+20*minute, eq(690)) || state.TrippedWindowMS != 30*minute {
			t.Fatalf("31%% in 20 minutes must trip the 30m window: %+v", state.LossStopStatus)
		}
		until := state.PausedUntilMS
		if until != t0+20*minute+policy.PauseMS {
			t.Fatalf("pause until %d", until)
		}
		// While paused, further losses do not extend the pause.
		state.observe(t0+30*minute, eq(100))
		if state.PausedUntilMS != until {
			t.Fatal("an active pause must not be extended")
		}
	})

	t.Run("a gap in observations shrinks the window instead of widening it", func(t *testing.T) {
		state := lossStopState{LossStopStatus: LossStopStatus{Policy: policy}}
		state.observe(t0, eq(1000))
		// Two days unobserved: the only older sample is far outside every window.
		later := t0 + 2*24*60*minute
		state.observe(later, eq(500))
		if state.PausedUntilMS != 0 {
			t.Fatalf("a stale anchor must not be compared: %+v", state.LossStopStatus)
		}
		state.observe(later+5*minute, eq(340))
		if state.PausedUntilMS == 0 || state.TrippedWindowMS != 30*minute {
			t.Fatalf("loss since the first in-window sample must count: %+v", state.LossStopStatus)
		}
	})

	t.Run("history stays bounded", func(t *testing.T) {
		state := lossStopState{LossStopStatus: LossStopStatus{Policy: policy}}
		for i := int64(0); i < 5000; i++ {
			state.observe(t0+i*minute, eq(1000))
		}
		if len(state.Samples) > 182 {
			t.Fatalf("kept %d samples for a 180 minute window", len(state.Samples))
		}
	})
}

func lossStopService(t *testing.T, memory bool, price *decimal.Decimal, quoteErr *error) *SharedAccountBorrow {
	t.Helper()
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewPaperAdapter(intentPrice("1000"), intentPrice("0"), intentPrice("0"))
	if err != nil {
		t.Fatal(err)
	}
	opts := SharedExecutionOptions{Memory: memory, Adapter: adapter, AuthoritativeSnapshot: true}
	if !memory {
		opts.StorePath = filepath.Join(t.TempDir(), "execution.db")
		opts.SenderLeaseDir = t.TempDir()
	}
	service, err := NewSharedAccount(owner, opts)
	if err != nil {
		t.Fatal(err)
	}
	borrow := service.Borrow()
	t.Cleanup(func() { borrow.Release(); registry.Close(); service.Close() })
	fundStrategies(t, service.store)
	if err := borrow.Reconcile("ready", 0); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterRiskPolicy("v1", domainRisk()); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		if *quoteErr != nil {
			return VisibleQuote{}, *quoteErr
		}
		return VisibleQuote{Bid: *price, Ask: *price, AtMS: now, ReceivedMS: now, ValidUntilMS: now + 1000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	return borrow
}

func TestLossStopCountsFloatingLossAndPersists(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "SQLite"
		if memory {
			name = "MemoryStore"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			minute := int64(60_000)
			t0 := int64(1_700_000_000_000)
			price := intentPrice("100")
			var quoteErr error
			borrow := lossStopService(t, memory, &price, &quoteErr)

			if status, err := borrow.LossStopStatus(ctx, t0); err != nil || status.PausedUntilMS != 0 || len(status.Policy.Windows) != 0 {
				t.Fatalf("no policy must report a disabled breaker: %+v %v", status, err)
			}
			policy := LossStopPolicy{PauseMS: 60 * minute, Windows: []LossStopWindow{{WindowMS: 10 * minute, MaxLossRate: decimal.RequireFromString("0.05")}}}
			if err := borrow.SetLossStopPolicy(ctx, policy); err != nil {
				t.Fatal(err)
			}
			if _, err := borrow.LossStopStatus(ctx, t0); err != nil {
				t.Fatal(err)
			}
			// Open 4 BTC at 100 for strategy a: no realized result at all afterwards.
			request := StrategyRebalance{Strategy: "a", PlanID: "open", DecisionMS: t0 + 1, ExpiresMS: t0 + 1000, Mode: StrategyTargetsFull, Requests: []InstrumentRebalance{{
				Instrument: ledgerInstrument(), Targets: []ExecutableTarget{{Strategy: "a", Lot: "lot", SignedSteps: 40}},
				Quote: VisibleQuote{Bid: price, Ask: price, AtMS: t0 + 1, ReceivedMS: t0 + 1, ValidUntilMS: t0 + 1000, Bar: t0 + 1},
			}}}
			if err := borrow.RebalanceStrategy(request, t0+2); err != nil {
				t.Fatal(err)
			}
			snapshot, err := borrow.Snapshot(ctx)
			if err != nil || len(snapshot.ActualPositions) != 1 || snapshot.ActualPositions[0].SignedSteps != 40 {
				t.Fatalf("position not opened: %+v %v", snapshot.ActualPositions, err)
			}

			// A 4% floating loss stays below the limit.
			price = intentPrice("90")
			status, err := borrow.LossStopStatus(ctx, t0+2*minute)
			if err != nil || status.PausedUntilMS != 0 {
				t.Fatalf("4%% loss must not trip: %+v %v", status, err)
			}
			// An unavailable mark neither trips nor fails.
			price, quoteErr = intentPrice("1"), errors.New("quote down")
			status, err = borrow.LossStopStatus(ctx, t0+3*minute)
			if err != nil || status.PausedUntilMS != 0 {
				t.Fatalf("missing mark must be skipped: %+v %v", status, err)
			}
			// An 8% loss that is entirely unrealized trips the breaker.
			price, quoteErr = intentPrice("80"), nil
			status, err = borrow.LossStopStatus(ctx, t0+4*minute)
			if err != nil || status.PausedUntilMS != t0+4*minute+policy.PauseMS || status.TrippedWindowMS != 10*minute {
				t.Fatalf("8%% floating loss must pause entries: %+v %v", status, err)
			}
			if !status.TrippedLossRate.Equal(decimal.RequireFromString("0.08")) {
				t.Fatalf("loss rate %s, want 0.08", status.TrippedLossRate)
			}
			// The pause is durable owner state, also after a recovery in price.
			price = intentPrice("100")
			again, err := borrow.LossStopStatus(ctx, t0+5*minute)
			if err != nil || again.PausedUntilMS != status.PausedUntilMS {
				t.Fatalf("pause must persist: %+v %v", again, err)
			}
			// Replacing the policy keeps the active pause.
			policy.Windows[0].MaxLossRate = decimal.RequireFromString("0.5")
			if err := borrow.SetLossStopPolicy(ctx, policy); err != nil {
				t.Fatal(err)
			}
			again, err = borrow.LossStopStatus(ctx, t0+6*minute)
			if err != nil || again.PausedUntilMS != status.PausedUntilMS || !again.Policy.Windows[0].MaxLossRate.Equal(policy.Windows[0].MaxLossRate) {
				t.Fatalf("policy change lost the pause: %+v %v", again, err)
			}
		})
	}
}

func TestHubLossStopRPCRoundTrip(t *testing.T) {
	var closed atomic.Int32
	hub, err := NewHub(func(_ context.Context, registration HubAccountRegistration) (HubAccountBinding, error) {
		return hubTestBinding(t, registration.Account, &closed), nil
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
	client := NewHubClient(server.path)
	defer client.Close()
	registration := hubTestRegistration()
	ctx := context.Background()
	if err := client.Register(ctx, registration); err != nil {
		t.Fatal(err)
	}
	if status, err := client.LossStopStatus(ctx, registration.Account); err != nil || len(status.Policy.Windows) != 0 || status.PausedUntilMS != 0 {
		t.Fatalf("fresh account: %+v %v", status, err)
	}
	if err := client.SetLossStopPolicy(ctx, registration.Account, LossStopPolicy{PauseMS: 1}); err == nil {
		t.Fatal("invalid policy accepted over RPC")
	}
	policy := LossStopPolicy{PauseMS: 3600_000, Windows: []LossStopWindow{{WindowMS: 30 * 60_000, MaxLossRate: decimal.RequireFromString("0.25")}}}
	if err := client.SetLossStopPolicy(ctx, registration.Account, policy); err != nil {
		t.Fatal(err)
	}
	status, err := client.LossStopStatus(ctx, registration.Account)
	if err != nil || !status.Policy.equal(policy) || status.PausedUntilMS != 0 {
		t.Fatalf("installed policy not returned: %+v %v", status, err)
	}
	// Disabling the breaker is an explicit empty policy.
	if err := client.SetLossStopPolicy(ctx, registration.Account, LossStopPolicy{}); err != nil {
		t.Fatal(err)
	}
	if status, err = client.LossStopStatus(ctx, registration.Account); err != nil || len(status.Policy.Windows) != 0 {
		t.Fatalf("breaker not disabled: %+v %v", status, err)
	}
}
