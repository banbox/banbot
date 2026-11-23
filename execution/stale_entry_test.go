package execution

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func staleMetadata(timeframe string, createdMS int64) *TimeSeriesMetadata {
	return &TimeSeriesMetadata{OrderID: 7, StrategyName: "strat", Symbol: "BTC/USDT", TimeFrame: timeframe, EnterTag: "t", CreatedMS: createdMS, InitPrice: intentPrice("100")}
}

func staleCreate(id string, md *TimeSeriesMetadata) PendingCommand {
	c := pendingCreate(id, "a")
	c.Metadata = md
	return c
}

func wantStaleEntry(t *testing.T, err error) {
	t.Helper()
	if code, ok := RejectionCode(err); !ok || code != RejectStaleEntry {
		t.Fatalf("want typed %s, got %v", RejectStaleEntry, err)
	}
}

func TestStaleEntryWindowTable(t *testing.T) {
	const now = int64(10_000_000_000)
	min := time.Minute.Milliseconds()
	for name, tc := range map[string]struct {
		timeframe string
		age       int64
		stale     bool
	}{
		"1m fresh":                {"1m", 30_000, false},
		"1m just inside floor":    {"1m", 2*min - 1, false},
		"1m exactly floor":        {"1m", 2 * min, false},
		"1m just past floor":      {"1m", 2*min + 1, true},
		"5m inside 2x":            {"5m", 9 * min, false},
		"5m past 2x":              {"5m", 10*min + 1, true},
		"1h inside 2x":            {"1h", 119 * min, false},
		"1h past 2x":              {"1h", 121 * min, true},
		"ws uses floor, inside":   {"ws", 2 * min, false},
		"ws uses floor, past":     {"ws", 3 * min, true},
		"unparsable uses floor":   {"zz", 3 * min, true},
		"unparsable inside floor": {"zz", min, false},
		"future signal time":      {"1m", -5_000, false},
	} {
		if got := staleEntry(&TimeSeriesMetadata{TimeFrame: tc.timeframe, CreatedMS: now - tc.age}, now); got != tc.stale {
			t.Errorf("%s: stale=%v, want %v", name, got, tc.stale)
		}
	}
	if staleEntry(&TimeSeriesMetadata{TimeFrame: "1m", CreatedMS: 0}, now) {
		t.Error("a create without a signal time is not judged stale")
	}
}

func TestPendingCreateStaleEntryAdmission(t *testing.T) {
	for _, memory := range []bool{true, false} {
		t.Run(map[bool]string{true: "memory", false: "sqlite"}[memory], func(t *testing.T) {
			b, _ := strategyService(t, memory)
			ctx := context.Background()
			now := time.Now().UnixMilli()
			swapQuote(t, b, func(_ context.Context, _ string, at int64) (VisibleQuote, error) {
				return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: at, ReceivedMS: at, ValidUntilMS: at + 3_600_000, Bar: at}, nil
			})

			// A fresh create is accepted.
			fresh := staleCreate("fresh", staleMetadata("1m", now-30_000))
			if revision, err := b.ApplyPendingCommand(ctx, fresh, now); err != nil || revision != 1 {
				t.Fatal(revision, err)
			}
			// Without time-series metadata the create is unaffected, whatever the clock.
			plain := pendingCreate("plain", "b")
			if _, err := b.ApplyPendingCommand(ctx, plain, now); err != nil {
				t.Fatal("create without metadata refused:", err)
			}

			// A stale create is refused with the typed code and commits nothing.
			before, err := b.PendingOrders(ctx)
			if err != nil {
				t.Fatal(err)
			}
			beforeSnap, _ := b.Snapshot(ctx)
			stale := staleCreate("stale", staleMetadata("1m", now-3*time.Minute.Milliseconds()))
			stale.Strategy, stale.Lot = "b", "lot-stale"
			_, err = b.ApplyPendingCommand(ctx, stale, now)
			wantStaleEntry(t, err)
			var rej *RejectedError
			if !errors.As(err, &rej) || rej.Scope.Lot != "lot-stale" || rej.Scope.Kind != EntryIntent {
				t.Fatalf("rejection lost its scope: %+v", rej)
			}
			if _, err := b.service.store.StrategyCheckpoint(ctx, "b", "pending-command:stale"); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("stale create left a receipt:", err)
			}
			after, _ := b.PendingOrders(ctx)
			afterSnap, _ := b.Snapshot(ctx)
			if len(after) != len(before) || beforeSnap.RiskFrozen != afterSnap.RiskFrozen {
				t.Fatalf("stale create changed the book: %d -> %d", len(before), len(after))
			}
			// The same time-series timeframe widens the window: 3 minutes is fresh for 1h.
			wide := staleCreate("wide", staleMetadata("1h", now-3*time.Minute.Milliseconds()))
			wide.Strategy, wide.Lot = "b", "lot-wide"
			if _, err := b.ApplyPendingCommand(ctx, wide, now); err != nil {
				t.Fatal("1h create 3 minutes old refused:", err)
			}

			// An accepted command resent much later replays idempotently, even though stale now.
			later := now + time.Hour.Milliseconds()
			if revision, err := b.ApplyPendingCommand(ctx, fresh, later); err != nil || revision != 1 {
				t.Fatalf("accepted command replay after the window: revision %d, %v", revision, err)
			}
		})
	}
}

func TestStaleEntryRejectionIsTransportedForCreateOnly(t *testing.T) {
	if !hubRejectCodes[RejectStaleEntry] {
		t.Fatal("stale_entry missing from the closed rejection set")
	}
	err := hubReplyError(hubRPCResponse{Rejected: RejectStaleEntry})
	if code, ok := RejectionCode(err); !ok || code != RejectStaleEntry {
		t.Fatalf("reply decoding: %v", err)
	}
	request := hubRPCRequest{Pending: &PendingCommand{Action: PendingCreate}}
	if got := hubRPCRejection("/v1/pending-command", request, rejected(RejectStaleEntry, "x")); got != RejectStaleEntry {
		t.Fatalf("create rejection not transported: %q", got)
	}
	request.Pending.Action = PendingSetProtections
	if got := hubRPCRejection("/v1/pending-command", request, rejected(RejectStaleEntry, "x")); got != "" {
		t.Fatalf("non-create must stay generic: %q", got)
	}
}
