package execution

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestStrategyWriterRejectsLateCommandsAndFencesOldSessions(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(map[bool]string{false: "SQLite", true: "Memory"}[memory], func(t *testing.T) {
			ctx := context.Background()
			borrow, venue := strategyService(t, memory)
			claim := StrategyWriterClaim{Strategy: "a", WriterID: "session-one"}
			lease, err := borrow.ClaimStrategyWriter(ctx, claim)
			if err != nil || lease.Epoch != 1 || lease.Revision != 0 {
				t.Fatal(lease, err)
			}
			request := func(id string, seq uint64, steps int64) StrategyRebalance {
				r := strategyRequest("a", id, 10, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: steps})
				r.StrategyEpoch, r.StrategySequence = lease.Epoch, seq
				return r
			}
			newer := request("new-target", 2, 0)
			if err := borrow.RebalanceStrategy(newer, 11); err != nil {
				t.Fatal(err)
			}
			if err := borrow.RebalanceStrategy(request("late-target", 1, 10), 12); err == nil {
				t.Fatal("late first arrival reopened an old target")
			}
			if err := borrow.RebalanceStrategy(request("duplicate-sequence", 2, 10), 12); err == nil {
				t.Fatal("new command reused an accepted sequence")
			}
			wrongRevision := uint64(0)
			badCAS := request("bad-cas", 3, 10)
			badCAS.ExpectedRevision = &wrongRevision
			if err := borrow.RebalanceStrategy(badCAS, 12); err == nil {
				t.Fatal("stale expected revision accepted")
			}
			if _, err := borrow.ClaimStrategyWriter(ctx, StrategyWriterClaim{Strategy: "a", WriterID: "stale-session"}); err == nil {
				t.Fatal("writer replaced without matching epoch")
			}
			next, err := borrow.ClaimStrategyWriter(ctx, StrategyWriterClaim{Strategy: "a", WriterID: "session-two", ExpectedEpoch: 1})
			if err != nil || next.Epoch != 2 || next.Revision != 1 {
				t.Fatal(next, err)
			}
			if err := borrow.RebalanceStrategy(request("old-writer", 99, 10), 13); err == nil {
				t.Fatal("old writer bypassed fencing with a higher sequence")
			}
			before := venue.Metrics().Fills
			if err := borrow.RebalanceStrategy(newer, 14); err != nil || venue.Metrics().Fills != before {
				t.Fatal("accepted old command retry must return its original outcome", err)
			}
			if retried, err := borrow.ClaimStrategyWriter(ctx, claim); err != nil || retried != lease {
				t.Fatal("lost claim ACK reactivated or changed fenced writer", retried, err)
			}
			lease = next
			fresh := request("current-writer", 1, 2)
			fresh.ExpectedRevision = &next.Revision
			if err := borrow.RebalanceStrategy(fresh, 15); err != nil {
				t.Fatal(err)
			}
			state, err := borrow.service.store.strategyWriter(ctx, "a")
			if err != nil || state != (strategyWriterState{Epoch: 2, Sequence: 1, Revision: 2}) {
				t.Fatal(state, err)
			}
			if !memory {
				opts := borrow.service.opts
				if err := borrow.service.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := OpenStoreWithLeaseDir(opts.StorePath, borrow.service.store.Account(), opts.SenderLeaseDir)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				restored, err := reopened.strategyWriter(ctx, "a")
				if err != nil || restored != state {
					t.Fatal("restart lost writer fence/highwater", restored, err)
				}
				if _, err := reopened.StrategyCheckpoint(ctx, "a", "target-revision:current-writer"); err != nil {
					t.Fatal("restart lost accepted command identity", err)
				}
			}
		})
	}
}

func TestStrategyWriterPlanAndHighwaterCommitAtomically(t *testing.T) {
	ctx := context.Background()
	borrow, _ := strategyService(t, false)
	lease, err := borrow.ClaimStrategyWriter(ctx, StrategyWriterClaim{Strategy: "a", WriterID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	store := borrow.service.store
	if _, err := store.db.Exec(`CREATE TRIGGER reject_highwater BEFORE INSERT ON exec_checkpoint WHEN NEW.name='strategy-writer:v1' BEGIN SELECT RAISE(ABORT,'highwater persistence failed'); END`); err != nil {
		t.Fatal(err)
	}
	request := strategyRequest("a", "atomic-command", 10, StrategyTargetsFull, ExecutableTarget{Lot: "one", SignedSteps: 2})
	request.StrategyEpoch, request.StrategySequence = lease.Epoch, 1
	if _, err := borrow.PrepareStrategy(request); err == nil {
		t.Fatal("failed highwater write accepted plan")
	}
	state, err := store.strategyWriter(ctx, "a")
	if err != nil || state.Sequence != 0 || state.Revision != 0 {
		t.Fatal("failed transaction changed highwater", state, err)
	}
	if _, err := store.Plan(ctx, request.PlanID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("plan escaped failed highwater transaction", err)
	}
	if _, err := store.StrategyCheckpoint(ctx, "a", "target-revision:"+request.PlanID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("receipt escaped failed highwater transaction", err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER reject_highwater`); err != nil {
		t.Fatal(err)
	}
	if _, err := borrow.PrepareStrategy(request); err != nil {
		t.Fatal("rolled-back command cannot retry", err)
	}
}

func TestStrategyWriterHighwaterRollsBackWithRejectedPlan(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(map[bool]string{false: "SQLite", true: "Memory"}[memory], func(t *testing.T) {
			ctx := context.Background()
			borrow, _ := strategyService(t, memory)
			lease, err := borrow.ClaimStrategyWriter(ctx, StrategyWriterClaim{Strategy: "a", WriterID: "session"})
			if err != nil {
				t.Fatal(err)
			}
			bad := strategyRequest("a", "rejected", 10, StrategyTargetsFull, ExecutableTarget{Lot: "one", SignedSteps: 2})
			bad.StrategyEpoch, bad.StrategySequence = lease.Epoch, 1
			bad.Requests[0].Targets[0].Strategy = "b"
			if _, err := borrow.PrepareStrategy(bad); err == nil {
				t.Fatal("foreign target accepted")
			}
			state, err := borrow.service.store.strategyWriter(ctx, "a")
			if err != nil || state.Sequence != 0 || state.Revision != 0 {
				t.Fatal("rejected plan advanced highwater", state, err)
			}
			bad.Requests[0].Targets[0].Strategy = "a"
			if _, err := borrow.PrepareStrategy(bad); err != nil {
				t.Fatal("failed attempt consumed sequence or command identity", err)
			}
		})
	}
}

func TestStrategyWriterFenceSpansPendingTargetsAndTimeSeriesExits(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(map[bool]string{false: "SQLite", true: "Memory"}[memory], func(t *testing.T) {
			ctx := context.Background()
			b, _ := strategyService(t, memory)
			if _, err := b.ClaimStrategyWriter(ctx, StrategyWriterClaim{Strategy: "a", WriterID: "old"}); err != nil {
				t.Fatal(err)
			}
			// An empty selector is still an accepted command and consumes the
			// global writer sequence in the same transaction as its receipt.
			exit := TimeSeriesExitCommand{ID: "no-lots", Strategy: "a", StrategyEpoch: 1, StrategySequence: 2, Force: true}
			if results, err := b.ApplyTimeSeriesExit(ctx, exit, 10); err != nil || len(results) != 0 {
				t.Fatal(results, err)
			}
			create := pendingCreate("late-create", "a")
			create.StrategyEpoch, create.StrategySequence = 1, 1
			if _, err := b.ApplyPendingCommand(ctx, create, 11); err == nil {
				t.Fatal("pending create bypassed earlier exit sequence")
			}
			create.StrategySequence = 3
			if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
				t.Fatal(err)
			}
			staleTarget := strategyRequest("a", "late-target", 12, StrategyTargetsFull, ExecutableTarget{Lot: "one", SignedSteps: 3})
			staleTarget.StrategyEpoch, staleTarget.StrategySequence = 1, 2
			if _, err := b.PrepareStrategy(staleTarget); err == nil {
				t.Fatal("target used separate highwater from pending create")
			}
			if _, err := b.ClaimStrategyWriter(ctx, StrategyWriterClaim{Strategy: "a", WriterID: "new", ExpectedEpoch: 1}); err != nil {
				t.Fatal(err)
			}
			lateExit := exit
			lateExit.ID, lateExit.StrategySequence = "old-exit", 100
			if _, err := b.ApplyTimeSeriesExit(ctx, lateExit, 13); err == nil {
				t.Fatal("old writer issued a new exit")
			}
			cancel := PendingCommand{ID: "old-cancel", Strategy: "a", Lot: "lot-a", ExpectedRevision: 1, Action: PendingCancelEntry, StrategyEpoch: 1, StrategySequence: 101}
			if _, err := b.ApplyPendingCommand(ctx, cancel, 13); err == nil {
				t.Fatal("old writer changed pending protection/entry")
			}
			if _, err := b.ApplyTimeSeriesExit(ctx, exit, 14); err != nil {
				t.Fatal("fenced writer cannot retrieve accepted exit receipt", err)
			}
			if revision, err := b.ApplyPendingCommand(ctx, create, 14); err != nil || revision != 1 {
				t.Fatal("fenced writer cannot retrieve accepted pending receipt", revision, err)
			}
		})
	}
}
