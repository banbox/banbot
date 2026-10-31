package runtime

import (
	"context"
	"database/sql"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
)

func TestSharedConstructorMigratesLegacyFeedCountdownWithoutRestartReset(t *testing.T) {
	for _, existingBook := range []bool{false, true} {
		name := "facade-only"
		if existingBook {
			name = "existing-execution-book"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newSharedTriggerFixture(t)
			quote := f.bridge.Quote
			f.bridge.Quote = func(id string, now int64) (execution.VisibleQuote, error) {
				q, err := quote(id, now)
				q.Bar = 0
				return q, err
			}
			f.job.TimeFrame = "1m"
			f.entry(t, &strat.EnterReq{Tag: "legacy-feed-upgrade", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90, StopBars: 2})
			f.process.Close()
			db, err := sql.Open("sqlite", f.opts.StorePath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			// Recreate the prior facade-owned countdown: one of two bars has
			// elapsed, and no execution-owned feed deadline/counter exists.
			patch := func(query string, args ...any) {
				t.Helper()
				result, err := db.Exec(query, args...)
				if err != nil {
					t.Fatal(err)
				}
				if rows, err := result.RowsAffected(); err != nil || rows != 1 {
					t.Fatalf("legacy fixture patch must affect one checkpoint: rows=%d err=%v", rows, err)
				}
			}
			patch(`UPDATE exec_checkpoint SET payload=json_set(payload,'$.Feeds',json('{"1/1m":{"EndMS":101,"Bars":1}}')) WHERE kind='strategy' AND strategy='__legacy_bridge' AND name='legacy-ts'`)
			patch(`UPDATE exec_checkpoint SET payload=json_set(payload,'$.FeedExpiry',json('true'),'$.CreatedFeedBar',0,'$.EntryFeedExpiry',NULL,'$.Entry.Conditions.StopBars',2) WHERE kind='strategy' AND strategy='__legacy_bridge' AND name LIKE 'legacy-ts/order:id:%'`)
			if existingBook {
				patch(`UPDATE exec_checkpoint SET payload=json_set(payload,'$[0].Order.EntryFeedExpiry',NULL,'$[0].Order.Entry.Conditions.StopBars',2) WHERE kind='strategy' AND strategy=? AND name=?`, execution.PendingBookStrategy, execution.PendingBookName)
			} else {
				patch(`DELETE FROM exec_checkpoint WHERE kind='strategy' AND strategy=? AND name=?`, execution.PendingBookStrategy, execution.PendingBookName)
			}
			if _, err := db.Exec(`DELETE FROM exec_checkpoint WHERE kind='strategy' AND strategy=? AND name=?`, execution.PendingBookStrategy, execution.PendingFeedsName); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			restart := func(at int64) *Process {
				t.Helper()
				p := NewProcess()
				t.Cleanup(p.Close)
				rt, err := p.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
				if err != nil {
					t.Fatal(err)
				}
				f.rt = rt
				rt.Clock.SetTimeMS(at)
				if err := rt.SharedExecution().Reconcile("feed-upgrade-restart", at); err != nil {
					t.Fatal(err)
				}
				if _, err := biz.NewSharedOrderMgr(rt.BizDeps(), rt.SharedExecution(), f.bridge, nil); err != nil {
					t.Fatal("legacy feed constructor upgrade failed", err)
				}
				return p
			}
			check := func(bars int64, canceled bool) {
				t.Helper()
				if err := f.rt.SharedExecution().WithState(func(account *execution.SharedAccount) error {
					feeds, err := account.Store().LoadPendingFeeds(ctx)
					if err != nil {
						return err
					}
					book, err := account.Store().LoadPendingBook(ctx)
					if err != nil {
						return err
					}
					if feeds["1/1m"].Bars != bars || len(book) != 1 || book[0].Order.EntryFeedExpiry == nil || book[0].Order.EntryFeedExpiry.Feed != "1/1m" || book[0].Order.EntryFeedExpiry.ExpiresAtBar != 2 || book[0].Order.Entry.Conditions.StopBars != 0 || book[0].Order.Canceled != canceled {
						t.Fatalf("constructor reset or lost legacy countdown: feeds=%+v book=%+v", feeds, book)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			first := restart(102)
			check(1, false)
			if f.adapter.Metrics().Fills != 0 {
				t.Fatal("constructor migration traded")
			}
			if accepted, err := f.rt.SharedExecution().ObservePendingFeed(ctx, execution.PendingFeedObservation{Feed: "1/1m", EndMS: 102, Closed: true}); err != nil || !accepted {
				t.Fatal(accepted, err)
			}
			first.Close()
			restart(103)
			check(2, false)
			if accepted, err := f.rt.SharedExecution().ObservePendingFeed(ctx, execution.PendingFeedObservation{Feed: "1/1m", EndMS: 102, Closed: true}); err != nil || accepted {
				t.Fatal("restart replay consumed another feed bar", accepted, err)
			}
			if err := f.rt.SharedExecution().AdvancePendingExecution(ctx, 103); err != nil {
				t.Fatal(err)
			}
			check(2, true)
			if f.adapter.Metrics().Fills != 0 {
				t.Fatal("upgraded feed expiry manufactured a fill")
			}
		})
	}
}
