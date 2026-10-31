package execution

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func watermarkOrders(t *testing.T, memory bool) *SharedAccountBorrow {
	t.Helper()
	b, _ := strategyService(t, memory)
	command := pendingCreate("watermarked-create", "a")
	command.Create.StopBars, command.Create.Feed = 2, "17/1m"
	if _, err := b.ApplyPendingCommand(context.Background(), command, 100); err != nil {
		t.Fatal(err)
	}
	book, err := b.PendingOrders(context.Background())
	if err != nil || len(book) != 1 || book[0].Order.EntryFeedExpiry.NotBeforeEndMS != 100 {
		t.Fatal("new command did not freeze owner acceptance watermark", book, err)
	}
	// A preexisting legacy order may spend the very same queued observations.
	legacy := pendingTestOrder(b.service.store, "b")
	if err := b.WithState(func(s *SharedAccount) error {
		return s.BindPendingFeedExpiry(context.Background(), legacy, "17/1m", 2)
	}); err != nil {
		t.Fatal(err)
	}
	book = append(book, PendingDefinition{Order: *legacy, Instrument: ledgerInstrument()})
	saveTestPendingBook(t, b.service.store, book)
	return b
}

func TestPendingFeedAcceptanceWatermarkIgnoresBacklogWithoutLosingLegacyProgress(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(map[bool]string{false: "SQLite", true: "Memory"}[memory], func(t *testing.T) {
			ctx := context.Background()
			b := watermarkOrders(t, memory)
			for index, end := range []int64{90, 100, 101, 102} {
				fact := PendingFeedObservation{Feed: "17/1m", EndMS: end, Closed: true}
				if accepted, err := b.ObservePendingFeed(ctx, fact); err != nil || !accepted {
					t.Fatal(accepted, err)
				}
				before, err := b.PendingOrders(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if accepted, err := b.ObservePendingFeed(ctx, fact); err != nil || accepted {
					t.Fatal("lost ACK observation retry shifted deadline twice", accepted, err)
				}
				after, _ := b.PendingOrders(ctx)
				if !reflect.DeepEqual(before, after) {
					t.Fatal("duplicate observation changed order lifetimes")
				}
				if err := b.WithState(func(s *SharedAccount) error { return s.expirePendingFeeds(ctx, after) }); err != nil {
					t.Fatal(err)
				}
				for _, definition := range after {
					wantExpired := definition.Order.Strategy == "a" && index == 3 || definition.Order.Strategy == "b" && index >= 1
					if (definition.Order.Entry.State == Expired) != wantExpired {
						t.Fatalf("incorrect lifetime at fact=%d strategy=%s: %+v", end, definition.Order.Strategy, definition.Order.EntryFeedExpiry)
					}
				}
			}
			if err := b.AdvancePendingExecution(ctx, 103); err != nil {
				t.Fatal(err)
			}
			book, _ := b.PendingOrders(ctx)
			for _, definition := range book {
				if !definition.Order.Canceled {
					t.Fatal("deadline did not reach actual execution expiry", definition.Order)
				}
			}
			if !memory {
				store := b.service.store
				opts := b.service.opts
				if err := b.service.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := OpenStoreWithLeaseDir(opts.StorePath, store.Account(), opts.SenderLeaseDir)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				restored, err := reopened.LoadPendingBook(ctx)
				if err != nil || !reflect.DeepEqual(book, restored) {
					t.Fatal("restart lost acceptance watermark/deadline", restored, err)
				}
			}
		})
	}
}

func TestPendingFeedAcceptanceWatermarkOffsetOverflowIsAtomic(t *testing.T) {
	ctx := context.Background()
	b := watermarkOrders(t, true)
	book, _ := b.PendingOrders(ctx)
	book[0].Order.EntryFeedExpiry.ExpiresAtBar = math.MaxInt64
	saveTestPendingBook(t, b.service.store, book)
	if accepted, err := b.ObservePendingFeed(ctx, PendingFeedObservation{Feed: "17/1m", EndMS: 90, Closed: true}); err == nil || accepted {
		t.Fatal("offset overflow was accepted", accepted, err)
	}
	feeds, _ := b.service.store.LoadPendingFeeds(ctx)
	if len(feeds) != 0 {
		t.Fatal("overflow advanced shared feed without its order offset", feeds)
	}
}

func TestPendingFeedAcceptanceWatermarkPersistenceFailureRollsBackFeedAndBook(t *testing.T) {
	ctx := context.Background()
	b := watermarkOrders(t, false)
	store := b.service.store
	if _, err := store.db.Exec(`CREATE TRIGGER reject_watermark BEFORE INSERT ON exec_checkpoint WHEN NEW.name='book-v1' BEGIN SELECT RAISE(ABORT,'watermark write failed'); END`); err != nil {
		t.Fatal(err)
	}
	defer store.db.Exec(`DROP TRIGGER reject_watermark`)
	before, _ := b.PendingOrders(ctx)
	if accepted, err := b.ObservePendingFeed(ctx, PendingFeedObservation{Feed: "17/1m", EndMS: 90, Closed: true}); err == nil || accepted {
		t.Fatal("failed watermark write accepted global observation", accepted, err)
	}
	after, _ := b.PendingOrders(ctx)
	feeds, _ := store.LoadPendingFeeds(ctx)
	if !reflect.DeepEqual(before, after) || len(feeds) != 0 {
		t.Fatal("failed order offset did not roll back shared feed and book", feeds)
	}
}
