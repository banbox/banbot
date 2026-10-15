package execution

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/shopspring/decimal"
)

func cvd(s string) decimal.Decimal { return decimal.RequireFromString(s) }
func cvInitial() []CrossVenueBalance {
	return []CrossVenueBalance{{"cheap", "BTC", "USD", cvd("1000"), cvd("1")}, {"rich", "BTC", "USD", cvd("1000"), cvd("2")}}
}
func cvBook(venue, bid, ask string) CrossVenueBook {
	return CrossVenueBook{Venue: venue, Asset: "BTC", Currency: "USD", AtMS: 100, PublishedMS: 101, ReceivedMS: 102, Bids: []CrossVenueLevel{{cvd(bid), cvd("2")}}, Asks: []CrossVenueLevel{{cvd(ask), cvd("2")}}, FeeRate: cvd("0.002")}
}
func cvRequest() CrossVenueRequest {
	return CrossVenueRequest{ID: "trade", Buy: cvBook("cheap", "233.232", "233.546"), Sell: cvBook("rich", "239.19", "239.48"), Units: cvd("1"), MinProfit: decimal.Zero, EstimatedRebalanceCash: cvd("2.33546"), NowMS: 102, MaxAgeMS: 10}
}
func cvOpen(t *testing.T) *CrossVenuePaper {
	t.Helper()
	p, err := OpenCrossVenuePaper(filepath.Join(t.TempDir(), "paper.sqlite"), cvInitial())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}
func cvEqual(t *testing.T, actual decimal.Decimal, expected string) {
	t.Helper()
	if !actual.Equal(cvd(expected)) {
		t.Fatalf("got %s want %s", actual, expected)
	}
}

func TestCrossVenuePaperSourceCashInventoryAndTransfer(t *testing.T) {
	p, ctx := cvOpen(t), context.Background()
	initial, _ := p.Snapshot(ctx)
	base, _ := initial.Equity(cvd("233.546"))
	r := cvRequest()
	trade, err := p.Begin(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if trade.Status != "bought" {
		t.Fatal(trade)
	}
	mid, _ := p.Snapshot(ctx)
	cvEqual(t, mid.Balances[0].Cash, "765.986908")
	cvEqual(t, mid.Balances[0].Units, "2")
	cvEqual(t, mid.Balances[1].Units, "2") // sell has not happened
	trade, err = p.Exit(ctx, r.ID, r.Sell, 102, false)
	if err != nil {
		t.Fatal(err)
	}
	cvEqual(t, trade.NetCash, "4.698528")
	s, _ := p.Snapshot(ctx)
	cvEqual(t, s.Balances[1].Cash, "1238.71162")
	cvEqual(t, s.Balances[1].Units, "1")
	nav, _ := s.Equity(cvd("233.546"))
	cvEqual(t, nav.Sub(base), "4.698528")
	// Same equal-unit trade redistributes inventory; a real withdrawal loses
	// .01 coin. Use its exact common mark instead of the book's rounded $2.
	x := CrossVenueTransfer{ID: "rebalance", From: "cheap", To: "rich", Units: cvd(".99"), UnitFee: cvd(".01"), AtMS: 103, ArrivalMS: 200}
	if err = p.StartTransfer(ctx, x); err != nil {
		t.Fatal(err)
	}
	s, _ = p.Snapshot(ctx)
	nav, _ = s.Equity(cvd("233.546"))
	cvEqual(t, nav.Sub(base), "2.363068")
	cvEqual(t, s.Balances[1].Units, "1") // in transit cannot fund trading
	if err = p.CompleteTransfer(ctx, x.ID, 199); err == nil {
		t.Fatal("credited early")
	}
	if err = p.CompleteTransfer(ctx, x.ID, 200); err != nil {
		t.Fatal(err)
	}
	if err = p.CompleteTransfer(ctx, x.ID, 200); err != nil {
		t.Fatal(err)
	}
	s, _ = p.Snapshot(ctx)
	cvEqual(t, s.Balances[1].Units, "1.99")
	nav, _ = s.Equity(cvd("233.546"))
	cvEqual(t, nav.Sub(base), "2.363068")
	if len(s.Fills) != 2 {
		t.Fatal("actual leg fills missing", s.Fills)
	}
}

func TestCrossVenuePaperDurableRetryAndConflicts(t *testing.T) {
	path, ctx := filepath.Join(t.TempDir(), "paper.sqlite"), context.Background()
	p, err := OpenCrossVenuePaper(path, cvInitial())
	if err != nil {
		t.Fatal(err)
	}
	r := cvRequest()
	if _, err = p.Begin(ctx, r); err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	p, err = OpenCrossVenuePaper(path, cvInitial())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Begin(ctx, r); err != nil {
		t.Fatal(err)
	}
	bad := r
	bad.Units = cvd(".5")
	if _, err = p.Begin(ctx, bad); err == nil {
		t.Fatal("accepted changed trade")
	}
	if _, err = p.Exit(ctx, r.ID, r.Sell, 102, false); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exit(ctx, r.ID, r.Sell, 102, false); err != nil {
		t.Fatal(err)
	}
	badBook := r.Sell
	badBook.FeeRate = cvd(".01")
	if _, err = p.Exit(ctx, r.ID, badBook, 102, false); err == nil {
		t.Fatal("accepted changed exit")
	}
	s, _ := p.Snapshot(ctx)
	if len(s.Fills) != 2 {
		t.Fatal("duplicated fill")
	}
	changed := cvInitial()
	changed[0].Cash = cvd("1001")
	if other, e := OpenCrossVenuePaper(path, changed); e == nil {
		other.Close()
		t.Fatal("reseeded portfolio")
	}
}

func TestCrossVenuePaperFailedSecondLegAndUnwind(t *testing.T) {
	p, ctx := cvOpen(t), context.Background()
	r := cvRequest()
	if _, err := p.Begin(ctx, r); err != nil {
		t.Fatal(err)
	}
	sell := r.Sell
	sell.Bids = []CrossVenueLevel{{cvd("239.19"), cvd(".25")}}
	if _, err := p.Exit(ctx, r.ID, sell, 103, false); err == nil {
		t.Fatal("filled nonexistent depth")
	}
	other := r
	other.ID = "second"
	if _, err := p.Begin(ctx, other); err == nil {
		t.Fatal("ignored exposed first leg")
	}
	s, _ := p.Snapshot(ctx)
	if len(s.Fills) != 1 || s.Trades[r.ID].Status != "bought" {
		t.Fatal("failed attempt mutated journal")
	}
	u := cvBook("cheap", "230", "231")
	u.AtMS = 103
	u.PublishedMS = 104
	u.ReceivedMS = 105
	trade, err := p.Exit(ctx, r.ID, u, 105, true)
	if err != nil {
		t.Fatal(err)
	}
	cvEqual(t, trade.NetCash, "-4.473092")
	s, _ = p.Snapshot(ctx)
	cvEqual(t, s.Balances[0].Units, "1")
	cvEqual(t, s.Balances[1].Units, "2")
	if trade.Status != "unwound" {
		t.Fatal(trade.Status)
	}
}

func TestCrossVenuePaperDepthAndCausality(t *testing.T) {
	for _, variant := range []string{"depth", "cash", "inventory", "future", "receipt", "stale", "currency", "fee", "threshold"} {
		t.Run(variant, func(t *testing.T) {
			p, ctx := cvOpen(t), context.Background()
			r := cvRequest()
			switch variant {
			case "depth":
				r.Buy.Asks[0].Units = cvd(".2")
			case "cash":
				r.Units = cvd("5")
			case "inventory":
				r.Units = cvd("3")
				r.Buy.Asks[0].Units = cvd("10")
				r.Sell.Bids[0].Units = cvd("10")
			case "future":
				r.Buy.AtMS = 103
			case "receipt":
				r.Buy.ReceivedMS = 103
			case "stale":
				r.NowMS = 111
			case "currency":
				r.Sell.Currency = "EUR"
			case "fee":
				r.Buy.FeeRate = cvd(".1")
			case "threshold":
				r.MinProfit = cvd("3")
			}
			if _, err := p.Begin(ctx, r); err == nil {
				t.Fatal("accepted invalid opportunity")
			}
			s, _ := p.Snapshot(ctx)
			if len(s.Fills) != 0 {
				t.Fatal("rejected request filled")
			}
		})
	}
	p, ctx := cvOpen(t), context.Background()
	r := cvRequest()
	r.Buy.Asks = []CrossVenueLevel{{cvd("233.546"), cvd(".5")}, {cvd("234"), cvd(".5")}}
	r.Sell.Bids = []CrossVenueLevel{{cvd("239.19"), cvd(".25")}, {cvd("239"), cvd(".75")}}
	tt, err := p.Begin(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	cvEqual(t, tt.Buy.Notional, "233.773")
	tt, err = p.Exit(ctx, r.ID, r.Sell, 102, false)
	if err != nil {
		t.Fatal(err)
	}
	cvEqual(t, tt.Exit.Notional, "239.0475")
	cvEqual(t, tt.NetCash, "4.328859")
}

func TestCrossVenuePaperConsumedDepthAndImmutableBook(t *testing.T) {
	p, ctx := cvOpen(t), context.Background()
	r := cvRequest()
	r.Buy.Asks[0].Units = cvd("1")
	r.Sell.Bids[0].Units = cvd("1")
	if _, err := p.Begin(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exit(ctx, r.ID, r.Sell, 102, false); err != nil {
		t.Fatal(err)
	}
	r.ID = "second"
	if _, err := p.Begin(ctx, r); err == nil {
		t.Fatal("reused consumed depth")
	}
	r.Buy.Asks[0].Units = cvd("2")
	if _, err := p.Begin(ctx, r); err == nil {
		t.Fatal("changed immutable quote vintage")
	}
	r.Buy.Asks[0].Units = cvd("1")
	r.Buy.ReceivedMS = 103
	r.NowMS = 103
	if _, err := p.Begin(ctx, r); err == nil {
		t.Fatal("replenished depth using changed receipt time")
	}
	r.Buy.AtMS, r.Buy.PublishedMS, r.Buy.ReceivedMS = 103, 104, 105
	r.Sell.AtMS, r.Sell.PublishedMS, r.Sell.ReceivedMS = 103, 104, 105
	r.NowMS = 105
	if _, err := p.Begin(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exit(ctx, r.ID, r.Sell, 105, false); err != nil {
		t.Fatal(err)
	}
}

func TestCrossVenuePaperCashTransferDurableConservation(t *testing.T) {
	path, ctx := filepath.Join(t.TempDir(), "transfer.sqlite"), context.Background()
	p, err := OpenCrossVenuePaper(path, cvInitial())
	if err != nil {
		t.Fatal(err)
	}
	x := CrossVenueTransfer{ID: "cash", From: "rich", To: "cheap", Cash: cvd("100"), CashFee: cvd(".5"), AtMS: 10, ArrivalMS: 20}
	if err = p.StartTransfer(ctx, x); err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	p, err = OpenCrossVenuePaper(path, cvInitial())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.StartTransfer(ctx, x); err != nil {
		t.Fatal(err)
	}
	s, _ := p.Snapshot(ctx)
	cvEqual(t, s.Balances[1].Cash, "899.5")
	cvEqual(t, s.Balances[0].Cash, "1000")
	equity, _ := s.Equity(cvd("100"))
	cvEqual(t, equity, "2299.5")
	bad := x
	bad.CashFee = cvd(".6")
	if err = p.StartTransfer(ctx, bad); err == nil {
		t.Fatal("changed durable transfer")
	}
	if err = p.CompleteTransfer(ctx, x.ID, 20); err != nil {
		t.Fatal(err)
	}
	if err = p.CompleteTransfer(ctx, x.ID, 19); err == nil {
		t.Fatal("claimed prior completion")
	}
	if err = p.StartTransfer(ctx, x); err != nil {
		t.Fatal(err)
	}
	s, _ = p.Snapshot(ctx)
	cvEqual(t, s.Balances[0].Cash, "1100")
	cvEqual(t, s.Balances[1].Cash, "899.5")
	equity, _ = s.Equity(cvd("100"))
	cvEqual(t, equity, "2299.5")
}

func TestCrossVenuePaperPrepositionedCashCannotBorrow(t *testing.T) {
	seed := cvInitial()
	seed[0].Cash = cvd("233.546") // premium enough, commission missing
	p, err := OpenCrossVenuePaper(filepath.Join(t.TempDir(), "cash.sqlite"), seed)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Begin(context.Background(), cvRequest()); err == nil {
		t.Fatal("borrowed cash to fund commission")
	}
	s, _ := p.Snapshot(context.Background())
	if len(s.Fills) != 0 {
		t.Fatal("unsupported credit created a fill")
	}
	cvEqual(t, s.Balances[0].Cash, "233.546")
}

func TestCrossVenuePaperSupersededVintageDurableAndRetry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vintages.sqlite")
	seed := cvInitial()
	seed[1].Units = cvd("10")
	p, err := OpenCrossVenuePaper(path, seed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	old := cvRequest() // two displayed units; leave one unused
	if _, err = p.Begin(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exit(ctx, old.ID, old.Sell, 102, false); err != nil {
		t.Fatal(err)
	}
	newer := cvRequest()
	newer.ID, newer.Units, newer.NowMS = "newer", cvd("2"), 105
	newer.Buy.AtMS, newer.Buy.PublishedMS, newer.Buy.ReceivedMS = 103, 104, 105
	newer.Sell.AtMS, newer.Sell.PublishedMS, newer.Sell.ReceivedMS = 103, 104, 105
	if _, err = p.Begin(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exit(ctx, newer.ID, newer.Sell, 105, false); err != nil {
		t.Fatal(err)
	}
	backdated := old
	backdated.ID, backdated.NowMS = "backdated", 105
	before, _ := p.Snapshot(ctx)
	if _, err = p.Begin(ctx, backdated); err == nil {
		t.Fatal("reused remaining depth of superseded book")
	}
	after, _ := p.Snapshot(ctx)
	if !crossVenueSame(before, after) || len(after.Fills) != 4 {
		t.Fatal("rejected vintage changed durable state")
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = OpenCrossVenuePaper(path, seed)
	if err != nil {
		t.Fatal(err)
	}
	// Exact earlier retries remain valid after newer books and a cold reopen.
	if _, err = p.Begin(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exit(ctx, old.ID, old.Sell, 102, false); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Begin(ctx, backdated); err == nil {
		t.Fatal("reopen forgot the newest observed vintage")
	}
	x := CrossVenueTransfer{ID: "rebalance", From: "cheap", To: "rich", Units: cvd("1"), AtMS: 105, ArrivalMS: 106}
	if err = p.StartTransfer(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err = p.CompleteTransfer(ctx, x.ID, 106); err != nil {
		t.Fatal(err)
	}
	backdated.NowMS = 106
	if _, err = p.Begin(ctx, backdated); err == nil {
		t.Fatal("transfer replenished a superseded book")
	}
	// Later receipt/publication metadata at the same observation timestamp
	// cannot turn the exhausted newest book into another depth allocation.
	for _, field := range []string{"publication", "receipt"} {
		changed := newer
		changed.ID, changed.NowMS, changed.Units = field, 106, cvd("1")
		if field == "publication" {
			changed.Buy.PublishedMS = 105
		} else {
			changed.Buy.ReceivedMS = 106
		}
		if _, err = p.Begin(ctx, changed); err == nil {
			t.Fatalf("%s metadata replenished displayed depth", field)
		}
	}
	after, _ = p.Snapshot(ctx)
	if len(after.Fills) != 4 {
		t.Fatal("retry or rejected request duplicated fills")
	}
}

func TestCrossVenuePaperLatestBookAppliesToSellAndUnwind(t *testing.T) {
	for _, unwind := range []bool{false, true} {
		t.Run(map[bool]string{false: "sell", true: "unwind"}[unwind], func(t *testing.T) {
			p, ctx := cvOpen(t), context.Background()
			r := cvRequest()
			r.Buy.AtMS, r.Buy.PublishedMS, r.Buy.ReceivedMS = 103, 104, 105
			r.Sell.AtMS, r.Sell.PublishedMS, r.Sell.ReceivedMS = 103, 104, 105
			r.NowMS = 105
			if _, err := p.Begin(ctx, r); err != nil {
				t.Fatal(err)
			}
			// Begin records both books, including the sell book whose depth has
			// not been consumed, and both sides of the bought venue's book.
			fresh := r.Sell
			if unwind {
				fresh = r.Buy
			}
			older := fresh
			older.AtMS, older.PublishedMS, older.ReceivedMS = 100, 101, 102
			before, _ := p.Snapshot(ctx)
			if _, err := p.Exit(ctx, r.ID, older, 105, unwind); err == nil {
				t.Fatal("recovery used a superseded book")
			}
			after, _ := p.Snapshot(ctx)
			if !crossVenueSame(before, after) {
				t.Fatal("rejected recovery changed state")
			}
			if _, err := p.Exit(ctx, r.ID, fresh, 105, unwind); err != nil {
				t.Fatal(err)
			}
		})
	}
}
