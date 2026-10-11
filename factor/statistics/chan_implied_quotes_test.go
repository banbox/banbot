package statistics

import (
	"math"
	"reflect"
	"testing"
)

func calendarSourceBooks() ([]CalendarBBO, []CalendarBBO, []CalendarBBO) {
	return []CalendarBBO{{Instrument: "H", LiquidityID: "h", Bid: 98.78, Ask: 98.79, BidQty: 100, AskQty: 100, EventMS: 1, AvailableMS: 2, ReceivedMS: 3}}, []CalendarBBO{{Instrument: "M", LiquidityID: "m", Bid: 98.55, Ask: 98.56, BidQty: 100, AskQty: 100, EventMS: 2, AvailableMS: 3, ReceivedMS: 4}}, []CalendarBBO{{Instrument: "H-M", LiquidityID: "s", Bid: .23, BidQty: 50, EventMS: 3, AvailableMS: 4, ReceivedMS: 5}}
}

func TestChanCalendarImpliedSource(t *testing.T) {
	h, m, s := calendarSourceBooks()
	out, err := ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		q          *CalendarQuoteSide
		price, qty float64
	}{{out.ImpliedIn.Bid, .22, 100}, {out.ImpliedIn.Ask, .24, 100}, {out.HImpliedOut.Bid, 98.78, 50}, {out.MImpliedOut.Ask, 98.56, 50}} {
		if c.q == nil || math.Abs(c.q.Price-c.price) > 1e-12 || c.q.Quantity != c.qty {
			t.Fatalf("source oracle %+v", c)
		}
	}
	if out.HImpliedOut.Bid.Relation != "at_touch" || out.MImpliedOut.Ask.Relation != "at_touch" || out.HImpliedOut.Bid.ReceivedMS != 5 || out.HImpliedOut.Bid.Dependencies != [2]string{"s:bid", "m:bid"} || out.MImpliedOut.Ask.Dependencies[1] != "s:bid" {
		t.Fatalf("%+v %+v", out.HImpliedOut.Bid, out.MImpliedOut.Ask)
	}
	// Exercise6.3 uses Hbid98.77; .23 improves Hbid, ties Mask. .24
	// improves both; .21 is behind both touches and adds no BBO depth.
	h[0].Bid = 98.77
	for _, c := range []struct {
		bid  float64
		h, m string
	}{{.23, "improved", "at_touch"}, {.24, "improved", "improved"}, {.21, "worse", "worse"}} {
		s[0].Bid = c.bid
		out, err = ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 10)
		if err != nil || out.HImpliedOut.Bid.Relation != c.h || out.MImpliedOut.Ask.Relation != c.m {
			t.Fatalf("%+v: %v %+v %+v", c, err, out.HImpliedOut.Bid, out.MImpliedOut.Ask)
		}
	}
	// Independent ask-side oracle: sell spread .25 and buy M at98.56
	// sells H at98.81; sell H at98.77 and buy spread .25 buys M98.52.
	s[0].Ask, s[0].AskQty = .25, 7
	out, err = ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 10)
	if err != nil || math.Abs(out.HImpliedOut.Ask.Price-98.81) > 1e-12 || math.Abs(out.MImpliedOut.Bid.Price-98.52) > 1e-12 || out.HImpliedOut.Ask.Quantity != 7 || out.MImpliedOut.Bid.Quantity != 7 {
		t.Fatal(err, out)
	}
	m[0].BidQty, m[0].AskQty, h[0].BidQty, h[0].AskQty = 11, 13, 17, 19
	s[0].BidQty, s[0].AskQty = 23, 29
	out, err = ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 10)
	if err != nil || out.ImpliedIn.Bid.Quantity != 13 || out.ImpliedIn.Ask.Quantity != 11 || out.HImpliedOut.Bid.Quantity != 11 || out.MImpliedOut.Ask.Quantity != 19 || out.HImpliedOut.Ask.Quantity != 13 || out.MImpliedOut.Bid.Quantity != 17 {
		t.Fatal("side-specific capacity bottleneck", err, out)
	}
}

func TestChanCalendarImpliedVisiblePrefix(t *testing.T) {
	h, m, s := calendarSourceBooks()
	a, err := ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 4)
	if err != nil {
		t.Fatal(err)
	}
	s[0].Bid = .2 // unreceived spread cannot change a visible snapshot
	b, err := ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 4)
	if err != nil || !reflect.DeepEqual(a, b) || b.HImpliedOut.Bid != nil {
		t.Fatal(err, a, b)
	}
	future := h[0]
	future.LiquidityID = "h2"
	future.EventMS = 4
	future.AvailableMS = 6
	future.ReceivedMS = 7
	future.Bid = 90
	h = append(h, future)
	b, err = ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 4)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal(err, a, b)
	}
	b, err = ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 7)
	if err != nil || b.ImpliedIn.Bid.EventMS != 4 || b.ImpliedIn.Bid.AvailableMS != 6 || b.ImpliedIn.Bid.ReceivedMS != 7 || math.Abs(b.ImpliedIn.Bid.Price-(-8.56)) > 1e-12 {
		t.Fatal(err, b)
	}
}

func TestChanCalendarImpliedComparisonClock(t *testing.T) {
	h, m, s := calendarSourceBooks()
	h[0].AvailableMS, h[0].ReceivedMS = 7, 8
	out, err := ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 10)
	if err != nil || out.HImpliedOut.Bid.AvailableMS != 7 || out.HImpliedOut.Bid.ReceivedMS != 8 || out.HImpliedOut.Bid.ComparisonLiquidityID != "h:bid" {
		t.Fatal(err, out.HImpliedOut.Bid)
	}
}

func TestChanCalendarImpliedMalformedFutureIsolation(t *testing.T) {
	h, m, s := calendarSourceBooks()
	base, err := ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, book := range []string{"H", "M", "H-M"} {
		future := CalendarBBO{Instrument: book, LiquidityID: "future", Bid: math.NaN(), Ask: math.Inf(1), BidQty: -1, AskQty: math.NaN(), EventMS: 6, AvailableMS: 7, ReceivedMS: 20}
		fh, fm, fs := append([]CalendarBBO(nil), h...), append([]CalendarBBO(nil), m...), append([]CalendarBBO(nil), s...)
		switch book {
		case "H":
			fh = append(fh, future)
		case "M":
			fm = append(fm, future)
		case "H-M":
			fs = append(fs, future)
		}
		got, err := ChanCalendarImpliedQuotes("H", "M", "H-M", fh, fm, fs, 10)
		if err != nil || !reflect.DeepEqual(base, got) {
			t.Fatalf("unreceived %s changed earlier snapshot: %v", book, err)
		}
		if _, err = ChanCalendarImpliedQuotes("H", "M", "H-M", fh, fm, fs, 20); err == nil {
			t.Fatalf("visible malformed %s accepted", book)
		}
		// Also an unpublished future event must not alter prior payload/math.
		future.EventMS, future.AvailableMS, future.ReceivedMS = 30, 31, 32
		switch book {
		case "H":
			fh[len(fh)-1] = future
		case "M":
			fm[len(fm)-1] = future
		case "H-M":
			fs[len(fs)-1] = future
		}
		got, err = ChanCalendarImpliedQuotes("H", "M", "H-M", fh, fm, fs, 10)
		if err != nil || !reflect.DeepEqual(base, got) {
			t.Fatalf("future %s changed earlier snapshot: %v", book, err)
		}
	}
}

func TestChanCalendarImpliedRejectMalformed(t *testing.T) {
	for _, mutate := range []func([]CalendarBBO, []CalendarBBO, []CalendarBBO){
		func(h, m, s []CalendarBBO) { h[0].Instrument = "wrong" },
		func(h, m, s []CalendarBBO) { s[0].LiquidityID = "h" },
		func(h, m, s []CalendarBBO) { h[0].Bid = 100 },
		func(h, m, s []CalendarBBO) { m[0].Ask = math.NaN() },
		func(h, m, s []CalendarBBO) { s[0].BidQty = math.Inf(1) },
		func(h, m, s []CalendarBBO) { s[0].BidQty = -1 },
		func(h, m, s []CalendarBBO) { s[0].ReceivedMS = 2 },
		func(h, m, s []CalendarBBO) {
			h[0].Bid = math.MaxFloat64
			h[0].Ask = math.MaxFloat64
			m[0].Bid = -math.MaxFloat64
			m[0].Ask = -math.MaxFloat64
		},
	} {
		h, m, s := calendarSourceBooks()
		mutate(h, m, s)
		if _, err := ChanCalendarImpliedQuotes("H", "M", "H-M", h, m, s, 10); err == nil {
			t.Fatal("malformed quote accepted")
		}
	}
}
