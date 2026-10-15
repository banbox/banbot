package statistics

import (
	"math"
	"reflect"
	"testing"
)

func isoSourceBooks() []DisplayedDepth {
	return []DisplayedDepth{
		{Venue: "L", Instrument: "X", EventMS: 100, AvailableMS: 100, ReceivedMS: 101, Protected: true, Levels: []DisplayedLevel{{10.02, 100}, {10, 200}}},
		{Venue: "N1", Instrument: "X", EventMS: 100, AvailableMS: 100, ReceivedMS: 102, Protected: true, Levels: []DisplayedLevel{{10.03, 100}, {10.01, 300}}},
		{Venue: "N2", Instrument: "X", EventMS: 100, AvailableMS: 100, ReceivedMS: 103, Protected: true, Levels: []DisplayedLevel{{10, 100}}},
	}
}

func TestDisplayedSweepSourceISOAndIOC(t *testing.T) {
	b := isoSourceBooks()
	r := DisplayedSweepRequest{Kind: ISOLimitIOC, LocalVenue: "L", Instrument: "X", Side: -1, Quantity: 300, LimitPrice: 10, SubmitMS: 103, MaxAgeMS: 3}
	p, err := PlanDisplayedSweep(b, r)
	if err != nil {
		t.Fatal(err)
	}
	want := []DisplayedSweepFill{{"L", 10.02, 100}, {"L", 10, 200}}
	if !reflect.DeepEqual(p.LocalFills, want) || p.LocalFilled != 300 || p.LocalCanceled != 0 || p.LocalNotional != 3002 || p.ReceivedMS != 103 || p.AdditionalProtectedQuantity != 100 || len(p.ProtectedOrders) != 1 || p.ProtectedOrders[0].Venue != "N1" || p.ProtectedOrders[0].Price != 10.03 {
		t.Fatalf("source Ex6.1 plan: %+v", p)
	}
	// N1's deeper300 at10.01 is not protected; N2 at the limit is not superior.
	// Local300 plus separate protected100 means400 total planned quantity.
	r.Quantity = 350
	p, err = PlanDisplayedSweep(b, r)
	if err != nil || p.LocalCanceled != 50 {
		t.Fatalf("IOC residual: %+v %v", p, err)
	}
	r.Kind = LocalMarketIOC
	r.LimitPrice = 0
	p, err = PlanDisplayedSweep(b, r)
	if err != nil || p.LocalFilled != 0 || p.LocalCanceled != 350 || p.AveragePriceValid || len(p.ProtectedOrders) != 0 {
		t.Fatalf("local non-NBBO market IOC: %+v %v", p, err)
	}
	// After the caller supplies a new frozen NBBO at L, plain market IOC can
	// take only L's top100, not its worse200, and performs no routing.
	b[1].Levels[0].Price = 10.015
	p, err = PlanDisplayedSweep(b, r)
	if err != nil || p.LocalFilled != 100 || p.LocalCanceled != 250 {
		t.Fatalf("NBBO-only IOC: %+v %v", p, err)
	}
}

func TestDisplayedSweepBuyMirrorOwnershipAndInvalid(t *testing.T) {
	b := isoSourceBooks()
	for i := range b {
		for j := range b[i].Levels {
			b[i].Levels[j].Price = 20 - b[i].Levels[j].Price
		}
	}
	r := DisplayedSweepRequest{Kind: ISOLimitIOC, LocalVenue: "L", Instrument: "X", Side: 1, Quantity: 300, LimitPrice: 10, SubmitMS: 103, MaxAgeMS: 3}
	p, err := PlanDisplayedSweep(b, r)
	if err != nil || p.LocalFilled != 300 || len(p.ProtectedOrders) != 1 {
		t.Fatalf("buy mirror: %+v %v", p, err)
	}
	p.LocalFills[0].Price = 0
	if b[0].Levels[0].Price == 0 {
		t.Fatal("plan aliases depth")
	}
	for _, change := range []func([]DisplayedDepth){
		func(x []DisplayedDepth) { x[1].ReceivedMS = 104 },
		func(x []DisplayedDepth) { x[1].EventMS = 99 },
		func(x []DisplayedDepth) { x[1].Venue = "L" },
		func(x []DisplayedDepth) { x[1].Instrument = "Y" },
		func(x []DisplayedDepth) { x[0].Levels[1].Price = x[0].Levels[0].Price },
		func(x []DisplayedDepth) { x[0].Levels[0].Quantity = math.NaN() },
	} {
		x := isoSourceBooks()
		for i := range x {
			for j := range x[i].Levels {
				x[i].Levels[j].Price = 20 - x[i].Levels[j].Price
			}
		}
		change(x)
		if _, err := PlanDisplayedSweep(x, r); err == nil {
			t.Fatal("invalid/stale/unreceived source accepted")
		}
	}
}

func TestDisplayedSweepSourceExerciseMarketIOC(t *testing.T) {
	// Exercise6.2 table: L100@10.03/200@10.00; N100@10.02/300@10.01.
	books := []DisplayedDepth{
		{Venue: "L", Instrument: "X", EventMS: 1, AvailableMS: 1, ReceivedMS: 1, Protected: true, Levels: []DisplayedLevel{{10.03, 100}, {10, 200}}},
		{Venue: "N", Instrument: "X", EventMS: 1, AvailableMS: 1, ReceivedMS: 1, Protected: true, Levels: []DisplayedLevel{{10.02, 100}, {10.01, 300}}},
	}
	r := DisplayedSweepRequest{Kind: LocalMarketIOC, LocalVenue: "L", Instrument: "X", Side: -1, Quantity: 300, SubmitMS: 1}
	p, err := PlanDisplayedSweep(books, r)
	if err != nil || p.LocalFilled != 100 || p.LocalCanceled != 200 || len(p.LocalFills) != 1 || p.LocalFills[0].Price != 10.03 {
		t.Fatalf("exercise6.2: %+v %v", p, err)
	}
}

func TestDarkPoolProRataSourceAndFiniteScale(t *testing.T) {
	r, err := AllocateDarkPoolProRata([]float64{1000, 2000}, 1500, 10, 10.02)
	if err != nil || !reflect.DeepEqual(r.Fills, []float64{500, 1000}) || r.IncomingRemaining != 0 || r.MidPrice != 10.01 {
		t.Fatalf("source p172: %+v %v", r, err)
	}
	r, err = AllocateDarkPoolProRata([]float64{1000, 2000}, 4000, 10, 10.02)
	if err != nil || r.IncomingRemaining != 1000 {
		t.Fatalf("resting exhaustion: %+v %v", r, err)
	}
	r, err = AllocateDarkPoolProRata([]float64{1e308, 1e308}, 1e308, 1, 2)
	if err != nil || r.Fills[0] != 5e307 || r.Fills[1] != 5e307 || r.IncomingRemaining != 0 {
		t.Fatalf("finite scale: %+v %v", r, err)
	}
	if _, err := AllocateDarkPoolProRata([]float64{1}, 1, 3, 2); err == nil {
		t.Fatal("crossed BBO accepted")
	}
}

func TestDarkPoolProRataRepresentableSmallAllocations(t *testing.T) {
	for _, incoming := range []float64{1e-20, 1e-10} {
		r, err := AllocateDarkPoolProRata([]float64{1e308, 1e308}, incoming, 1, 2)
		if err != nil || r.Fills[0] != incoming/2 || r.Fills[1] != incoming/2 || r.IncomingRemaining != 0 {
			t.Fatalf("equal-share incoming %g: %+v %v", incoming, r, err)
		}
	}
	// A tiny resting order still receives half its representable quantity when
	// the incoming order consumes half the available capacity.
	r, err := AllocateDarkPoolProRata([]float64{1e-20, 1e308}, 5e307, 1, 2)
	if err != nil || r.Fills[0] != 5e-21 || r.Fills[1] != 5e307 {
		t.Fatalf("small resting order: %+v %v", r, err)
	}
	for _, incoming := range []float64{math.SmallestNonzeroFloat64, 3 * math.SmallestNonzeroFloat64, 5 * math.SmallestNonzeroFloat64} {
		if _, err := AllocateDarkPoolProRata([]float64{1e308, 1e308}, incoming, 1, 2); err == nil {
			t.Fatalf("unrepresentable equal allocation accepted for %g", incoming)
		}
	}
	incoming := 2 * math.SmallestNonzeroFloat64
	r, err = AllocateDarkPoolProRata([]float64{1e308, 1e308}, incoming, 1, 2)
	if err != nil || r.Fills[0] != math.SmallestNonzeroFloat64 || r.Fills[1] != math.SmallestNonzeroFloat64 || r.IncomingFilled != incoming {
		t.Fatalf("representable subnormal equal allocation: %+v %v", r, err)
	}
}
