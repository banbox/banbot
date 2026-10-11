package statistics

import (
	"math"
	"reflect"
	"testing"
)

func chanBookTape() []ChanBookMessage {
	rows := []ChanBookMessage{
		{OrderID: "b1", Side: "buy", Action: "add", PriceTicks: 100, Quantity: 10},
		{OrderID: "b2", Side: "buy", Action: "add", PriceTicks: 100, Quantity: 5},
		{OrderID: "b3", Side: "buy", Action: "add", PriceTicks: 99, Quantity: 8},
		{OrderID: "a1", Side: "sell", Action: "add", PriceTicks: 102, Quantity: 7},
		{OrderID: "a2", Side: "sell", Action: "add", PriceTicks: 101, Quantity: 4},
		{OrderID: "b1", Side: "buy", Action: "partial_fill", PriceTicks: 100, Quantity: 3},
		{OrderID: "b2", Side: "buy", Action: "cancel", PriceTicks: 100, Quantity: 5},
		{OrderID: "b1", Side: "buy", Action: "fill", PriceTicks: 100, Quantity: 7},
		{OrderID: "a2", Side: "sell", Action: "fill", PriceTicks: 101, Quantity: 4},
		{OrderID: "b3", Side: "buy", Action: "cancel", PriceTicks: 99, Quantity: 8},
	}
	for i := range rows {
		rows[i].Venue = "V"
		rows[i].Instrument = "BTC"
		rows[i].Owner = "P"
		rows[i].Sequence = uint64(i + 1)
		rows[i].EventMS = int64(i + 1)
		rows[i].AvailableMS = int64(i + 2)
		rows[i].ReceivedMS = int64(i + 3)
	}
	return rows
}

func chanBookBounds() ChanBookLimits { return ChanBookLimits{100, 100, 5, 100000, 0.01, 0.001} }

func TestChanBookIndependentDepthOracle(t *testing.T) {
	r, err := ReconstructChanOrderBook(chanBookTape(), 100, chanBookBounds())
	if err != nil {
		t.Fatal(err)
	}
	// Hand-accounted best price and aggregate quantity after each event.
	wantB := [][2]int64{{100, 10}, {100, 15}, {100, 15}, {100, 15}, {100, 15}, {100, 12}, {100, 7}, {99, 8}, {99, 8}, {0, 0}}
	wantA := [][2]int64{{0, 0}, {0, 0}, {0, 0}, {102, 7}, {101, 4}, {101, 4}, {101, 4}, {101, 4}, {102, 7}, {102, 7}}
	for i, s := range r.Snapshots {
		for j, side := range [][]ChanBookLevel{s.Bids, s.Asks} {
			want := wantB[i]
			if j == 1 {
				want = wantA[i]
			}
			if want[1] == 0 {
				if len(side) != 0 {
					t.Fatal(i, side)
				}
			} else if len(side) == 0 || side[0].PriceTicks != want[0] || side[0].Quantity != want[1] {
				t.Fatal(i, side, want)
			}
		}
	}
	if !reflect.DeepEqual(r.Snapshots[4].Bids, []ChanBookLevel{{100, 15, 2}, {99, 8, 1}}) || !reflect.DeepEqual(r.Snapshots[4].Asks, []ChanBookLevel{{101, 4, 1}, {102, 7, 1}}) {
		t.Fatal(r.Snapshots[4])
	}
	if r.Snapshots[9].ReceivedMS != 12 || r.Processed != 10 {
		t.Fatal(r)
	}
	// Source five-field path shares aggregate oracle but cannot infer order counts.
	levels := make([]ChanPriceLevelMessage, 10)
	for i, m := range chanBookTape() {
		levels[i] = ChanPriceLevelMessage{m.Venue, m.Instrument, m.Action, m.Side, m.PriceTicks, m.Quantity, m.Sequence, m.EventMS, m.AvailableMS, m.ReceivedMS}
	}
	lr, err := ReconstructChanPriceLevels(levels, 100, chanBookBounds())
	if err != nil {
		t.Fatal(err)
	}
	for i := range lr.Snapshots {
		s := r.Snapshots[i]
		s.ActiveOrders = 0
		for j := range s.Bids {
			s.Bids[j].Orders = 0
		}
		for j := range s.Asks {
			s.Asks[j].Orders = 0
		}
		if !reflect.DeepEqual(s, lr.Snapshots[i]) {
			t.Fatal(i, s, lr.Snapshots[i])
		}
	}
}

func TestChanBookReceiptGapPrefix(t *testing.T) {
	rows := chanBookTape()
	rows[1].ReceivedMS = 50
	a, err := ReconstructChanOrderBook(rows, 20, chanBookBounds())
	if err != nil {
		t.Fatal(err)
	}
	rows[2].Quantity = 999
	rows[3].Action = "invalid"
	b, err := ReconstructChanOrderBook(rows, 20, chanBookBounds())
	if err != nil || !reflect.DeepEqual(a, b) || a.Processed != 1 || !a.BlockedByReceipt {
		t.Fatal(a, b, err)
	}
	if a.Snapshots[0].ReceivedMS != 3 {
		t.Fatal(a)
	}
}

func TestChanBookRejectMalformed(t *testing.T) {
	cases := map[string]func([]ChanBookMessage){
		"duplicate":     func(r []ChanBookMessage) { r[1].OrderID = r[0].OrderID },
		"unknown":       func(r []ChanBookMessage) { r[5].OrderID = "missing" },
		"owner":         func(r []ChanBookMessage) { r[5].Owner = "other" },
		"price":         func(r []ChanBookMessage) { r[5].PriceTicks = 99 },
		"side":          func(r []ChanBookMessage) { r[5].Side = "sell" },
		"overfill":      func(r []ChanBookMessage) { r[5].Quantity = 11 },
		"partial all":   func(r []ChanBookMessage) { r[5].Quantity = 10 },
		"fill short":    func(r []ChanBookMessage) { r[5].Action = "fill" },
		"sequence gap":  func(r []ChanBookMessage) { r[1].Sequence = 3 },
		"clock":         func(r []ChanBookMessage) { r[1].ReceivedMS = 0 },
		"scope":         func(r []ChanBookMessage) { r[1].Venue = "other" },
		"overflow":      func(r []ChanBookMessage) { r[0].Quantity = math.MaxInt64 },
		"reused closed": func(r []ChanBookMessage) { r[9].Action = "add"; r[9].OrderID = "b1" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			r := chanBookTape()
			change(r)
			if _, err := ReconstructChanOrderBook(r, 100, chanBookBounds()); err == nil {
				t.Fatal("accepted", name)
			}
		})
	}
	for _, limits := range []ChanBookLimits{{1, 100, 1, 1000, 0.01, 0.001}, {100, 1, 1, 1000, 0.01, 0.001}, {100, 100, 5, 1, 0.01, 0.001}} {
		if _, err := ReconstructChanOrderBook(chanBookTape(), 100, limits); err == nil {
			t.Fatal(limits)
		}
	}
}

func TestChanPriceLevelsStrictSourceDecrements(t *testing.T) {
	rows := []ChanPriceLevelMessage{{"V", "BTC", "add", "buy", -100, 125, 1, 1, 1, 2}, {"V", "BTC", "add", "buy", -100, 25, 2, 2, 2, 3}, {"V", "BTC", "fill", "buy", -100, 50, 3, 3, 3, 4}}
	r, err := ReconstructChanPriceLevels(rows, 4, chanBookBounds())
	if err != nil || r.Snapshots[2].Bids[0].Quantity != 100 {
		t.Fatal(r, err)
	}
	// Example fractional BTC sizes: 125 lots at 0.001 BTC/lot = 0.125 BTC.
	rows[2].Quantity = 151
	if _, err = ReconstructChanPriceLevels(rows, 4, chanBookBounds()); err == nil {
		t.Fatal("overdraw accepted")
	}
	rows[2].PriceTicks = 1
	if _, err = ReconstructChanPriceLevels(rows, 4, chanBookBounds()); err == nil {
		t.Fatal("unknown level accepted")
	}
	rows[1].ReceivedMS = 20
	a, err := ReconstructChanPriceLevels(rows, 4, chanBookBounds())
	if err != nil {
		t.Fatal(err)
	}
	rows[2].Action = "invalid"
	b, err := ReconstructChanPriceLevels(rows, 4, chanBookBounds())
	if err != nil || !reflect.DeepEqual(a, b) || a.Processed != 1 {
		t.Fatal(a, b, err)
	}
}

func TestChanBookDeclaredScalesDepthAndTiedTimes(t *testing.T) {
	rows := chanBookTape()
	for i := range rows {
		rows[i].EventMS = 1
	}
	limits := chanBookBounds()
	limits.Depth = 1
	r, err := ReconstructChanOrderBook(rows, 100, limits)
	if err != nil || len(r.Snapshots[4].Bids) != 1 || len(r.Snapshots[4].Asks) != 1 || r.QuantityLotSize != 0.001 || r.PriceTickSize != 0.01 {
		t.Fatal(r, err)
	}
	for _, size := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		limits.PriceTickSize = size
		if _, err := ReconstructChanOrderBook(rows, 100, limits); err == nil {
			t.Fatal("invalid scale", size)
		}
	}
	rows = chanBookTape()
	rows[1].AvailableMS = 50
	r, err = ReconstructChanOrderBook(rows, 20, chanBookBounds())
	if err != nil || r.Processed != 1 || !r.BlockedByReceipt {
		t.Fatal(r, err)
	}
}

func TestChanBookRejectReceiptBeforePublication(t *testing.T) {
	rows := chanBookTape()[:1]
	rows[0].AvailableMS, rows[0].ReceivedMS = 3, 2
	if _, err := ReconstructChanOrderBook(rows, 10, chanBookBounds()); err == nil {
		t.Fatal("order-ID book accepted receipt before publication")
	}
	m := rows[0]
	levels := []ChanPriceLevelMessage{{m.Venue, m.Instrument, m.Action, m.Side, m.PriceTicks, m.Quantity, m.Sequence, m.EventMS, m.AvailableMS, m.ReceivedMS}}
	if _, err := ReconstructChanPriceLevels(levels, 10, chanBookBounds()); err == nil {
		t.Fatal("price-level book accepted receipt before publication")
	}
}
