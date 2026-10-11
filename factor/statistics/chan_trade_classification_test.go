package statistics

import (
	"reflect"
	"testing"
)

func tradeClassificationFixture() []MatchedTradeQuote {
	prices := []float64{100, 101, 101, 99, 99, 100}
	mids := []float64{100, 100, 101, 100, 99, 100}
	var rows []MatchedTradeQuote
	for i, p := range prices {
		at := int64(10 + i)
		rows = append(rows, MatchedTradeQuote{Sequence: int64(50 + i), EventMS: at, AvailableMS: at, ReceivedMS: at, Price: p, Quantity: 2, QuoteEventMS: at - 1, QuoteAvailableMS: at - 1, QuoteReceivedMS: at, Bid: mids[i] - 1, Ask: mids[i] + 1})
	}
	return rows
}

func TestMatchedTradesSourceQuoteAndLeeReady(t *testing.T) {
	rows := tradeClassificationFixture()
	r, err := ClassifyMatchedTrades(rows, -1, 0, 2, 15)
	if err != nil {
		t.Fatal(err)
	}
	wantTick := []int{-1, 1, 1, -1, -1, 1}
	wantQuote := []int{0, 1, 0, -1, 0, 0}
	for i, row := range r.Rows {
		if row.TickSign != wantTick[i] || row.QuoteSign != wantQuote[i] || row.LeeReadySign != wantTick[i] || row.QuoteFlow != 2*float64(wantQuote[i]) {
			t.Fatalf("row%d %+v", i, row)
		}
	}
	for n := 1; n <= len(rows); n++ {
		prefix, err := ClassifyMatchedTrades(rows[:n], -1, 0, 2, 15)
		if err != nil || !reflect.DeepEqual(prefix.Rows, r.Rows[:n]) {
			t.Fatalf("prefix%d %v", n, err)
		}
	}
	// No source universal5-second lag is invented; the declared lag controls
	// quote matching and rejects a too-recent quote when explicitly requested.
	if _, err := ClassifyMatchedTrades(rows, -1, 2, 3, 15); err == nil {
		t.Fatal("quote lag ignored")
	}
}

func TestMatchedTradesPendingPrefixAndDependencyClocks(t *testing.T) {
	rows := tradeClassificationFixture()
	rows[0].QuoteReceivedMS = 14
	r, err := ClassifyMatchedTrades(rows, -1, 0, 2, 15)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range r.Rows {
		if x.ReceivedMS < 14 {
			t.Fatal("preceding trade-sign receipt lost")
		}
	}
	rows[2].ReceivedMS = 20
	r, err = ClassifyMatchedTrades(rows, -1, 0, 2, 15)
	if err != nil || len(r.Rows) != 2 || !r.PendingPrefix {
		t.Fatalf("unseen predecessor skipped: %+v %v", r, err)
	}
	rows[3].Price = 1e100
	other, err := ClassifyMatchedTrades(rows, -1, 0, 2, 15)
	if err != nil || !reflect.DeepEqual(r, other) {
		t.Fatal("future suffix changed received prefix")
	}
	rows = tradeClassificationFixture()
	rows[1].Sequence++
	if _, err := ClassifyMatchedTrades(rows, -1, 0, 2, 15); err == nil {
		t.Fatal("sequence gap accepted")
	}
	rows = tradeClassificationFixture()
	rows[1].QuoteEventMS = 12
	rows[1].QuoteAvailableMS = 12
	rows[1].QuoteReceivedMS = 12
	if _, err := ClassifyMatchedTrades(rows, -1, 0, 2, 15); err == nil {
		t.Fatal("future quote accepted")
	}
}
