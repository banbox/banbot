package data

import "testing"

func TestTradeFeederEndPrefetchDoesNotAdvanceCurrentBatch(t *testing.T) {
	const hourMS = int64(3_600_000)
	feeder := &TradeFeeder{
		nextMS:   23*hourMS + 1,
		endMS:    24 * hourMS,
		offsetMS: 23 * hourMS,
		waitNext: make(chan int, 1),
	}

	feeder.loadNextBatch()

	if feeder.nextMS != 23*hourMS+1 {
		t.Fatalf("end prefetch changed current batch time to %d", feeder.nextMS)
	}
	select {
	case <-feeder.waitNext:
	default:
		t.Fatal("end prefetch did not signal completion")
	}
}
