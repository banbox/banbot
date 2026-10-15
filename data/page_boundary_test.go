package data

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

// Return the adapter's actual page without repairing its contract in the fake.
type invalidPageRepo struct {
	*stubSeriesRepo
	page []*orm.DataRecord
}

func (r *invalidPageRepo) QuerySeriesRange(context.Context, *orm.SeriesInfo, int32, int64, int64, int) ([]*orm.DataRecord, *errs.Error) {
	return r.page, nil
}

func TestRepositoryPagesRejectInvalidAdapterOutput(t *testing.T) {
	source := newStubRegistrySource("adapter_page")
	source.info.TimeFrame = "event"
	catalog := NewDataSourceCatalog()
	if err := catalog.RegisterDataSource(source); err != nil {
		t.Fatal(err)
	}
	sub := Subscription{Source: source.info.Name, ExSymbol: &orm.ExSymbol{ID: 7}, TimeFrame: "event"}
	valid := &orm.DataRecord{Sid: 7, TimeMS: 10, EndMS: 11, Values: map[string]any{"value": int64(9007199254740993), "nullable": nil}}
	for name, page := range map[string][]*orm.DataRecord{
		"too many":  {valid, {Sid: 7, TimeMS: 20, EndMS: 21}, {Sid: 7, TimeMS: 30, EndMS: 31}},
		"duplicate": {valid, valid}, "nil": {nil},
		"foreign": {{Sid: 8, TimeMS: 10}}, "before start": {{TimeMS: 9}}, "at end": {{TimeMS: 40}},
		"unordered": {{TimeMS: 20}, valid},
	} {
		t.Run(name, func(t *testing.T) {
			repo := &invalidPageRepo{stubSeriesRepo: &stubSeriesRepo{}, page: page}
			rows, err := catalog.ReadSubscriptionPage(context.Background(), repo, nil, sub, 10, 40, 2)
			if err == nil || rows != nil {
				t.Errorf("invalid adapter page published: rows=%v err=%v", rows, err)
			}
			called := false
			feeder, err := NewHistSeriesFeeder(repo, source.info, &sub, func(*orm.DataSeries) { called = true }, 0)
			if err != nil {
				t.Fatal(err)
			}
			feeder.BatchRows = 2
			feeder.SetEndMS(40)
			feeder.SetSeek(10)
			if err := feeder.RunBatch(feeder.GetBatch()); err == nil || called || len(feeder.rows) > 0 {
				t.Errorf("invalid feeder page reached callback: err=%v called=%v retained=%d", err, called, len(feeder.rows))
			}
		})
	}

	repo := &invalidPageRepo{stubSeriesRepo: &stubSeriesRepo{}, page: []*orm.DataRecord{valid}}
	rows, err := catalog.ReadSubscriptionPage(context.Background(), repo, nil, sub, 10, 40, 2)
	if err != nil || len(rows) != 1 || rows[0].Values["value"] != int64(9007199254740993) {
		t.Fatalf("valid concrete payload rejected: rows=%v err=%v", rows, err)
	}
	if value, present := rows[0].Values["nullable"]; !present || value != nil {
		t.Fatal("explicit NULL lost")
	}
}

func TestKlineLoaderReleasesExhaustedPageAtEndBoundary(t *testing.T) {
	row := &orm.DataSeries{TimeMS: 0, EndMS: 60_000, Values: map[string]any{"custom": "wide"}}
	for _, limits := range []struct {
		rows  int
		bytes int64
	}{{1, 0}, {0, 1000}, {0, 0}} {
		loader := &TfSeriesLoader{BatchRows: limits.rows, BatchBytes: limits.bytes, EndMS: 60_000, caches: []*orm.DataSeries{row}, nextMS: 60_000}
		loader.SetNext()
		if loader.caches != nil || loader.GetRow() != nil || loader.nextMS != math.MaxInt64 || loader.offsetMS != 60_000 {
			t.Fatalf("finished loader retained page or lost resume cursor: %+v", loader)
		}
		if row.Values["custom"] != "wide" {
			t.Fatal("releasing page changed previously emitted record")
		}
	}
}

func TestHistoryPageConsumerCancellationAtEOF(t *testing.T) {
	for _, paged := range []bool{false, true} {
		base := newStubRegistrySource("final_cancel")
		base.rows = []*orm.DataRecord{{TimeMS: 39, EndMS: 40}}
		var source DataSource = base
		if paged {
			source = &pagedTestSource{stubSeriesSource: base, page: func(context.Context, int64, int64, int) ([]*orm.DataRecord, error) { return base.rows, nil }}
		}
		ctx, cancel := context.WithCancel(context.Background())
		err := ReadSourceHistory(ctx, source, &Subscription{Source: base.info.Name, ExSymbol: &orm.ExSymbol{ID: 7}}, 10, 40, 1, func([]*orm.DataRecord) error {
			cancel()
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("paged=%v completed after consumer cancellation: %v", paged, err)
		}
	}
}
