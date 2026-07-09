package data

import (
	"context"
	"errors"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func TestNewFuncDataSourceWrapsFetchAndOptionalSubscribe(t *testing.T) {
	info := orm.NewSeriesInfo("custom_metric", "1h", []orm.SeriesField{{Name: "value", Type: "float"}})
	src, err := NewFuncDataSource(info,
		func(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
			return []*orm.DataRecord{{TimeMS: startMS, EndMS: endMS, Values: map[string]any{"value": 1.5}}}, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("NewFuncDataSource failed: %v", err)
	}
	if src.Info() != info {
		t.Fatalf("expected source to expose original info")
	}
	rows, err := src.FetchHistory(context.Background(), &strat.DataSub{}, 100, 200)
	if err != nil {
		t.Fatalf("FetchHistory failed: %v", err)
	}
	if len(rows) != 1 || rows[0].Values["value"] != 1.5 {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if err := src.SubscribeLive(context.Background(), nil, nil); err != nil {
		t.Fatalf("nil SubscribeLive should be a no-op, got %v", err)
	}
}

func TestNewFuncDataSourceRequiresValidInputs(t *testing.T) {
	info := orm.NewSeriesInfo("custom_metric", "1h", []orm.SeriesField{{Name: "value", Type: "float"}})
	if _, err := NewFuncDataSource(info, nil, nil); err == nil {
		t.Fatalf("expected missing fetch function to fail")
	}
	if _, err := NewFuncDataSource(nil, func(context.Context, *strat.DataSub, int64, int64) ([]*orm.DataRecord, error) {
		return nil, nil
	}, nil); err == nil {
		t.Fatalf("expected invalid info to fail")
	}
}

func TestFuncDataSourceSubscribeUsesCallback(t *testing.T) {
	info := orm.NewSeriesInfo("custom_metric", "1h", []orm.SeriesField{{Name: "value", Type: "float"}})
	wantErr := errors.New("boom")
	src, err := NewFuncDataSource(info,
		func(context.Context, *strat.DataSub, int64, int64) ([]*orm.DataRecord, error) {
			return nil, nil
		},
		func(context.Context, []*strat.DataSub, DataSink) error {
			return wantErr
		},
	)
	if err != nil {
		t.Fatalf("NewFuncDataSource failed: %v", err)
	}
	if err := src.SubscribeLive(context.Background(), nil, nil); !errors.Is(err, wantErr) {
		t.Fatalf("expected callback error, got %v", err)
	}
}
