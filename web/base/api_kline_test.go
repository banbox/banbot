package base

import (
	"context"
	"testing"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func TestResolveSeriesQueryInfoProjectsRegisteredFields(t *testing.T) {
	info := orm.NewSeriesInfo("web_series_query_test", "1h", []orm.SeriesField{
		{Name: "rate", Type: "float"},
		{Name: "label", Type: "string"},
	})
	if err := data.RegisterFuncDataSource(info,
		func(context.Context, *strat.DataSub, int64, int64) ([]*orm.DataRecord, error) { return nil, nil }, nil); err != nil {
		t.Fatal(err)
	}

	got, fields, err := resolveSeriesQueryInfo(info.Name, "1h", []string{"label"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Binding.Table != info.Binding.Table || len(got.Binding.Fields) != 1 || got.Binding.Fields[0].Name != "label" {
		t.Fatalf("unexpected projected series info: %+v", got)
	}
	if len(fields) != 1 || fields[0] != "label" {
		t.Fatalf("unexpected selected fields: %v", fields)
	}
	if _, _, err := resolveSeriesQueryInfo(info.Name, "1h", []string{"missing"}); err == nil {
		t.Fatal("expected unknown field error")
	}
	if _, _, err := resolveSeriesQueryInfo(info.Name, "5m", nil); err == nil {
		t.Fatal("expected timeframe mismatch error")
	}
}

func TestResolveSeriesQueryInfoBuildsKlineMetadata(t *testing.T) {
	info, fields, err := resolveSeriesQueryInfo("kline", "15m", []string{"close", "trade_num"})
	if err != nil {
		t.Fatal(err)
	}
	if info.Binding.Table != "kline_15m" || len(info.Binding.Fields) != 2 {
		t.Fatalf("unexpected kline info: %+v", info)
	}
	if info.Binding.Fields[1].Type != "int" {
		t.Fatalf("trade_num type = %q, want int", info.Binding.Fields[1].Type)
	}
	if len(fields) != 2 || fields[0] != "close" || fields[1] != "trade_num" {
		t.Fatalf("unexpected fields: %v", fields)
	}
}
