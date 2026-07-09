package data

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func TestNormalizeDataSub(t *testing.T) {
	info := orm.NewSeriesInfo("custom_metric", "1h", []orm.SeriesField{{Name: "value", Type: "float"}})
	sub := &strat.DataSub{ExSymbol: &orm.ExSymbol{Symbol: "BTCUSDT"}}
	got, err := NormalizeDataSub(info, sub)
	if err != nil {
		t.Fatalf("NormalizeDataSub failed: %v", err)
	}
	if got == sub {
		t.Fatalf("expected a copy")
	}
	if got.Source != info.Name || got.TimeFrame != info.TimeFrame || got.ExSymbol != sub.ExSymbol {
		t.Fatalf("unexpected normalized sub: %+v", got)
	}
}

func TestNormalizeDataSubRejectsBadInput(t *testing.T) {
	info := orm.NewSeriesInfo("custom_metric", "1h", []orm.SeriesField{{Name: "value", Type: "float"}})
	cases := []struct {
		name string
		sub  *strat.DataSub
		want string
	}{
		{"nil sub", nil, "data sub is required"},
		{"missing exsymbol", &strat.DataSub{}, "exsymbol is required"},
		{"missing symbol", &strat.DataSub{ExSymbol: &orm.ExSymbol{}}, "symbol is required"},
		{"wrong source", &strat.DataSub{Source: "other", ExSymbol: &orm.ExSymbol{Symbol: "BTCUSDT"}}, `unsupported source "other"`},
		{"wrong timeframe", &strat.DataSub{TimeFrame: "4h", ExSymbol: &orm.ExSymbol{Symbol: "BTCUSDT"}}, `unsupported timeframe "4h"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeDataSub(info, tc.sub)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestParseJSONNumbers(t *testing.T) {
	num, err := ParseJSONFloat(json.RawMessage(`"1.25"`), "rate")
	if err != nil || num != 1.25 {
		t.Fatalf("unexpected string float parse: %v %v", num, err)
	}
	num, err = ParseJSONFloat(json.RawMessage(`1.5`), "rate")
	if err != nil || num != 1.5 {
		t.Fatalf("unexpected numeric float parse: %v %v", num, err)
	}
	ival, err := ParseJSONInt(json.RawMessage(`"1704067200000"`), "timestamp")
	if err != nil || ival != 1704067200000 {
		t.Fatalf("unexpected string int parse: %v %v", ival, err)
	}
	ival, err = ParseJSONInt(json.RawMessage(`1704067200001`), "timestamp")
	if err != nil || ival != 1704067200001 {
		t.Fatalf("unexpected numeric int parse: %v %v", ival, err)
	}
}
