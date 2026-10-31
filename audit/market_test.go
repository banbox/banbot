package audit

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

func auditRow(sid int32, values map[string]any) *orm.DataSeries {
	return &orm.DataSeries{Source: "kline", TimeFrame: "1m", Sid: sid, TimeMS: 60000, EndMS: 120000, Closed: true, Values: values}
}

func TestStageRefreshPreservesCustomTypesNullAndInputs(t *testing.T) {
	baseline := auditRow(1, map[string]any{"close": 10.0, "custom": int64(1<<53 + 1), "null": nil, "changed": int64(2)})
	candidate := auditRow(1, map[string]any{"close": 11.0, "changed": float64(2), "explicit_null": nil})
	rows, report, err := StageRefresh([]*orm.DataSeries{baseline}, []*orm.DataSeries{candidate})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Changes) != 1 || len(rows) != 1 {
		t.Fatalf("report=%+v rows=%v", report, rows)
	}
	got := rows[0].Series.Values
	if got["custom"] != int64(1<<53+1) {
		t.Fatalf("custom=%T %v", got["custom"], got["custom"])
	}
	if v, ok := got["null"]; !ok || v != nil {
		t.Fatal("NULL removed")
	}
	if _, ok := got["changed"].(float64); !ok {
		t.Fatal("type change hidden")
	}
	if _, ok := baseline.Values["explicit_null"]; ok || baseline.Values["close"] != 10.0 {
		t.Fatal("baseline mutated")
	}
	store, _ := factor.NewVersionStore(10)
	if err := store.Put(rows[0]); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "review.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	reopened, err := factor.OpenVersionStore(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	roundtrip, err := reopened.Records()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundtrip[0].Series.Values, got) {
		t.Fatal("staged file lost types/NULL")
	}
}

func TestStageRefreshDoesNotDeleteMissingRowsAndRejectsDuplicates(t *testing.T) {
	base := auditRow(1, map[string]any{"close": 10.0})
	rows, report, err := StageRefresh([]*orm.DataSeries{base}, nil)
	if err != nil || len(rows) != 0 || report.BaselineOnly != 1 {
		t.Fatalf("rows=%v report=%+v err=%v", rows, report, err)
	}
	if _, _, err := StageRefresh(nil, []*orm.DataSeries{base, base}); err == nil {
		t.Fatal("duplicate candidate accepted")
	}
}

func TestDetectFlatMarketRequiresExplicitThresholdAndDistinctSymbols(t *testing.T) {
	flat := func(sid int32) *orm.DataSeries {
		return auditRow(sid, map[string]any{"open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 0.0})
	}
	rows := []*orm.DataSeries{flat(1), flat(2), flat(3), auditRow(4, map[string]any{"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 1.0})}
	alerts, err := DetectFlatMarket(rows, FlatPolicy{MinSymbols: 3, MinFraction: .75})
	if err != nil || len(alerts) != 1 || alerts[0].FlatSymbols != 3 || alerts[0].ObservedSymbols != 4 {
		t.Fatalf("alerts=%+v err=%v", alerts, err)
	}
	alerts, err = DetectFlatMarket(rows, FlatPolicy{MinSymbols: 3, MinFraction: .8})
	if err != nil || len(alerts) != 0 {
		t.Fatalf("threshold ignored: %+v %v", alerts, err)
	}
	if _, err := DetectFlatMarket(append(rows, rows[0]), FlatPolicy{MinSymbols: 3, MinFraction: .75}); err == nil {
		t.Fatal("duplicate counted twice")
	}
	if _, err := DetectFlatMarket(rows, FlatPolicy{}); err == nil {
		t.Fatal("unspecified policy accepted")
	}
}

func TestFlatWarningDoesNotRoundLargeIntegersOrTreatNullAsZero(t *testing.T) {
	values := map[string]any{"open": int64(1 << 53), "high": int64(1<<53 + 1), "low": int64(1 << 53), "close": int64(1 << 53), "volume": 0}
	if isFlatZeroVolume(values) {
		t.Fatal("rounded a real price change into a flat bar")
	}
	values["high"] = int64(1 << 53)
	if !isFlatZeroVolume(values) {
		t.Fatal("valid typed flat bar missed")
	}
	values["volume"] = nil
	if isFlatZeroVolume(values) {
		t.Fatal("NULL volume treated as zero")
	}
	delete(values, "volume")
	if isFlatZeroVolume(values) {
		t.Fatal("missing volume treated as zero")
	}
}
