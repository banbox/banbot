package orm

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestSeriesPageByteBudgetPreservesConcreteValuesAndNulls(t *testing.T) {
	var typedNull *int64
	values := map[string]any{"integer": int64(9007199254740993), "nullable": nil, "typed_null": typedNull, "nil_slice": []int64(nil), "json": map[string]any{"flag": true, "data": []byte{1, 2, 3}}, "text": "宽字段"}
	row := &DataRecord{Sid: 1, TimeMS: 10, EndMS: 11, Values: values}
	size, err := DataRecordBytes(row)
	if err != nil || size <= 0 {
		t.Fatalf("typed bytes=%d error=%v", size, err)
	}
	if err := CheckDataRecordBytes(WithSeriesReadByteLimit(context.Background(), size), []*DataRecord{row}); err != nil {
		t.Fatal(err)
	}
	err = CheckDataRecordBytes(WithSeriesReadByteLimit(context.Background(), size-1), []*DataRecord{row})
	var budgetErr *SeriesByteLimitError
	if !errors.As(err, &budgetErr) || budgetErr.Limit != size-1 || budgetErr.Observed != size {
		t.Fatalf("budget error=%v", err)
	}
	if !reflect.DeepEqual(row.Values, values) || row.Values["integer"] != int64(9007199254740993) {
		t.Fatal("budget validation changed typed input")
	}
	if _, present := row.Values["missing"]; present {
		t.Fatal("budget validation inserted a missing key")
	}
	withNull, _ := DataRecordBytes(&DataRecord{Values: map[string]any{"nullable": nil}})
	withoutNull, _ := DataRecordBytes(&DataRecord{Values: map[string]any{}})
	if withNull <= withoutNull {
		t.Fatal("explicit NULL and missing counted as the same payload")
	}
}

func TestSeriesPageByteBudgetCountsWideNestedFieldsAndAllRows(t *testing.T) {
	narrow := &DataRecord{Values: map[string]any{"payload": "small"}}
	wide := &DataRecord{Values: map[string]any{"payload": strings.Repeat("x", 1<<20)}}
	narrowBytes, _ := DataRecordBytes(narrow)
	wideBytes, _ := DataRecordBytes(wide)
	if wideBytes-narrowBytes != (1<<20)-5 {
		t.Fatalf("string width missing: narrow=%d wide=%d", narrowBytes, wideBytes)
	}
	ctx := WithSeriesReadByteLimit(context.Background(), narrowBytes)
	if err := CheckDataRecordBytes(ctx, []*DataRecord{narrow, narrow}); err == nil {
		t.Fatal("page counted each row against a fresh limit")
	}
	if err := CheckDataRecordBytes(context.Background(), []*DataRecord{wide}); err != nil {
		t.Fatal("default-disabled budget rejected legacy wide fields")
	}
}

func TestSeriesPageByteBudgetRejectsCyclesWithoutRecursingForever(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	row := &DataRecord{Values: cyclic}
	if err := CheckDataRecordBytes(WithSeriesReadByteLimit(context.Background(), 10000), []*DataRecord{row}); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cyclic byte accounting error=%v", err)
	}
	if err := CheckDataRecordBytes(context.Background(), []*DataRecord{row}); err != nil {
		t.Fatal("disabled budget narrowed legacy Values")
	}
}

func TestSeriesPageByteBudgetAcceptsTypedPointerAliases(t *testing.T) {
	payload := &struct {
		Integer int64
		Alias   *int64
	}{Integer: 9007199254740993}
	payload.Alias = &payload.Integer // Same address, distinct concrete pointer types.
	row := &DataRecord{Values: map[string]any{"payload": payload, "nullable": nil}}
	size, err := DataRecordBytes(row)
	if err != nil {
		t.Fatalf("acyclic concrete pointer alias rejected: %v", err)
	}
	if err := CheckDataRecordBytes(WithSeriesReadByteLimit(context.Background(), size), []*DataRecord{row}); err != nil {
		t.Fatal(err)
	}
	if row.Values["payload"] != payload || *payload.Alias != int64(9007199254740993) {
		t.Fatal("byte accounting changed payload identity or integer type")
	}
}

type budgetTrackingRows struct {
	*interfaceRows
	closed  bool
	scanned int
}

func (r *budgetTrackingRows) Close() { r.closed = true }
func (r *budgetTrackingRows) Scan(dest ...any) error {
	r.scanned++
	return r.interfaceRows.Scan(dest...)
}

func TestSeriesPageByteBudgetSQLMapperStopsBeforeRetainingPartialPage(t *testing.T) {
	fields := NormalizeSeriesFields(SeriesSourceKline, []string{"signal"})
	makeRow := func(stamp int64, signal string) []any {
		row := make([]any, len(fields)+1)
		row[0] = stamp
		for i, field := range fields {
			if field == "signal" {
				row[i+1] = signal
			}
		}
		return row
	}
	baseline := newInterfaceRows([][]any{makeRow(1000, "small")})
	first, err := mapToSeriesFields(nil, "1m", fields, baseline, nil)
	if err != nil {
		t.Fatal(err)
	}
	limit, err := DataSeriesBytes(first[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, wideFirst := range []bool{false, true} {
		rows := [][]any{makeRow(1000, "small"), makeRow(2000, strings.Repeat("x", 10000)), makeRow(3000, "unread")}
		expectedScans := 2
		if wideFirst {
			rows[0] = rows[1]
			expectedScans = 1
		}
		tracked := &budgetTrackingRows{interfaceRows: newInterfaceRows(rows)}
		got, err := mapToSeriesFieldsWithContext(WithSeriesReadByteLimit(context.Background(), limit), nil, "1m", fields, tracked, nil)
		var budgetErr *SeriesByteLimitError
		if !errors.As(err, &budgetErr) || got != nil || !tracked.closed || tracked.scanned != expectedScans {
			t.Fatalf("oversized SQL page retained/scanned: rows=%v err=%v closed=%v scans=%d", got, err, tracked.closed, tracked.scanned)
		}
	}
}
