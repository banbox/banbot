package orm

import (
	"context"
	"testing"
)

func TestBoundSeriesStorePreservesStoreContract(t *testing.T) {
	repo := &stubStoreRepo{}
	store := NewSeriesStore(repo)
	info := testSeriesInfo("macro")
	target := &ExSymbol{ID: 7}
	bound := store.Bind(info, target)
	ctx := context.Background()
	values := map[string]any{"value": nil, "count": int64(9007199254740993), "label": "release", "valid": false}
	row := &DataRecord{TimeMS: 60_000, EndMS: 120_000, Values: values}
	if err := bound.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bound.Write(ctx, row); err != nil {
		t.Fatal(err)
	}
	if row.Sid != 0 || repo.inserted[0].Sid != target.ID || repo.coverageCalls != 1 {
		t.Fatal("binding changed normalization, input ownership or coverage")
	}
	repo.queryRows = repo.inserted
	rows, err := bound.Read(ctx, 60_000, 120_000, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("read: %v, %v", rows, err)
	}
	got := rows[0]
	if got.Source != info.Name || got.TimeFrame != info.TimeFrame || got.ExSymbol != target {
		t.Fatalf("identity lost: %+v", got)
	}
	if value, ok := got.Values["value"]; !ok || value != nil {
		t.Fatal("explicit NULL lost")
	}
	if _, ok := got.Values["missing"]; ok || got.Values["count"] != int64(9007199254740993) || got.Values["valid"] != false {
		t.Fatal("raw value semantics changed")
	}
	// Binding does not clone or narrow the arbitrary field map.
	values["extra"] = "kept"
	if got.Values["extra"] != "kept" {
		t.Fatal("unexpected map copy")
	}
	if _, err := store.Bind(info, nil).Read(ctx, 0, 1, 1); err == nil {
		t.Fatal("missing target must still fail validation")
	}
	if err := store.Bind(nil, target).Write(ctx, row); err == nil {
		t.Fatal("missing schema must still fail validation")
	}
	// An explicitly answered empty interval must reach coverage unchanged.
	if err := bound.UpdateCoverage(ctx, 120_000, 180_000, nil); err != nil {
		t.Fatal(err)
	}
	if repo.coverageStart != 120_000 || repo.coverageEnd != 180_000 || len(repo.coverageRows) != 0 {
		t.Fatal("answered empty interval changed")
	}
}

func BenchmarkSeriesStoreAccess(b *testing.B) {
	ctx := context.Background()
	info := testSeriesInfo("macro")
	target := &ExSymbol{ID: 7}
	repo := &stubStoreRepo{queryRows: []*DataRecord{{Sid: 7, TimeMS: 1, EndMS: 2, Values: map[string]any{"value": 1.0, "null": nil}}}}
	store := NewSeriesStore(repo)
	bound := store.Bind(info, target)
	b.Run("Read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := store.Read(ctx, info, target, 1, 2, 1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("BoundRead", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := bound.Read(ctx, 1, 2, 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}
