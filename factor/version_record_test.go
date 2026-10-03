package factor

import (
	"testing"

	"github.com/banbox/banbot/orm"
)

func versionRecordFixture() VersionRecord {
	var typedNull *int64
	return VersionRecord{Series: orm.DataSeries{Source: "custom", TimeFrame: "event", Sid: 1, TimeMS: 1, EndMS: 2, Closed: true, Values: map[string]any{"large": int64(1<<53 + 7), "null": nil, "typedNull": typedNull, "nested": map[string]any{"items": []int64{1<<53 + 7}, "enabled": true, "name": "asset"}}}, EventTime: 2, AvailableAt: 2, IngestedAt: 2, Revision: 1, SourceVersion: "v1"}
}

func TestVersionStoreSingleRecordBoundary(t *testing.T) {
	row := versionRecordFixture()
	store, _ := NewVersionStore(1)
	if err := store.Put(row); err != nil {
		t.Fatal(err)
	}
	rows, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	values := rows[0].Series.Values
	row.Series.Values["nested"].(map[string]any)["items"].([]int64)[0] = 0
	row.Series.Values["large"] = float64(0)
	if values["large"] != int64(1<<53+7) || values["nested"].(map[string]any)["items"].([]int64)[0] != 1<<53+7 {
		t.Fatal("record shares nested values or loses large integer type")
	}
	if _, ok := values["typedNull"].(*int64); !ok {
		t.Fatal("typed NULL lost concrete type")
	}
	if value, ok := values["null"]; !ok || value != nil {
		t.Fatal("NULL became missing")
	}
}

func TestCloneVersionRecordValidationAndOwnership(t *testing.T) {
	row := versionRecordFixture()
	row.Series.ExSymbol = &orm.ExSymbol{}
	row.Series.Adj = &orm.AdjInfo{}
	copy, err := CloneVersionRecord(row)
	if err != nil {
		t.Fatal(err)
	}
	store, _ := NewVersionStore(1)
	if err := store.Put(row); err != nil {
		t.Fatal(err)
	}
	rows, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	copyHash, err := contentHash(copy)
	if err != nil {
		t.Fatal(err)
	}
	storeHash, err := contentHash(rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if copyHash != storeHash {
		t.Fatal("direct clone differs from the previous store boundary")
	}
	if copy.Series.ExSymbol != nil || copy.Series.Adj != nil || copy.Series.TimeMS != row.Series.TimeMS || copy.Series.EndMS != row.Series.EndMS || copy.Series.Closed != row.Series.Closed {
		t.Fatal("runtime metadata escaped or immutable record metadata changed")
	}
	row.Series.Values["nested"].(map[string]any)["items"].([]int64)[0] = 0
	if copy.Series.Values["nested"].(map[string]any)["items"].([]int64)[0] != 1<<53+7 {
		t.Fatal("clone shares caller-owned nested Values")
	}
	for name, mutate := range map[string]func(*VersionRecord){
		"sid":           func(r *VersionRecord) { r.Series.Sid = 0 },
		"source":        func(r *VersionRecord) { r.Series.Source = "" },
		"frequency":     func(r *VersionRecord) { r.Series.TimeFrame = "" },
		"sourceVersion": func(r *VersionRecord) { r.SourceVersion = "" },
		"revision":      func(r *VersionRecord) { r.Revision = 0 },
		"available":     func(r *VersionRecord) { r.AvailableAt = -1 },
		"ingested":      func(r *VersionRecord) { r.IngestedAt = -1 },
		"cycle":         func(r *VersionRecord) { r.Series.Values["cycle"] = r.Series.Values },
		"unsupported":   func(r *VersionRecord) { r.Series.Values["callback"] = func() {} },
	} {
		t.Run(name, func(t *testing.T) {
			bad := versionRecordFixture()
			mutate(&bad)
			if _, err := CloneVersionRecord(bad); err == nil {
				t.Fatal("invalid observation accepted")
			}
			if err := store.Put(bad); err == nil {
				t.Fatal("store validation differs from direct clone")
			}
		})
	}
}

func BenchmarkSingleRecordCloneBoundary(b *testing.B) {
	row := versionRecordFixture()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := CloneVersionRecord(row); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingleRecordStoreBoundary(b *testing.B) {
	row := versionRecordFixture()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store, _ := NewVersionStore(1)
		if err := store.Put(row); err != nil {
			b.Fatal(err)
		}
		if _, err := store.Records(); err != nil {
			b.Fatal(err)
		}
	}
}
