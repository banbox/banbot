package factor

import "testing"

func TestVersionStoreRecordsRetainsTimelineAndDefensiveTypes(t *testing.T) {
	store, _ := NewVersionStore(3)
	first := testRecord(1, 10, map[string]any{"nested": []any{int16(7), nil}})
	second := first
	second.Revision, second.AvailableAt, second.IngestedAt = 2, 20, 30
	later := testRecord(2, 11, map[string]any{"close": float32(4)})
	for _, row := range []VersionRecord{later, second, first} {
		if err := store.Put(row); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].Revision != 1 || rows[1].Revision != 2 || rows[2].EventTime != 11 || rows[1].AvailableAt != 20 || rows[1].IngestedAt != 30 {
		t.Fatalf("lost raw revision timeline: %#v", rows)
	}
	rows[0].Series.Values["nested"].([]any)[0] = int16(99)
	rows[0].Series.Values["extra"] = true
	again, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	if again[0].Series.Values["nested"].([]any)[0] != int16(7) || again[0].Series.Values["extra"] != nil {
		t.Fatal("records accessor exposed mutable stored values")
	}
	if _, ok := again[2].Series.Values["close"].(float32); !ok {
		t.Fatal("raw type lost")
	}
	visible, err := store.Visible(0, 100, 100, 100)
	if err != nil || len(visible) != 2 {
		t.Fatalf("visible selection changed: %v %v", visible, err)
	}
}
