package factor

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/banbox/banbot/orm"
)

func testRecord(sid int32, event int64, values map[string]any) VersionRecord {
	return Record(orm.DataSeries{Source: "prices", Sid: sid, TimeMS: event - 1, EndMS: event, Closed: true, TimeFrame: "1h", Values: values}, 1, event, event, "prices-v1")
}

func testSnapshot(t testing.TB, event int64, values map[int32]map[string]any) *Snapshot {
	t.Helper()
	sids := make([]int32, 0, len(values))
	sidMap := make(map[int32]string)
	rows := make([]VersionRecord, 0, len(values))
	requirements := make([]Requirement, 0, len(values))
	for sid, fields := range values {
		sids = append(sids, sid)
		sidMap[sid] = string(rune('A' + sid))
		rows = append(rows, testRecord(sid, event, fields))
		requirements = append(requirements, Requirement{SID: sid, Source: "prices", Frequency: "1h", EventTime: event})
	}
	snapshot, err := Freeze(SnapshotSpec{DecisionTime: event, ReplayTime: event, Universe: Universe{Version: "universe-v1", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Static: true}, SIDMap: sidMap, Schemas: map[string]string{"prices": "schema-v1"}, SourceVersions: map[string]string{"prices": "prices-v1"}, VisibilityPolicy: "published-and-received"}, rows, requirements)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestSnapshotPreservesTypesValidityAndImmutableAccess(t *testing.T) {
	fields := map[string]any{"close": float64(12), "integer": int16(7), "single": float32(2), "name": "sector", "tradable": true, "null": nil, "nan": math.NaN(), "nested": map[string]any{"items": []any{int32(9), "x"}}}
	snapshot := testSnapshot(t, 10, map[int32]map[string]any{1: fields})
	for field, want := range map[string]Validity{"close": Valid, "integer": Valid, "single": Valid, "absent": Missing, "name": NotNumeric, "tradable": NotNumeric, "null": Null, "nan": NonFinite} {
		if got := snapshot.Numeric(1, "prices", "1h", field); got.Validity != want {
			t.Fatalf("%s validity=%s want %s", field, got.Validity, want)
		}
	}
	fields["close"] = 999.0
	fields["nested"].(map[string]any)["items"].([]any)[0] = int32(100)
	copy, _ := snapshot.Row(1, "prices", "1h")
	if _, ok := copy.Series.Values["integer"].(int16); !ok {
		t.Fatal("integer type lost")
	}
	if _, exists := copy.Series.Values["absent"]; exists {
		t.Fatal("missing filled in")
	}
	if got := copy.Series.Values["nested"].(map[string]any)["items"].([]any)[0]; got != int32(9) {
		t.Fatalf("nested input mutated snapshot: %v", got)
	}
	copy.Series.Values["close"] = 200.0
	spec := snapshot.Spec()
	spec.Universe.Reference[0] = 99
	spec.SIDMap[1] = "changed"
	if snapshot.Numeric(1, "prices", "1h", "close").Value != 12 || snapshot.Spec().Universe.Reference[0] != 1 {
		t.Fatal("accessor mutated frozen snapshot")
	}
}

func TestVersionChunkReopenVisibilityConflictAndFrequency(t *testing.T) {
	store, err := NewVersionStore(4)
	if err != nil {
		t.Fatal(err)
	}
	first := testRecord(1, 10, map[string]any{"close": int32(100), "flag": true, "name": "A", "null": nil})
	second := first
	second.Revision = 2
	second.AvailableAt = 20
	second.IngestedAt = 30
	second.Series.Values = map[string]any{"close": int32(101), "flag": true, "name": "A", "null": nil}
	minute := first
	minute.Series.TimeFrame = "1m"
	minute.Series.Values = map[string]any{"close": float32(5)}
	for _, record := range []VersionRecord{second, minute, first, first} {
		if err := store.Put(record); err != nil {
			t.Fatal(err)
		}
	}
	conflict := first
	conflict.Series.Values = map[string]any{"close": int32(9)}
	if err := store.Put(conflict); err == nil {
		t.Fatal("immutable revision overwritten")
	}
	for _, filter := range []struct {
		asof, replay int64
		want         int32
	}{{15, 15, 100}, {25, 25, 100}, {25, 35, 101}} {
		visible, err := store.Visible(10, 10, filter.asof, filter.replay)
		if err != nil {
			t.Fatal(err)
		}
		if len(visible) != 2 {
			t.Fatalf("frequencies collapsed: %#v", visible)
		}
		for _, row := range visible {
			if row.Series.TimeFrame == "1h" && row.Series.Values["close"] != filter.want {
				t.Fatalf("wrong visible revision: %#v", row)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "versions.gob")
	hash, err := store.Export(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenVersionStore(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	otherPath := filepath.Join(t.TempDir(), "same.gob")
	secondHash, err := reopened.Export(otherPath)
	if err != nil {
		t.Fatal(err)
	}
	if hash != secondHash {
		t.Fatal("typed logical hash changed after reopen")
	}
	if reused, err := store.Export(path); err != nil || reused != hash {
		t.Fatalf("same immutable archive cannot be reused: %v", err)
	}
	different, _ := NewVersionStore(4)
	if err := different.Put(conflict); err != nil {
		t.Fatal(err)
	}
	if _, err := different.Export(path); err == nil {
		t.Fatal("different contents replaced immutable archive")
	}
	if _, err := OpenVersionStore(path, 4); err != nil {
		t.Fatalf("conflict damaged prior archive: %v", err)
	}
	if _, err := OpenVersionStore(path, 2); err == nil {
		t.Fatal("oversized chunk admitted")
	}
	visible, err := reopened.Visible(10, 10, 15, 15)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range visible {
		if row.Series.TimeFrame == "1h" && reflect.TypeOf(row.Series.Values["close"]) != reflect.TypeOf(int32(0)) {
			t.Fatal("gob lost integer type")
		}
	}
}

func TestSnapshotBarrierLateFutureAndSparseSources(t *testing.T) {
	base := testSnapshot(t, 10, map[int32]map[string]any{1: {"close": 1.0}, 2: {"close": nil}})
	spec := base.Spec()
	status := base.Status()
	if !status.Ready {
		t.Fatal("NULL row incorrectly counted missing")
	}
	rows := []VersionRecord{testRecord(1, 10, map[string]any{"close": 1.0})}
	requirements := []Requirement{{SID: 1, Source: "prices", Frequency: "1h", EventTime: 10}, {SID: 2, Source: "prices", Frequency: "1h", EventTime: 10}}
	missing, err := Freeze(spec, rows, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if missing.Status().Ready || len(missing.Status().Arrived) != 1 {
		t.Fatal("missing barrier released")
	}
	late := testRecord(2, 10, map[string]any{"close": nil})
	late.IngestedAt = 11
	rows = append(rows, late)
	stillMissing, err := Freeze(spec, rows, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if stillMissing.Status().Ready {
		t.Fatal("late received data entered replay snapshot")
	}
	late.IngestedAt = 10
	rows[1] = late
	future := testRecord(1, 10, map[string]any{"close": 999.0})
	future.Revision = 2
	future.AvailableAt = 20
	future.IngestedAt = 20
	rows = append(rows, future)
	same, err := Freeze(spec, rows, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if same.ID() != base.ID() {
		t.Fatal("future revision changed past snapshot hash")
	}
	rows[0].Series.Closed = false
	nonclosed, err := Freeze(spec, rows, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if nonclosed.Status().Ready || len(nonclosed.Status().Invalid) != 1 {
		t.Fatal("nonclosed stream accepted")
	}
	rows[0].Series.Closed = true
	rows[0].EventTime = 8
	requirements[0].AsOfLatest = true
	requirements[0].MaxAge = 2
	sparse, err := Freeze(spec, rows, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if !sparse.Status().Ready {
		t.Fatal("visible sparse source rejected")
	}
	requirements[0].MaxAge = 1
	stale, err := Freeze(spec, rows, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Status().Ready {
		t.Fatal("stale sparse source accepted")
	}
}

func TestSnapshotFrequencyIdentityAndCanonicalHash(t *testing.T) {
	base := testSnapshot(t, 10, map[int32]map[string]any{1: {"close": 100.0}})
	hour, _ := base.Row(1, "prices", "1h")
	minute := hour
	minute.Series.TimeFrame = "1m"
	minute.Series.Values = map[string]any{"close": 7.0}
	need := []Requirement{{SID: 1, Source: "prices", Frequency: "1h", EventTime: 10}, {SID: 1, Source: "prices", Frequency: "1m", EventTime: 10}}
	snapshot, err := Freeze(base.Spec(), []VersionRecord{minute, hour}, need)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Numeric(1, "prices", "1h", "close").Value != 100 || snapshot.Numeric(1, "prices", "1m", "close").Value != 7 {
		t.Fatal("frequency fields crossed")
	}
	need[0], need[1] = need[1], need[0]
	reordered, err := Freeze(base.Spec(), []VersionRecord{hour, minute}, need)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ID() != reordered.ID() {
		t.Fatal("input order changed snapshot identity")
	}
	noBarrier, err := Freeze(base.Spec(), []VersionRecord{hour}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if noBarrier.Status().Ready {
		t.Fatal("empty barrier accepted")
	}
}

func TestVersionChunkExplicitUnsupportedExport(t *testing.T) {
	type unregistered struct{ Value int }
	store, _ := NewVersionStore(1)
	if err := store.Put(testRecord(1, 10, map[string]any{"custom": unregistered{7}})); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "unsupported.gob")
	if _, err := store.Export(path); err == nil {
		t.Fatal("unsupported gob type silently discarded")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("failed export published partial archive: %v", err)
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	if _, err := cloneValues(cycle); err == nil {
		t.Fatal("cyclic raw value accepted")
	}
	type privateMutable struct{ items []int }
	if _, err := cloneValues(map[string]any{"opaque": privateMutable{[]int{1}}}); err == nil {
		t.Fatal("private mutable raw alias accepted")
	}
}
