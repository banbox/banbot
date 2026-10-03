package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

func TestLiveSubscriptionPreparedMappingsKeepFIFOReceiptAndSealOnStop(t *testing.T) {
	mapperCalls := 0
	sink := &factorLiveSourceSink{mapper: func(*orm.DataSeries, int64) (factor.VersionRecord, error) {
		mapperCalls++
		return factor.VersionRecord{}, errors.New("queued callback must reuse its original mapping")
	}}
	sink.mappings.limit = 2
	row := &orm.DataSeries{Source: "kline", Sid: 1, TimeFrame: "1m", TimeMS: 0, EndMS: 60_000, Closed: true}
	for i, revision := range []uint64{2, 3} {
		received := int64(60_001 + i)
		mapped := *row
		mapped.Values = map[string]any{"integer": int64(revision), "nullable": nil}
		record := factor.VersionRecord{Series: mapped, EventTime: 60_000, AvailableAt: 60_000, IngestedAt: received, Revision: revision, SourceVersion: "v1"}
		reads := 1
		if revision == 2 {
			reads = 2 // Factor and fixed legacy consume the same accepted mapping.
		}
		if err := sink.mappings.stage(row, record, received, reads); err != nil {
			t.Fatal(err)
		}
	}
	for _, revision := range []uint64{2, 2, 3} {
		record, err := sink.mapRecord(row, 90_000)
		if err != nil {
			t.Fatal(err)
		}
		if record.Revision != revision || record.IngestedAt != 59_999+int64(revision) || record.Series.Values["integer"] != int64(revision) {
			t.Fatalf("queued revision or original receipt changed: %#v", record)
		}
		if value, exists := record.Series.Values["nullable"]; !exists || value != nil {
			t.Fatal("queued explicit NULL lost")
		}
	}
	if mapperCalls != 0 {
		t.Fatal("queued callback invoked mapper again")
	}

	// A callback received before shutdown must never fall back to a mapper after
	// its generation has stopped; neither may late ingress stage another record.
	record := factor.VersionRecord{Series: *row, IngestedAt: 90_000}
	if err := sink.mappings.stage(row, record, 90_000, 1); err != nil {
		t.Fatal(err)
	}
	sink.mappings.close()
	if _, err := sink.mapRecord(row, 90_001); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped generation admitted prepared delivery: %v", err)
	}
	if err := sink.mappings.stage(row, record, 90_000, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped generation admitted late mapping: %v", err)
	}
	if mapperCalls != 0 {
		t.Fatal("stopped generation invoked mapper")
	}
}
