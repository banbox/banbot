package orm

import (
	"reflect"
	"testing"
)

func TestPhysicalKlineStorageUsesActualTable(t *testing.T) {
	for _, test := range []struct {
		requested string
		storage   string
		table     string
	}{
		{requested: "1m", storage: "1m", table: "kline_1m"},
		{requested: "5m", storage: "5m", table: "kline_5m"},
		{requested: "15m", storage: "15m", table: "kline_15m"},
		{requested: "4h", storage: "1h", table: "kline_1h"},
		{requested: "3d", storage: "1d", table: "kline_1d"},
	} {
		storage, table, err := physicalKlineStorage(test.requested)
		if err != nil {
			t.Fatalf("%s: %v", test.requested, err)
		}
		if storage != test.storage || table != test.table {
			t.Fatalf("%s resolved to %s/%s, want %s/%s", test.requested, storage, table, test.storage, test.table)
		}
	}
}

func TestPhysicalKlineCollectorFindsGapsAndHashesTimestamps(t *testing.T) {
	collector := newPhysicalKlineCollector(100, 600, 100)
	for _, timestamp := range []int64{100, 200, 400, 500} {
		if err := collector.add(timestamp); err != nil {
			t.Fatal(err)
		}
	}
	first, last, count, missing, digest, _ := collector.finish()
	if first != 100 || last != 500 || count != 4 {
		t.Fatalf("summary=%d/%d/%d", first, last, count)
	}
	wantMissing := []PhysicalKlineGap{{StartMS: 300, StopMS: 400}}
	if !reflect.DeepEqual(missing, wantMissing) {
		t.Fatalf("missing=%#v, want %#v", missing, wantMissing)
	}
	if digest != "5ad09d6d4a37dc3548ccb451ce3fdd5c23c343202881649dadb7be4ea1d25be5" {
		t.Fatalf("timestamp digest=%s", digest)
	}
}

func TestPhysicalKlineCollectorRejectsMisalignedAndDuplicateRows(t *testing.T) {
	collector := newPhysicalKlineCollector(100, 600, 100)
	if err := collector.add(150); err == nil {
		t.Fatal("misaligned timestamp accepted")
	}
	collector = newPhysicalKlineCollector(100, 600, 100)
	if err := collector.add(100); err != nil {
		t.Fatal(err)
	}
	if err := collector.add(100); err == nil {
		t.Fatal("duplicate timestamp accepted")
	}
}

func TestPhysicalKlineCollectorReportsCompleteRange(t *testing.T) {
	collector := newPhysicalKlineCollector(100, 400, 100)
	for _, timestamp := range []int64{100, 200, 300} {
		if err := collector.add(timestamp); err != nil {
			t.Fatal(err)
		}
	}
	_, _, count, missing, _, _ := collector.finish()
	if count != 3 || len(missing) != 0 {
		t.Fatalf("count=%d missing=%#v", count, missing)
	}
}

func TestPhysicalKlineCollectorDataHashDetectsOHLCVChanges(t *testing.T) {
	first := newPhysicalKlineCollector(100, 200, 100)
	second := newPhysicalKlineCollector(100, 200, 100)
	if err := first.addRow(100, 1, 2, 0.5, 1.5, 10, 15, 4, 3); err != nil {
		t.Fatal(err)
	}
	if err := second.addRow(100, 1, 2, 0.5, 1.6, 10, 15, 4, 3); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, firstTimestamp, firstData := first.finish()
	_, _, _, _, secondTimestamp, secondData := second.finish()
	if firstTimestamp != secondTimestamp || firstData == secondData {
		t.Fatalf("timestamps=%s/%s data=%s/%s", firstTimestamp, secondTimestamp, firstData, secondData)
	}
}

func TestPhysicalKlineAlignmentIncludesConsumerSourcePrefix(t *testing.T) {
	const hour = int64(3_600_000)
	bounds := physicalKlineCoverageBounds(hour, 12*hour, 0, 0, 4*hour, 0, hour, 0)
	if bounds.consumerStart != 0 || bounds.storageStart != 0 {
		t.Fatalf("4h bounds=%#v, want consumer/storage start 0", bounds)
	}
	if got := alignPhysicalKlineCeil(10*hour+1, 4*hour, 0); got != 12*hour {
		t.Fatalf("4h ceil=%d, want %d", got, 12*hour)
	}
}

func TestPhysicalKlineBoundsIncludePartialDelistedBucket(t *testing.T) {
	const minute = int64(60_000)
	bounds := physicalKlineCoverageBounds(0, 20*minute, 0, 12*minute,
		5*minute, 0, 5*minute, 0)
	if bounds.consumerStop != 15*minute || bounds.storageStop != 15*minute || bounds.reason != "delisted_market" {
		t.Fatalf("delisted 5m bounds=%#v", bounds)
	}
	bounds = physicalKlineCoverageBounds(0, 20*minute, 0, 12*minute,
		4*60*minute, 0, 60*minute, 0)
	if bounds.consumerStop != 4*60*minute || bounds.storageStop != 60*minute || bounds.reason != "delisted_market" {
		t.Fatalf("delisted derived bounds=%#v", bounds)
	}
}

func TestPhysicalKlineBoundsSkipPartialListingBucket(t *testing.T) {
	const minute = int64(60_000)
	bounds := physicalKlineCoverageBounds(0, 20*minute, 2*minute, 0,
		5*minute, 0, 5*minute, 0)
	if bounds.consumerStart != 5*minute || bounds.storageStart != 5*minute {
		t.Fatalf("listed 5m bounds=%#v", bounds)
	}
}
