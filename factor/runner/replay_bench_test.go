package runner

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

// BenchmarkStorageReplay1m measures the per-record cost of the factor 1m price
// stream as Run drives it: page decode (synthetic rows shaped like the database
// K-line reader), fill/clone, batch assembly and the replay input digest.
// 20 streams x 3000 one-minute rows per op.
func BenchmarkStorageReplay1m(b *testing.B) {
	const tf, streamsN, rowsPer, from = "1m", 20, 3000, int64(60000)
	query := func(_ context.Context, sub orm.Subscription, start, end int64, limit int) ([]*orm.DataSeries, error) {
		rows := make([]*orm.DataSeries, 0, limit)
		for ts := (start + 59999) / 60000 * 60000; ts < end && len(rows) < limit && ts <= int64(rowsPer)*60000; ts += 60000 {
			rows = append(rows, &orm.DataSeries{Source: orm.SeriesSourceKline, Sid: sub.ExSymbol.ID, TimeMS: ts, EndMS: ts + 60000, TimeFrame: tf, Closed: true, ExSymbol: sub.ExSymbol,
				Values: map[string]any{"open": float64(ts) / 7, "high": float64(ts) / 6, "low": float64(ts) / 8, "close": float64(ts) / 7.5, "volume": float64(ts) / 3}})
		}
		return rows, nil
	}
	var streams []StorageStream
	for sid := int32(1); sid <= streamsN; sid++ {
		streams = append(streams, StorageStream{Subscription: orm.Subscription{Source: orm.SeriesSourceKline, TimeFrame: tf, ExSymbol: &orm.ExSymbol{ID: sid}}, SourceVersion: "v1", SchemaHash: "s1"})
	}
	options := StorageInputOptions{Namespace: "bench", PITPolicy: "static-approximation", FromMS: from, ToMS: from + int64(rowsPer)*60000, PageRows: 1000, PrefetchRows: 20000, Streams: streams, QueryPage: query}
	setOwnedPages(&options)
	factory, err := NewStorageInputFactory(options)
	if err != nil {
		b.Fatal(err)
	}
	c := Config{MaxRecords: 100000, Snapshot: factor.SnapshotSpec{VisibilityPolicy: "static-approximation", SourceVersions: map[string]string{orm.SeriesSourceKline: "v1"}, Schemas: map[string]string{orm.SeriesSourceKline: "s1"}}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		input, err := factory.Open(context.Background(), c, factory.Ranges()[0])
		if err != nil {
			b.Fatal(err)
		}
		digest := sha256.New()
		hasher := newBenchHasher()
		n := 0
		for {
			batch, err := input.Next(context.Background())
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			for j := range batch.Records {
				hasher.write(digest, &batch.Records[j])
			}
			n += len(batch.Records)
			if _, err := input.Visible(context.Background(), batch.AtMS, batch.AtMS, 0); err != nil {
				b.Fatal(err)
			}
		}
		if n != streamsN*(rowsPer+1) { // rows at 0 .. rowsPer x 1m
			b.Fatalf("replayed %d rows", n)
		}
		_ = input.Close()
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(streamsN*(rowsPer+1)), "ns/row")
}
