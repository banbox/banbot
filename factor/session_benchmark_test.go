package factor

import (
	"fmt"
	"math"
	"testing"
)

// Includes typed raw snapshot freeze, DAG evaluation and result-copy costs;
// excludes database I/O. Consumers reuse the identical snapshot within a round.
func BenchmarkMomentumVolatility500Assets(b *testing.B) {
	for _, consumers := range []int{1, 10} {
		b.Run(fmt.Sprintf("consumers-%d", consumers), func(b *testing.B) {
			plan, err := MomentumVolatility("prices", "close", "1h", 24)
			if err != nil {
				b.Fatal(err)
			}
			session, _ := NewSession(plan)
			snapshotAt := func(index int) *Snapshot {
				values := make(map[int32]map[string]any, 500)
				for sid := int32(1); sid <= 500; sid++ {
					values[sid] = map[string]any{"close": 100 + float64(index)*float64(sid)/1000 + math.Sin(float64(index)*float64(sid)/111)}
				}
				return testSnapshot(b, int64(index+1)*3600000, values)
			}
			for i := 0; i < 25; i++ {
				if _, err := session.Evaluate(snapshotAt(i)); err != nil {
					b.Fatal(err)
				}
			}
			initial := uint64(0)
			for _, count := range session.Updates() {
				initial += count
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snapshot := snapshotAt(i + 25)
				for j := 0; j < consumers; j++ {
					if _, err := session.Evaluate(snapshot); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			updated := uint64(0)
			for _, count := range session.Updates() {
				updated += count
			}
			b.ReportMetric(float64(updated-initial)/float64(b.N), "updates/round")
			b.ReportMetric(float64(session.RetainedValues()), "retained-values")
		})
	}
}
