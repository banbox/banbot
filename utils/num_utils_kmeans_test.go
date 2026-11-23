package utils

import "testing"

func TestKMeansValsNoEmptyClusters(t *testing.T) {
	inputs := [][]float64{
		{1, 1, 1, 1},
		{0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.05, 0.05},
		{1, 1, 1, 2, 2, 100},
		{3, 3},
	}
	for _, vals := range inputs {
		for num := 1; num <= 8; num++ {
			res := KMeansVals(vals, num)
			total := 0
			for _, c := range res.Clusters {
				if len(c.Items) == 0 {
					t.Fatalf("vals=%v num=%d: empty cluster", vals, num)
				}
				total += len(c.Items)
			}
			if total != len(vals) || len(res.RowGIds) != len(vals) {
				t.Fatalf("vals=%v num=%d: item count mismatch", vals, num)
			}
			for i, gid := range res.RowGIds {
				if gid < 0 || gid >= len(res.Clusters) {
					t.Fatalf("vals=%v num=%d: RowGIds[%d]=%d out of range", vals, num, i, gid)
				}
				found := false
				for _, it := range res.Clusters[gid].Items {
					if it == vals[i] {
						found = true
					}
				}
				if !found {
					t.Fatalf("vals=%v num=%d: row %d not in its cluster", vals, num, i)
				}
			}
		}
	}
}
