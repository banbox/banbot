package runner

import (
	"fmt"
)

// sourceDecisionGrid selects a real closed observation, never a synthetic
// periodic tick. A single publication batch containing several source grids
// cannot provide independent PIT snapshots for them, so fails explicitly.
func sourceDecisionGrid(c Config, batch HistoricalBatch) (int64, error) {
	var grid int64
	for _, r := range batch.Records {
		if r.Series.Source != c.Factor.Source || r.Series.TimeFrame != c.Factor.TimeFrame || !r.Series.Closed {
			continue
		}
		if r.EventTime <= 0 || r.EventTime > batch.AtMS || r.AvailableAt > batch.AtMS || r.IngestedAt > batch.AtMS {
			return 0, fmt.Errorf("runner: source decision record %s/%s is not observable at batch time", c.Factor.Source, c.Factor.TimeFrame)
		}
		if grid != 0 && grid != r.EventTime {
			return 0, fmt.Errorf("runner: ambiguous source decision batch for %s/%s: grids %d and %d published together", c.Factor.Source, c.Factor.TimeFrame, grid, r.EventTime)
		}
		grid = r.EventTime
	}
	return grid, nil
}
