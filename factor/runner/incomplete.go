package runner

import "github.com/banbox/banbot/factor"

// IncompleteStream summarizes rejected decision grids, excluding warmup.
// Storage is bounded by the number of required streams, not the run length.
type IncompleteStream struct {
	factor.StreamKey
	Missing, Invalid            int
	FirstGridTime, LastGridTime int64
}

func recordIncompleteStreams(result *Result, indexes map[factor.StreamKey]int, status factor.SnapshotStatus, grid int64) {
	arrived := make(map[factor.StreamKey]bool, len(status.Arrived))
	for _, key := range status.Arrived {
		arrived[key] = true
	}
	invalid := make(map[factor.StreamKey]bool, len(status.Invalid))
	for _, key := range status.Invalid {
		invalid[key] = true
	}
	for _, key := range status.Expected {
		if arrived[key] && !invalid[key] {
			continue
		}
		index, exists := indexes[key]
		if !exists {
			index = len(result.IncompleteStreams)
			indexes[key] = index
			result.IncompleteStreams = append(result.IncompleteStreams, IncompleteStream{StreamKey: key, FirstGridTime: grid, LastGridTime: grid})
		}
		issue := &result.IncompleteStreams[index]
		issue.FirstGridTime = min(issue.FirstGridTime, grid)
		issue.LastGridTime = max(issue.LastGridTime, grid)
		if !arrived[key] {
			issue.Missing++
		} else {
			issue.Invalid++
		}
	}
}
