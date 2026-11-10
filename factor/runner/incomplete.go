package runner

import (
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/banbox/banbot/factor"
)

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

// pitReduceUniverse (067) derives the point-in-time universe of one decision grid: Investable instruments with a
// missing or invalid required stream are dropped (data ended / not yet listed / hole). Reference instruments are
// never dropped (the plan may depend on them), and the reduction is refused (ok=false -> the old "skip the whole
// decision" behaviour) when anything else is missing or fewer than max(2, minRatio x Investable) instruments
// remain. The reduced universe carries a derived Version so sessions never see one version with two contents.
func pitReduceUniverse(u factor.Universe, status factor.SnapshotStatus, minRatio float64) (factor.Universe, []int32, bool) {
	if minRatio == 0 {
		minRatio = 0.5
	}
	ok := make(map[factor.StreamKey]bool, len(status.Arrived))
	for _, key := range status.Arrived {
		ok[key] = true
	}
	for _, key := range status.Invalid {
		delete(ok, key)
	}
	invest := make(map[int32]bool, len(u.Investable))
	for _, sid := range u.Investable {
		invest[sid] = true
	}
	for _, sid := range u.Reference {
		delete(invest, sid)
	}
	bad := map[int32]bool{}
	for _, key := range status.Expected {
		if ok[key] {
			continue
		}
		if !invest[key.SID] {
			return u, nil, false
		}
		bad[key.SID] = true
	}
	if len(bad) == 0 {
		return u, nil, false
	}
	dropped := make([]int32, 0, len(bad))
	for sid := range bad {
		dropped = append(dropped, sid)
	}
	sort.Slice(dropped, func(i, j int) bool { return dropped[i] < dropped[j] })
	keep := func(ids []int32) []int32 {
		out := make([]int32, 0, len(ids))
		for _, sid := range ids {
			if !bad[sid] {
				out = append(out, sid)
			}
		}
		return out
	}
	reduced := u
	reduced.Investable, reduced.Reference = keep(u.Investable), keep(u.Reference)
	reduced.Tradable, reduced.Evaluation = keep(u.Tradable), keep(u.Evaluation)
	if len(reduced.Investable) < 2 || float64(len(reduced.Investable)) < minRatio*float64(len(u.Investable)) {
		return u, nil, false
	}
	sum := sha256.Sum256([]byte(fmt.Sprint(dropped)))
	reduced.Version = fmt.Sprintf("%s#pit-%x", u.Version, sum[:4])
	return reduced, dropped, true
}

const defaultPITDelistPenaltyBps = 100

// pitReducePairs (070) is the pairs-mode PIT reduction. The unit is the pair, not the universe: Investable
// instruments without input at the grid are dropped whatever remains (no "universe too small" fallback) and the
// builder closes and pauses every pair that lost a leg. allPaused reports that no pair has both legs left. The
// reduced Investable may be empty (both legs of every pair gone): the caller then closes the book and skips the
// period as paused. Reference instruments missing -> refuse (ok=false), as in the weights mode.
func pitReducePairs(u factor.Universe, status factor.SnapshotStatus, pairs [][2]int32) (factor.Universe, []int32, bool, bool) {
	ok := make(map[factor.StreamKey]bool, len(status.Arrived))
	for _, key := range status.Arrived {
		ok[key] = true
	}
	for _, key := range status.Invalid {
		delete(ok, key)
	}
	invest := make(map[int32]bool, len(u.Investable))
	for _, sid := range u.Investable {
		invest[sid] = true
	}
	for _, sid := range u.Reference {
		delete(invest, sid)
	}
	bad := map[int32]bool{}
	for _, key := range status.Expected {
		if ok[key] {
			continue
		}
		if !invest[key.SID] {
			return u, nil, false, false
		}
		bad[key.SID] = true
	}
	if len(bad) == 0 {
		return u, nil, false, false
	}
	dropped := make([]int32, 0, len(bad))
	for sid := range bad {
		dropped = append(dropped, sid)
	}
	sort.Slice(dropped, func(i, j int) bool { return dropped[i] < dropped[j] })
	keep := func(ids []int32) []int32 {
		out := make([]int32, 0, len(ids))
		for _, sid := range ids {
			if !bad[sid] {
				out = append(out, sid)
			}
		}
		return out
	}
	reduced := u
	reduced.Investable, reduced.Reference = keep(u.Investable), keep(u.Reference)
	reduced.Tradable, reduced.Evaluation = keep(u.Tradable), keep(u.Evaluation)
	allPaused := true
	for _, p := range pairs {
		if !bad[p[0]] && !bad[p[1]] {
			allPaused = false
			break
		}
	}
	sum := sha256.Sum256([]byte(fmt.Sprint(dropped)))
	reduced.Version = fmt.Sprintf("%s#pit-%x", u.Version, sum[:4])
	return reduced, dropped, allPaused, true
}
