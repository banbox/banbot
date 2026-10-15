package strat

import (
	"github.com/banbox/banbot/orm"
)

func lockInfoJobsWrite(state *State) {
	if state == nil || state == legacyState {
		lockInfoJobs.Lock()
		return
	}
	state.infoJobsMu.Lock()
	state.infoSnapshotDirty.Store(true)
}

func unlockInfoJobsWrite(state *State) {
	if state == nil || state == legacyState {
		lockInfoJobs.Unlock()
		return
	}
	state.infoJobsMu.Unlock()
}

func lockInfoJobsReadForState(state *State) {
	if state == nil || state == legacyState {
		lockInfoJobs.Lock()
		return
	}
	state.infoJobsMu.RLock()
}

func unlockInfoJobsReadForState(state *State) {
	if state == nil || state == legacyState {
		lockInfoJobs.Unlock()
		return
	}
	state.infoJobsMu.RUnlock()
}

func DataSubKey(source string, sid int32, tf string) string {
	return (orm.StreamKey{Source: source, SID: sid, TimeFrame: tf}).String()
}

func ParseDataSubKey(key string) (string, int32, string, bool) {
	parsed, ok := orm.ParseStreamKey(key)
	return parsed.Source, parsed.SID, parsed.TimeFrame, ok
}
func CollectDataSubs(job *StratJob) []*DataSub {
	if job != nil && job.symbols != nil {
		return CollectDataSubsWithSymbolState(job.symbols, job)
	}
	return CollectDataSubsWithSymbolState(nil, job)
}

func canonicalDataSubSymbol(symbols *orm.SymbolState, exs *orm.ExSymbol) *orm.ExSymbol {
	if symbols == nil || exs == nil {
		return exs
	}
	canonical := symbols.GetExSymbol2(exs.Exchange, exs.Market, exs.Symbol)
	if canonical == nil || canonical.ID != exs.ID {
		return nil
	}
	return canonical
}

// CollectDataSubsWithSymbolState resolves pair-info symbols from the supplied state.
func CollectDataSubsWithSymbolState(symbols *orm.SymbolState, job *StratJob) []*DataSub {
	if job == nil || job.Strat == nil {
		return nil
	}
	var out []*DataSub
	if job.Strat.OnPairInfos != nil {
		for _, sub := range job.Strat.OnPairInfos(job) {
			if sub == nil {
				continue
			}
			exs := canonicalDataSubSymbol(symbols, job.Symbol)
			if sub.Pair != "" && sub.Pair != "_cur_" {
				if job.Symbol != nil {
					if symbols == nil {
						exs = orm.GetExSymbol2(job.Symbol.Exchange, job.Symbol.Market, sub.Pair)
					} else {
						exs = symbols.GetExSymbol2(job.Symbol.Exchange, job.Symbol.Market, sub.Pair)
					}
				} else {
					if symbols == nil {
						exs, _ = orm.GetExSymbolCur(sub.Pair)
					} else {
						exs, _ = symbols.GetExSymbolCur(sub.Pair)
					}
				}
				if exs == nil {
					continue
				}
			}
			if symbols != nil && exs == nil {
				continue
			}
			out = append(out, &DataSub{
				Source:       orm.SeriesSourceKline,
				ExSymbol:     exs,
				TimeFrame:    sub.TimeFrame,
				WarmupNum:    sub.WarmupNum,
				Fields:       orm.NormalizeSeriesFields(orm.SeriesSourceKline, nil),
				SeriesFields: nil,
			})
		}
	}
	if job.Strat.OnDataSubs != nil {
		for _, sub := range job.Strat.OnDataSubs(job) {
			if sub == nil {
				continue
			}
			exs := sub.ExSymbol
			if exs == nil {
				exs = job.Symbol
			}
			exs = canonicalDataSubSymbol(symbols, exs)
			if exs == nil {
				continue
			}
			source := orm.NormalizeSeriesSource(sub.Source)
			seriesFields := orm.MergeSeriesFields(sub.SeriesFields)
			fields := orm.NormalizeSeriesFields(source, sub.Fields)
			// Explicit default/all projections must remain declarations until the
			// catalog expands the source schema before unioning subscriptions.
			if sub.Projection == orm.ProjectionDefault || sub.Projection == orm.ProjectionAll {
				fields = nil
			}
			if len(sub.SeriesFields) > 0 {
				if sub.Projection == "" || sub.Projection == orm.ProjectionSelected {
					fields = orm.MergeSeriesFields(fields, seriesFields)
				}
			}
			out = append(out, &DataSub{
				Source:       source,
				ExSymbol:     exs,
				TimeFrame:    sub.TimeFrame,
				WarmupNum:    sub.WarmupNum,
				Fields:       fields,
				SeriesFields: seriesFields,
				Frequency:    sub.Frequency,
				Projection:   sub.Projection,
			})
		}
	}
	return out
}

// CollectKlineSubFields returns the projection needed by every strategy that
// consumes the same K-line stream. The default fields keep the primary OnBar
// path valid, while side-input subscriptions may extend the projection.
func CollectKlineSubFields(sid int32, tf string) []string {
	return CollectKlineSubFieldsWithSymbolState(nil, sid, tf)
}

// CollectKlineSubFieldsWithSymbolState collects fields using the supplied symbol state.
func CollectKlineSubFieldsWithSymbolState(symbols *orm.SymbolState, sid int32, tf string) []string {
	return legacyStateView().CollectKlineSubFields(symbols, sid, tf)
}

// CollectKlineSubFields collects the projection required by jobs owned by
// this state. Explicit runtimes never inspect the package-level AccInfoJobs
// registry, so two runtimes can subscribe to different extension columns for
// the same symbol and timeframe.
func (s *State) CollectKlineSubFields(symbols *orm.SymbolState, sid int32, tf string) []string {
	fields := orm.NormalizeSeriesFields(orm.SeriesSourceKline, nil)
	if s == nil {
		return fields
	}
	if s != legacyState && symbols == nil {
		symbols = s.Symbols
	}
	var jobsByAccount map[string]map[string]map[string]*StratJob
	if s == legacyState {
		lockInfoJobs.Lock()
		jobsByAccount = s.accInfoJobs
	} else {
		s.infoJobsMu.RLock()
		jobsByAccount = s.accInfoJobs
	}
	hasInfoJobs := false
	for _, accJobs := range jobsByAccount {
		if len(accJobs) > 0 {
			hasInfoJobs = true
			break
		}
	}
	if !hasInfoJobs {
		if s == legacyState {
			lockInfoJobs.Unlock()
		} else {
			s.infoJobsMu.RUnlock()
		}
		return fields
	}
	key := DataSubKey(orm.SeriesSourceKline, sid, tf)
	seenJobs := make(map[*StratJob]bool)
	for _, accJobs := range jobsByAccount {
		for _, job := range accJobs[key] {
			seenJobs[job] = true
		}
	}
	if s == legacyState {
		lockInfoJobs.Unlock()
	} else {
		s.infoJobsMu.RUnlock()
	}
	for job := range seenJobs {
		for _, sub := range CollectDataSubsWithSymbolState(symbols, job) {
			if sub == nil || sub.ExSymbol == nil || sub.ExSymbol.ID != sid || sub.TimeFrame != tf ||
				orm.NormalizeSeriesSource(sub.Source) != orm.SeriesSourceKline {
				continue
			}
			fields = orm.MergeSeriesFields(fields, sub.Fields)
		}
	}
	return fields
}
