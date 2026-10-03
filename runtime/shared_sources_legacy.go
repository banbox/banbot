package runtime

import (
	"errors"
	"github.com/banbox/banbot/data"
	"maps"
	"reflect"
	"slices"
	"sync"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

// The fixed TS sessions share the canonical current source generation with
// factors. Only initial history warms them. A bounded typed revision ledger
// covers the short producer overlap without hashing/reflection of raw Values.
type legacyLiveIngress struct {
	runtime     *Runtime
	bindings    []*strat.DataSub
	streams     map[orm.StreamKey]bool
	mu          sync.Mutex
	seen        map[legacyLiveEvent]uint64
	pinned      map[legacyLiveEvent]uint64
	ring        []legacyLiveEvent
	next, limit int
	overflow    bool
}

type legacyLiveEvent struct {
	stream            orm.StreamKey
	event, start, end int64
	version           string
	closed            bool
}

func newLegacyLiveIngress(r *Runtime, prefetch, pageRows int) *legacyLiveIngress {
	bindings := cloneLegacyLiveBindings(r.FactorLegacySubscriptions())
	if len(bindings) == 0 {
		return nil
	}
	s := &legacyLiveIngress{runtime: r, bindings: bindings, streams: map[orm.StreamKey]bool{}, seen: map[legacyLiveEvent]uint64{}, limit: max(1, prefetch, pageRows*len(bindings))}
	for _, sub := range bindings {
		s.streams[sub.Key()] = true
	}
	return s
}

func cloneLegacyLiveBindings(bindings []*strat.DataSub) []*strat.DataSub {
	result := make([]*strat.DataSub, len(bindings))
	for i, sub := range bindings {
		if sub == nil {
			continue
		}
		copySub := *sub
		copySub.Fields, copySub.SeriesFields = slices.Clone(sub.Fields), slices.Clone(sub.SeriesFields)
		if sub.ExSymbol != nil {
			copySymbol := *sub.ExSymbol
			copySub.ExSymbol = &copySymbol
		}
		result[i] = &copySub
	}
	return result
}

func (s *FactorLiveSubscription) validateLegacy() error {
	current := s.runtime.FactorLegacySubscriptions()
	if s.legacy == nil {
		if len(current) != 0 {
			return errors.New("runtime: dynamic updates must retain fixed legacy session bindings")
		}
		return nil
	}
	if !reflect.DeepEqual(s.legacy.bindings, current) {
		return errors.New("runtime: dynamic updates must retain fixed legacy session bindings")
	}
	return nil
}

func (s *legacyLiveIngress) pin() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinned = maps.Clone(s.seen)
	s.overflow = false
}
func (s *legacyLiveIngress) unpin() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinned = nil
}

func (s *legacyLiveIngress) overlapReady() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.overflow {
		return errors.New("runtime: legacy producer overlap exceeded the bounded revision budget; increase data prefetch_rows or retry")
	}
	return nil
}

func (s *legacyLiveIngress) mapped(g *factorLiveGeneration, sub *orm.Subscription, raw *orm.DataSeries, received int64, warm bool) (factor.VersionRecord, legacyLiveEvent, error) {
	if raw == nil {
		return factor.VersionRecord{}, legacyLiveEvent{}, errors.New("runtime: legacy ingress nil series")
	}
	original := *raw
	original.IsWarmUp = warm
	series := original
	record, err := g.mappingConsumer(sub, &series).mapRecord(&series, received)
	if err == nil && (record.Revision == 0 || record.SourceVersion == "") {
		err = errors.New("runtime: legacy ingress requires explicit valid source revision metadata")
	}
	// Legacy observers retain the source's raw fields and timestamps even
	// when a factor mapper derives a separate factor-specific series view.
	record.Series = original
	return record, legacyLiveEvent{stream: sub.Key(), event: record.EventTime, start: raw.TimeMS, end: raw.EndMS, version: record.SourceVersion, closed: raw.Closed}, err
}

func (s *legacyLiveIngress) remember(event legacyLiveEvent, revision uint64) {
	if s.pinned != nil {
		if _, exists := s.pinned[event]; exists || len(s.pinned) < 2*s.limit {
			s.pinned[event] = max(s.pinned[event], revision)
		} else {
			s.overflow = true
		}
	}
	if _, ok := s.seen[event]; !ok {
		if len(s.ring) < s.limit {
			s.ring = append(s.ring, event)
		} else {
			delete(s.seen, s.ring[s.next])
			s.ring[s.next] = event
			s.next = (s.next + 1) % s.limit
		}
	}
	s.seen[event] = revision
}

func (s *legacyLiveIngress) warmup(g *factorLiveGeneration, sub *orm.Subscription, rows []*orm.DataSeries) error {
	if !s.streams[sub.Key()] {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, raw := range rows {
		record, _, err := s.mapped(g, sub, raw, s.runtime.Clock.TimeMS(), true)
		if err != nil {
			return err
		}
		if err := s.runtime.feedFactorLegacy(&record.Series); err != nil {
			return err
		}
		if g.provider != nil && raw.Source == orm.SeriesSourceKline {
			g.provider.RememberSeriesRevision(raw, data.LiveSeriesRevision{Revision: record.Revision, EventTime: record.EventTime, SourceVersion: record.SourceVersion, Warmup: true})
		}
	}
	return nil
}

func (s *legacyLiveIngress) emit(g *factorLiveGeneration, sub *orm.Subscription, rows []*orm.DataSeries, received int64) error {
	if !s.streams[sub.Key()] {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, raw := range rows {
		record, event, err := s.mapped(g, sub, raw, received, false)
		if err != nil {
			return err
		}
		if max(s.seen[event], s.pinned[event]) >= record.Revision {
			continue
		}
		if err := s.runtime.feedFactorLegacy(&record.Series); err != nil {
			return err
		}
		s.remember(event, record.Revision)
		if g.provider != nil && raw.Source == orm.SeriesSourceKline {
			g.provider.RememberSeriesRevision(raw, data.LiveSeriesRevision{Revision: record.Revision, EventTime: record.EventTime, SourceVersion: record.SourceVersion})
		}
	}
	return nil
}
