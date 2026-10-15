package data

import (
	"fmt"
	"sync"

	"github.com/banbox/banbot/orm"
	utils2 "github.com/banbox/banexg/utils"
)

// LiveSeriesRevision comes from the source mapper's typed contract. Values
// remain arbitrary user data, including fields whose names resemble metadata.
type LiveSeriesRevision struct {
	EventTime     int64
	Revision      uint64
	SourceVersion string
	Warmup        bool
}

type LiveSeriesRevisionSink interface {
	SeriesRevision(*orm.Subscription, *orm.DataSeries) (LiveSeriesRevision, error)
}

type liveSeriesRevisionDiscarder interface {
	DiscardSeriesRevision(*orm.Subscription, *orm.DataSeries)
}

type liveSeriesRevisionKey struct {
	stream     orm.StreamKey
	start, end int64
	closed     bool
}

type liveSeriesRevisionLedger struct {
	mu   sync.Mutex
	seen map[liveSeriesRevisionKey]LiveSeriesRevision
	ring []liveSeriesRevisionKey
	next int
}

func seriesRevisionKey(row *orm.DataSeries) liveSeriesRevisionKey {
	return liveSeriesRevisionKey{stream: orm.StreamKey{Source: orm.NormalizeSeriesSource(row.Source), SID: row.Sid, TimeFrame: row.TimeFrame}, start: row.TimeMS, end: row.EndMS, closed: row.Closed}
}

// RememberSeriesRevision records metadata already mapped by a consumer, so
// normal ingress calls the mapper once and does no Values hashing/reflection.
func (p *LiveProvider) RememberSeriesRevision(row *orm.DataSeries, revision LiveSeriesRevision) {
	if row == nil || revision.Revision == 0 || revision.SourceVersion == "" {
		return
	}
	p.revisions.mu.Lock()
	defer p.revisions.mu.Unlock()
	if p.revisions.seen == nil {
		p.revisions.seen = make(map[liveSeriesRevisionKey]LiveSeriesRevision)
	}
	key := seriesRevisionKey(row)
	previous, exists := p.revisions.seen[key]
	if exists && previous.SourceVersion == revision.SourceVersion && previous.EventTime == revision.EventTime && (previous.Revision > revision.Revision || previous.Revision == revision.Revision && (!previous.Warmup || revision.Warmup)) {
		return
	}
	limit := 2 * p.deps.numTACache() * max(1, p.revisionStreams)
	if limit < 1 {
		limit = 1
	}
	if !exists {
		if len(p.revisions.ring) < limit {
			p.revisions.ring = append(p.revisions.ring, key)
		} else {
			delete(p.revisions.seen, p.revisions.ring[p.revisions.next])
			p.revisions.ring[p.revisions.next] = key
			p.revisions.next = (p.revisions.next + 1) % limit
		}
	}
	p.revisions.seen[key] = revision
}

func (p *LiveProvider) acceptRevisionRows(hold IDataFeeder, step int64, msg *SeriesMsg, rows []*orm.DataSeries) (accepted []*orm.DataSeries, gateErr error) {
	var staged []*orm.DataSeries
	discard := func(row *orm.DataSeries) {
		if sink, ok := p.subscriptionSink.sink.(liveSeriesRevisionDiscarder); ok {
			sink.DiscardSeriesRevision(&orm.Subscription{Source: row.Source, ExSymbol: row.ExSymbol, TimeFrame: row.TimeFrame}, row)
		}
	}
	defer func() {
		if gateErr != nil {
			for _, row := range staged {
				discard(row)
			}
		}
	}()
	states := hold.getStates()
	if len(states) == 0 {
		return rows, nil
	}
	accepted = make([]*orm.DataSeries, 0, len(rows))
	batch := make(map[liveSeriesRevisionKey]bool, len(rows))
	for _, raw := range rows {
		if raw == nil {
			accepted = append(accepted, raw)
			continue
		}
		row := *raw
		row.Source = orm.NormalizeSeriesSource(row.Source)
		row.TimeFrame = utils2.SecsToTF(msg.TFSecs)
		row.ExSymbol = p.symbols.GetExSymbol2(msg.ExgName, msg.Market, msg.Pair)
		if row.ExSymbol == nil || row.Sid != 0 && row.Sid != row.ExSymbol.ID || row.Source != orm.SeriesSourceKline {
			return nil, fmt.Errorf("kline ingress emitted foreign source/SID")
		}
		row.Sid = row.ExSymbol.ID
		if row.EndMS == 0 {
			row.EndMS = row.TimeMS + step
		}
		row.Closed = true
		key := seriesRevisionKey(&row)
		if batch[key] {
			return nil, fmt.Errorf("kline batch contains duplicate logical observations; revisions require ordered separate messages")
		}
		batch[key] = true
		p.revisions.mu.Lock()
		previous, known := p.revisions.seen[key]
		p.revisions.mu.Unlock()
		late := row.TimeMS < states[0].NextMS || known
		if late {
			if !known {
				return nil, fmt.Errorf("kline correction is outside retained typed revision evidence")
			}
			resolver, ok := p.subscriptionSink.sink.(LiveSeriesRevisionSink)
			if !ok {
				return nil, fmt.Errorf("kline correction requires typed source revision metadata")
			}
			sub := orm.Subscription{Source: row.Source, ExSymbol: row.ExSymbol, TimeFrame: row.TimeFrame}
			view := &row
			if projected, ok := hold.(interface {
				subscriptionRevisionView(*orm.DataSeries) *orm.DataSeries
			}); ok {
				view = projected.subscriptionRevisionView(&row)
			}
			revision, err := resolver.SeriesRevision(&sub, view)
			if err != nil {
				return nil, err
			}
			staged = append(staged, view)
			if revision.Revision == 0 || revision.SourceVersion == "" {
				return nil, fmt.Errorf("kline correction requires explicit source revision metadata")
			}
			if known && (revision.SourceVersion != previous.SourceVersion || revision.EventTime != previous.EventTime) {
				return nil, fmt.Errorf("kline correction changed its typed source/event identity")
			}
			if known && revision.SourceVersion == previous.SourceVersion && revision.EventTime == previous.EventTime && (revision.Revision < previous.Revision || revision.Revision == previous.Revision && (!previous.Warmup || len(states) > 1 || int64(states[0].TFSecs)*1000 != step)) {
				discard(view)
				staged = staged[:len(staged)-1]
				continue
			}
			if len(states) > 1 || int64(states[0].TFSecs)*1000 != step {
				return nil, fmt.Errorf("coarse kline correction requires a reliable typed derived revision identity")
			}
		}
		accepted = append(accepted, &row)
	}
	return accepted, nil
}
