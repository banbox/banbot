package runtime

import (
	"context"
	"errors"
	"sync"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

type seriesMappingKey struct {
	stream     orm.StreamKey
	start, end int64
	closed     bool
}
type preparedSeriesMapping struct {
	record   factor.VersionRecord
	received int64
	reads    int
}

// Only accepted revision-gate records await delivery. Active callbacks consume
// them in the same handler; prepared generations share the startup queue budget.
type preparedSeriesMappings struct {
	mu           sync.Mutex
	rows         map[seriesMappingKey][]preparedSeriesMapping
	count, limit int
	closed       bool
}

func mappingKey(row *orm.DataSeries) seriesMappingKey {
	return seriesMappingKey{stream: orm.StreamKey{Source: orm.NormalizeSeriesSource(row.Source), SID: row.Sid, TimeFrame: row.TimeFrame}, start: row.TimeMS, end: row.EndMS, closed: row.Closed}
}
func (m *preparedSeriesMappings) stage(row *orm.DataSeries, record factor.VersionRecord, received int64, reads int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return context.Canceled
	}
	if m.count >= m.limit {
		return errors.New("runtime: prepared revision mapping budget exceeded")
	}
	if m.rows == nil {
		m.rows = map[seriesMappingKey][]preparedSeriesMapping{}
	}
	key := mappingKey(row)
	m.rows[key] = append(m.rows[key], preparedSeriesMapping{record: record, received: received, reads: reads})
	m.count++
	return nil
}
func (m *preparedSeriesMappings) discard(row *orm.DataSeries) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := mappingKey(row)
	items := m.rows[key]
	if len(items) == 0 {
		return
	}
	items[len(items)-1] = preparedSeriesMapping{}
	items = items[:len(items)-1]
	m.count--
	if len(items) == 0 {
		delete(m.rows, key)
	} else {
		m.rows[key] = items
	}
}
func (m *preparedSeriesMappings) take(row *orm.DataSeries) (preparedSeriesMapping, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return preparedSeriesMapping{}, false, context.Canceled
	}
	key := mappingKey(row)
	items := m.rows[key]
	if len(items) == 0 {
		return preparedSeriesMapping{}, false, nil
	}
	result := items[0]
	items[0].reads--
	if items[0].reads == 0 {
		items[0] = preparedSeriesMapping{}
		items = items[1:]
		m.count--
	}
	if len(items) == 0 {
		delete(m.rows, key)
	} else {
		m.rows[key] = items
	}
	return result, true, nil
}
func (m *preparedSeriesMappings) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.rows = nil
	m.count = 0
}
func (s *factorLiveSourceSink) mapRecord(row *orm.DataSeries, received int64) (factor.VersionRecord, error) {
	var record factor.VersionRecord
	var err error
	mappedAt := received
	prepared, ok, takeErr := s.mappings.take(row)
	if takeErr != nil {
		return record, takeErr
	}
	if ok {
		record, mappedAt = prepared.record, prepared.received
	} else {
		record, err = s.mapper(row, received)
	}
	if err == nil && (record.IngestedAt != mappedAt || record.AvailableAt > mappedAt || record.EventTime > mappedAt || mappedAt > received) {
		err = errors.New("runtime: factor live source invalid publication/reception time")
	}
	return record, err
}

func (g *factorLiveGeneration) mappingConsumer(sub *orm.Subscription, row *orm.DataSeries) *factorLiveSourceSink {
	consumer := g.sink.consumers[0]
	for _, candidate := range g.sink.consumers {
		if candidate.streams[sub.Source+"/"+sub.TimeFrame] && candidate.engine.HasSID(row.Sid) {
			consumer = candidate
			break
		}
	}
	return consumer
}
func (g *factorLiveGeneration) DiscardSeriesRevision(sub *orm.Subscription, row *orm.DataSeries) {
	g.mappingConsumer(sub, row).mappings.discard(row)
}

func (g *factorLiveGeneration) DiscardPreparedSeries() {
	for _, consumer := range g.sink.consumers {
		consumer.mappings.close()
	}
}
