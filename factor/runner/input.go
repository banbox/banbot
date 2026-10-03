package runner

import (
	"container/heap"
	"context"
	"errors"
	"io"
	"math"
	"sort"

	"github.com/banbox/banbot/factor"
)

// HistoricalBatch groups observations by their effective visibility time.
// Quotes and funding in a batch are reconciled before ObserveBatch is called.
type HistoricalBatch struct {
	AtMS    int64
	Records []factor.VersionRecord
}

func sortHistoricalRecords(rows []factor.VersionRecord) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.EventTime != b.EventTime {
			return a.EventTime < b.EventTime
		}
		if a.Series.Sid != b.Series.Sid {
			return a.Series.Sid < b.Series.Sid
		}
		if a.Series.Source != b.Series.Source {
			return a.Series.Source < b.Series.Source
		}
		if a.Series.TimeFrame != b.Series.TimeFrame {
			return a.Series.TimeFrame < b.Series.TimeFrame
		}
		return a.Revision < b.Revision
	})
}

// HistoricalInputFactory opens independent bounded readers. Identity includes
// immutable source metadata, storage namespace, policy and all input ranges.
type HistoricalInputFactory interface {
	Identity() string
	Ranges() []Chunk
	Open(context.Context, Config, Chunk) (HistoricalInput, error)
}
type HistoricalInput interface {
	Next(context.Context) (HistoricalBatch, error)
	// Visible returns the latest eligible event/revision for each stream, for
	// freezing one decision. Next separately retains every raw observation.
	Visible(context.Context, int64, int64, int64) ([]factor.VersionRecord, error)
	MaxRetainedRecords() int
	Close() error
}
type HistoricalWarmupInput interface{ WarmupFrom() int64 }

type archiveInput struct {
	rows            []factor.VersionRecord
	batches         []HistoricalBatch
	index, retained int
	visibility      []int
	visibleIndex    int
	pending         archiveEvents
	latest          map[factor.StreamKey]int
	reception       bool
	grid, now       int64
	advanced        bool
	closed          bool
}

// The raw chunk is immutable and bounded. Visibility and event indexes borrow
// its records; only public batches and decision rows receive owned clones.
type archiveEvents struct {
	rows    []factor.VersionRecord
	indexes []int
}

func (q archiveEvents) Len() int { return len(q.indexes) }
func (q archiveEvents) Less(i, j int) bool {
	return q.rows[q.indexes[i]].EventTime < q.rows[q.indexes[j]].EventTime
}
func (q archiveEvents) Swap(i, j int) { q.indexes[i], q.indexes[j] = q.indexes[j], q.indexes[i] }
func (q *archiveEvents) Push(v any)   { q.indexes = append(q.indexes, v.(int)) }
func (q *archiveEvents) Pop() any {
	last := len(q.indexes) - 1
	index := q.indexes[last]
	q.indexes = q.indexes[:last]
	return index
}

func archiveStream(row factor.VersionRecord) factor.StreamKey {
	return factor.StreamKey{SID: row.Series.Sid, Source: row.Series.Source, Frequency: row.Series.TimeFrame}
}

func newerArchiveRecord(row, old factor.VersionRecord) bool {
	return row.EventTime > old.EventTime || row.EventTime == old.EventTime && row.Revision > old.Revision
}

func openArchiveInput(ctx context.Context, c Config, chunk Chunk) (HistoricalInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store, err := factor.OpenVersionStore(chunk.Path, c.MaxRecords)
	if err != nil {
		return nil, err
	}
	rows, err := store.Records()
	if err != nil {
		return nil, err
	}
	input := &archiveInput{rows: rows, retained: len(rows), reception: c.Snapshot.ReplayTime != 0,
		latest: map[factor.StreamKey]int{}, pending: archiveEvents{rows: rows}}
	input.visibility = make([]int, len(rows))
	for i := range rows {
		input.visibility[i] = i
	}
	sort.Slice(input.visibility, func(i, j int) bool {
		return input.visibleAt(rows[input.visibility[i]]) < input.visibleAt(rows[input.visibility[j]])
	})
	groups := map[int64][]factor.VersionRecord{}
	funding := map[int32]bool{}
	all := c.ObserveBatch != nil || c.timeline != nil && c.timeline.allSourceEvents
	for _, r := range rows {
		if !all && r.Series.Source != c.Prices.Source && r.Series.Source != c.FundingSource {
			continue
		}
		at := max(r.EventTime, r.AvailableAt)
		if c.Snapshot.ReplayTime != 0 {
			at = max(at, r.IngestedAt)
		}
		if at < chunk.From || at > chunk.To {
			continue
		}
		groups[at] = append(groups[at], r)
		if r.Series.Source == c.FundingSource {
			funding[r.Series.Sid] = true
		}
	}
	if c.Manifest.Costs.FundingPolicy == "required-stream" {
		for _, sid := range c.Snapshot.Universe.Investable {
			if !funding[sid] {
				return nil, errors.New("runner: funding stream missing SID in chunk")
			}
		}
	}
	for at, records := range groups {
		input.batches = append(input.batches, HistoricalBatch{at, records})
	}
	sort.Slice(input.batches, func(i, j int) bool { return input.batches[i].AtMS < input.batches[j].AtMS })
	return input, nil
}
func (a *archiveInput) Next(ctx context.Context) (HistoricalBatch, error) {
	if err := ctx.Err(); err != nil {
		return HistoricalBatch{}, err
	}
	if a.closed {
		return HistoricalBatch{}, errors.New("runner: archive input closed")
	}
	if a.index == len(a.batches) {
		return HistoricalBatch{}, io.EOF
	}
	batch := a.batches[a.index]
	// Callback mutation must not alter the indexed archive used by decisions.
	owned := make([]factor.VersionRecord, len(batch.Records))
	for i, record := range batch.Records {
		clone, err := factor.CloneVersionRecord(record)
		if err != nil {
			return HistoricalBatch{}, err
		}
		owned[i] = clone
	}
	a.batches[a.index] = HistoricalBatch{}
	a.index++
	batch.Records = owned
	return batch, nil
}

func (a *archiveInput) visibleAt(row factor.VersionRecord) int64 {
	// The decision barrier always requires local reception by now, including
	// publication replay. Select after that gate so an unreceived newer event
	// cannot hide the older observation that the barrier would still accept.
	return max(row.AvailableAt, row.IngestedAt)
}

func (a *archiveInput) Visible(ctx context.Context, grid, now, replay int64) ([]factor.VersionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.closed {
		return nil, errors.New("runner: archive input closed")
	}
	selected := a.latest
	// Replay advances both publication and reception to now. Independent or
	// backward cutoffs remain valid queries, but never rewind the live cursor.
	aligned := !a.reception && replay == 0 || a.reception && replay == now
	if !aligned || a.advanced && (grid < a.grid || now < a.now) {
		selected = map[factor.StreamKey]int{}
		for i, row := range a.rows {
			if row.EventTime < 0 || row.EventTime > grid || row.AvailableAt > now || row.IngestedAt > now || replay != 0 && row.IngestedAt > replay {
				continue
			}
			key := archiveStream(row)
			if previous, exists := selected[key]; !exists || newerArchiveRecord(row, a.rows[previous]) {
				selected[key] = i
			}
		}
	} else {
		for a.visibleIndex < len(a.visibility) {
			i := a.visibility[a.visibleIndex]
			if a.visibleAt(a.rows[i]) > now {
				break
			}
			heap.Push(&a.pending, i)
			a.visibleIndex++
		}
		for a.pending.Len() > 0 && a.rows[a.pending.indexes[0]].EventTime <= grid {
			i := heap.Pop(&a.pending).(int)
			row := a.rows[i]
			if row.EventTime < 0 {
				continue
			}
			key := archiveStream(row)
			if previous, exists := selected[key]; !exists || newerArchiveRecord(row, a.rows[previous]) {
				selected[key] = i
			}
		}
		a.grid, a.now, a.advanced = grid, now, true
	}
	result := make([]factor.VersionRecord, 0, len(selected))
	for _, i := range selected {
		clone, err := factor.CloneVersionRecord(a.rows[i])
		if err != nil {
			return nil, err
		}
		result = append(result, clone)
	}
	sortHistoricalRecords(result)
	return result, nil
}
func (a *archiveInput) MaxRetainedRecords() int { return a.retained }
func (a *archiveInput) Close() error {
	a.closed = true
	a.rows, a.batches, a.visibility, a.latest = nil, nil, nil, nil
	a.pending = archiveEvents{}
	return nil
}

// replayInput merges source batches and decision grids without materializing
// the historical timeline. Its only lookahead is one bounded source batch.
type replayInput struct {
	input                     HistoricalInput
	next                      HistoricalBatch
	loaded, ended             bool
	decision, last, end, step int64
}

func newReplayInput(input HistoricalInput, c Config, chunk Chunk) *replayInput {
	from := chunk.From
	if warmup, ok := input.(HistoricalWarmupInput); ok && warmup.WarmupFrom() < from {
		lookback := from - warmup.WarmupFrom()
		steps := lookback / c.DecisionInterval
		if lookback%c.DecisionInterval != 0 {
			steps++
		}
		if steps <= from/c.DecisionInterval {
			from -= steps * c.DecisionInterval
		}
	}
	next := int64(math.MaxInt64)
	if from <= math.MaxInt64-c.DecisionDelayMS {
		next = from + c.DecisionDelayMS
	}
	return &replayInput{input: input, decision: next, end: chunk.To, step: c.DecisionInterval, last: -1}
}
func (r *replayInput) Next(ctx context.Context) (HistoricalBatch, error) {
	if !r.loaded && !r.ended {
		next, err := r.input.Next(ctx)
		if errors.Is(err, io.EOF) {
			r.ended = true
		} else if err != nil {
			return HistoricalBatch{}, err
		} else {
			if next.AtMS <= r.last {
				return HistoricalBatch{}, errors.New("runner: historical batches must be strictly ordered")
			}
			r.next = next
			r.loaded = true
		}
	}
	at := r.decision
	if at > r.end {
		at = math.MaxInt64
	}
	if r.loaded {
		at = min(at, r.next.AtMS)
	}
	if at == math.MaxInt64 {
		return HistoricalBatch{}, io.EOF
	}
	result := HistoricalBatch{AtMS: at}
	if r.loaded && r.next.AtMS == at {
		result.Records = r.next.Records
		r.loaded = false
		r.last = at
	}
	if r.decision == at {
		if at > math.MaxInt64-r.step {
			r.decision = math.MaxInt64
		} else {
			r.decision += r.step
		}
	}
	return result, nil
}
