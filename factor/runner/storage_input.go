package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

// Cleanup plan: feed paged storage through the existing replay loop, preserving
// arbitrary Values and specialized readers, with explicit visibility policy.
type StorageStream struct {
	Subscription              orm.Subscription
	WarmupStartMS             int64
	SourceVersion, SchemaHash string
}
type StorageInputOptions struct {
	Namespace, PITPolicy   string
	FromMS, ToMS           int64
	PageRows, PrefetchRows int
	Streams                []StorageStream
	QueryPage              func(context.Context, orm.Subscription, int64, int64, int) ([]*orm.DataSeries, error)
	VersionPages           VersionPageFactory
}

// VersionPageFactory is an explicit PIT-capable storage adapter. It must return
// all immutable revisions in effective visibility order, preserving publication
// and reception timestamps. Latest-value SQL tables do not satisfy this contract.
type VersionPageFactory interface {
	Identity() string
	Open(context.Context, StorageStream, int64, int64, bool) (VersionPageReader, error)
}
type VersionPageReader interface {
	NextPage(context.Context, int) ([]factor.VersionRecord, error)
	Close() error
}
type storageInputFactory struct {
	options  StorageInputOptions
	identity string
}

func NewStorageInputFactory(options StorageInputOptions) (HistoricalInputFactory, error) {
	if options.Namespace == "" || options.FromMS <= 0 || options.ToMS < options.FromMS || len(options.Streams) == 0 || options.PageRows <= 0 || options.PrefetchRows < 0 {
		return nil, errors.New("runner: storage input requires namespace, range, streams and positive page budget")
	}
	if options.PITPolicy != "strict" && options.PITPolicy != "static-approximation" {
		return nil, errors.New("runner: storage input requires explicit strict or static-approximation PIT policy")
	}
	if options.PITPolicy == "strict" && (options.VersionPages == nil || options.VersionPages.Identity() == "") {
		return nil, errors.New("runner: strict PIT requires an attested immutable revision reader; latest-value storage is insufficient")
	}
	if options.PITPolicy == "static-approximation" && options.QueryPage == nil {
		return nil, errors.New("runner: static approximation requires a storage page reader")
	}
	streams := make([]StorageStream, len(options.Streams))
	seen := map[string]bool{}
	for i, stream := range options.Streams {
		sub := stream.Subscription
		if sub.ExSymbol == nil || sub.ExSymbol.ID <= 0 || sub.Source == "" || sub.TimeFrame == "" || stream.WarmupStartMS < 0 || stream.WarmupStartMS > options.FromMS || stream.SourceVersion == "" || stream.SchemaHash == "" || seen[sub.Key().String()] {
			return nil, errors.New("runner: storage stream requires unique identity, warmup range and source/schema version")
		}
		seen[sub.Key().String()] = true
		symbol := *sub.ExSymbol
		sub.ExSymbol = &symbol
		sub.Fields = append([]string(nil), sub.Fields...)
		sub.SeriesFields = append([]string(nil), sub.SeriesFields...)
		stream.Subscription = sub
		streams[i] = stream
	}
	sort.Slice(streams, func(i, j int) bool {
		return streams[i].Subscription.Key().String() < streams[j].Subscription.Key().String()
	})
	options.Streams = streams
	if options.PrefetchRows == 0 {
		options.PrefetchRows = options.PageRows * len(streams)
	}
	if options.PrefetchRows < len(streams) {
		return nil, errors.New("runner: prefetch budget cannot hold one row per stream")
	}
	options.PageRows = min(options.PageRows, options.PrefetchRows/len(streams))
	versionIdentity := ""
	if options.VersionPages != nil {
		versionIdentity = options.VersionPages.Identity()
	}
	raw, err := json.Marshal(struct {
		Namespace, Policy, VersionIdentity string
		From, To                           int64
		Page, Prefetch                     int
		Streams                            []StorageStream
	}{options.Namespace, options.PITPolicy, versionIdentity, options.FromMS, options.ToMS, options.PageRows, options.PrefetchRows, streams})
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	return &storageInputFactory{options: options, identity: hex.EncodeToString(digest[:])}, nil
}
func (f *storageInputFactory) Identity() string { return f.identity }
func (f *storageInputFactory) Ranges() []Chunk {
	return []Chunk{{From: f.options.FromMS, To: f.options.ToMS}}
}
func (f *storageInputFactory) Open(ctx context.Context, c Config, chunk Chunk) (HistoricalInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if chunk.From != f.options.FromMS || chunk.To != f.options.ToMS || c.Snapshot.VisibilityPolicy != f.options.PITPolicy {
		return nil, errors.New("runner: storage range or visibility policy differs from compiled input")
	}
	if c.MaxRecords < f.options.PrefetchRows+len(f.options.Streams) {
		return nil, errors.New("runner: retained record budget cannot hold prefetch plus stream snapshots")
	}
	input := &storageInput{factory: f, config: c, latest: map[factor.StreamKey][]factor.VersionRecord{}}
	for _, stream := range f.options.Streams {
		if c.Snapshot.SourceVersions[stream.Subscription.Source] != stream.SourceVersion || c.Snapshot.Schemas[stream.Subscription.Source] != stream.SchemaHash {
			return nil, fmt.Errorf("runner: storage source %s metadata differs from snapshot", stream.Subscription.Source)
		}
		cursor := &storageCursor{stream: stream, offset: stream.WarmupStartMS, lastAt: -1}
		if f.options.PITPolicy == "strict" {
			reader, err := f.options.VersionPages.Open(ctx, stream, stream.WarmupStartMS, chunk.To, c.Snapshot.ReplayTime != 0)
			if err != nil {
				_ = input.Close()
				return nil, err
			}
			if reader == nil {
				_ = input.Close()
				return nil, errors.New("runner: strict reader returned nil handle")
			}
			cursor.version = reader
		}
		input.cursors = append(input.cursors, cursor)
	}
	return input, nil
}

type storageCursor struct {
	stream         StorageStream
	version        VersionPageReader
	offset, lastAt int64
	rows           []factor.VersionRecord
	index          int
	done           bool
}
type storageInput struct {
	factory           *storageInputFactory
	config            Config
	cursors           []*storageCursor
	latest            map[factor.StreamKey][]factor.VersionRecord
	retained, maximum int
	closed            bool
}

func (s *storageInput) WarmupFrom() int64 {
	from := s.factory.options.FromMS
	for _, stream := range s.factory.options.Streams {
		from = min(from, stream.WarmupStartMS)
	}
	return from
}

func (s *storageInput) visibility(r factor.VersionRecord) int64 {
	at := max(r.EventTime, r.AvailableAt)
	if s.config.Snapshot.ReplayTime != 0 {
		at = max(at, r.IngestedAt)
	}
	return at
}
func (s *storageInput) fill(ctx context.Context, cursor *storageCursor) error {
	if cursor.done || cursor.index < len(cursor.rows) {
		return nil
	}
	cursor.rows = nil
	cursor.index = 0
	options := s.factory.options
	var records []factor.VersionRecord
	if cursor.version != nil {
		rows, err := cursor.version.NextPage(ctx, options.PageRows)
		if errors.Is(err, io.EOF) {
			cursor.done = true
			return nil
		}
		if err != nil {
			return err
		}
		records = rows
	} else {
		if cursor.offset > options.ToMS {
			cursor.done = true
			return nil
		}
		end := options.ToMS
		if end < math.MaxInt64 {
			end++
		}
		rows, err := options.QueryPage(ctx, cursor.stream.Subscription, cursor.offset, end, options.PageRows)
		if err != nil {
			return err
		}
		if len(rows) > options.PageRows {
			return errors.New("runner: page reader exceeded requested row budget")
		}
		var lastTime int64 = -1
		for _, row := range rows {
			if row == nil || row.TimeMS < cursor.offset || row.TimeMS <= lastTime || row.TimeMS >= end {
				return errors.New("runner: static page returned nil, duplicate, unordered or out-of-range observation")
			}
			lastTime = row.TimeMS
			event := row.EndMS
			if event <= 0 {
				event = row.TimeMS
			}
			copySeries := *row
			copySeries.IsWarmUp = event <= options.FromMS
			records = append(records, factor.VersionRecord{Series: copySeries, EventTime: event, Revision: 1, AvailableAt: event, IngestedAt: event, SourceVersion: cursor.stream.SourceVersion})
		}
		if len(rows) > 0 {
			if lastTime == math.MaxInt64 {
				cursor.done = true
			} else {
				cursor.offset = lastTime + 1
			}
		}
	}
	if len(records) > options.PageRows {
		return errors.New("runner: page reader exceeded requested row budget")
	}
	if len(records) == 0 {
		cursor.done = true
		return nil
	}
	for _, record := range records {
		if record.Series.Sid != cursor.stream.Subscription.ExSymbol.ID || record.Series.Source != cursor.stream.Subscription.Source || record.Series.TimeFrame != cursor.stream.Subscription.TimeFrame || record.SourceVersion != cursor.stream.SourceVersion {
			return errors.New("runner: storage page returned foreign stream/version")
		}
		at := s.visibility(record)
		if at < cursor.lastAt {
			return errors.New("runner: storage pages are not visibility ordered")
		}
		cursor.lastAt = at
		clone, err := factor.CloneVersionRecord(record)
		if err != nil {
			return err
		}
		cursor.rows = append(cursor.rows, clone)
	}
	return s.checkBound()
}
func (s *storageInput) checkBound() error {
	count := s.retained
	for _, cursor := range s.cursors {
		count += len(cursor.rows) - cursor.index
	}
	s.maximum = max(s.maximum, count)
	if count > s.config.MaxRecords {
		return errors.New("runner: storage replay retained record budget exceeded")
	}
	return nil
}
func (s *storageInput) Next(ctx context.Context) (HistoricalBatch, error) {
	if s.closed {
		return HistoricalBatch{}, errors.New("runner: storage input closed")
	}
	if err := ctx.Err(); err != nil {
		return HistoricalBatch{}, err
	}
	next := int64(math.MaxInt64)
	for _, cursor := range s.cursors {
		if err := s.fill(ctx, cursor); err != nil {
			return HistoricalBatch{}, err
		}
		if cursor.index < len(cursor.rows) {
			next = min(next, s.visibility(cursor.rows[cursor.index]))
		}
	}
	if next == math.MaxInt64 || next > s.factory.options.ToMS {
		return HistoricalBatch{}, io.EOF
	}
	batch := HistoricalBatch{AtMS: next}
	for _, cursor := range s.cursors {
		for {
			if cursor.index == len(cursor.rows) {
				if err := s.fill(ctx, cursor); err != nil {
					return HistoricalBatch{}, err
				}
			}
			if cursor.index == len(cursor.rows) || s.visibility(cursor.rows[cursor.index]) != next {
				break
			}
			record := cursor.rows[cursor.index]
			cursor.rows[cursor.index] = factor.VersionRecord{}
			cursor.index++
			clone, err := factor.CloneVersionRecord(record)
			if err != nil {
				return HistoricalBatch{}, err
			}
			key := factor.StreamKey{SID: record.Series.Sid, Source: record.Series.Source, TimeFrame: record.Series.TimeFrame}
			s.latest[key] = append(s.latest[key], clone)
			s.retained++
			batch.Records = append(batch.Records, record)
			if err := s.checkBound(); err != nil {
				return HistoricalBatch{}, err
			}
		}
	}
	sortHistoricalRecords(batch.Records)
	return batch, nil
}
func (s *storageInput) Visible(ctx context.Context, grid, now, replay int64) ([]factor.VersionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result []factor.VersionRecord
	for key, rows := range s.latest {
		latest := -1
		for i, row := range rows {
			if row.EventTime > grid || row.AvailableAt > now || replay != 0 && row.IngestedAt > replay {
				continue
			}
			if latest < 0 || row.EventTime > rows[latest].EventTime || row.EventTime == rows[latest].EventTime && row.Revision > rows[latest].Revision {
				latest = i
			}
		}
		if latest < 0 {
			continue
		}
		selected := rows[latest]
		clone, err := factor.CloneVersionRecord(selected)
		if err != nil {
			return nil, err
		}
		result = append(result, clone)
		// Future event rows survive a delayed decision. Older observations cannot
		// affect a later grid; late revisions of them remain behind selected event.
		kept := rows[:0]
		for i, row := range rows {
			if i == latest || row.EventTime > grid {
				kept = append(kept, row)
			} else {
				s.retained--
			}
		}
		clear(rows[len(kept):])
		s.latest[key] = kept
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.EventTime != b.EventTime {
			return a.EventTime < b.EventTime
		}
		if a.Series.Sid != b.Series.Sid {
			return a.Series.Sid < b.Series.Sid
		}
		if a.Series.Source != b.Series.Source {
			return a.Series.Source < b.Series.Source
		}
		return a.Series.TimeFrame < b.Series.TimeFrame
	})
	return result, nil
}
func (s *storageInput) MaxRetainedRecords() int { return s.maximum }
func (s *storageInput) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	for _, cursor := range s.cursors {
		if cursor.version != nil {
			err = errors.Join(err, cursor.version.Close())
		}
		cursor.rows = nil
	}
	s.latest = nil
	return err
}
