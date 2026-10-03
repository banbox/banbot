package factor

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/banbox/banbot/orm"
)

// VersionRecord has a separate logical revision identity. EventTime is never
// altered to manufacture uniqueness in existing SID/timestamp tables.
type VersionRecord struct {
	Series        orm.DataSeries
	EventTime     int64
	Revision      uint64
	AvailableAt   int64
	IngestedAt    int64
	SourceVersion string
}

type recordKey struct {
	Source    string
	Frequency string
	SID       int32
	Event     int64
	Revision  uint64
}

func (r VersionRecord) key() recordKey {
	return recordKey{r.Series.Source, r.Series.TimeFrame, r.Series.Sid, r.EventTime, r.Revision}
}

func cloneRecord(record VersionRecord) (VersionRecord, error) {
	values, err := cloneValues(record.Series.Values)
	if err != nil {
		return VersionRecord{}, err
	}
	// Instrument identities belong in the snapshot SID manifest, not mutable
	// runtime symbol/adjustment objects borrowed from a DataSeries callback.
	record.Series = orm.DataSeries{Source: record.Series.Source, Sid: record.Series.Sid, TimeMS: record.Series.TimeMS, EndMS: record.Series.EndMS, TimeFrame: record.Series.TimeFrame, Closed: record.Series.Closed, IsWarmUp: record.Series.IsWarmUp, Values: values}
	return record, nil
}

// CloneVersionRecord validates one source observation and takes ownership of
// its raw Values without creating an archive store. Concrete field types and
// NULLs are preserved; mutable runtime symbol/adjustment objects are omitted.
func CloneVersionRecord(record VersionRecord) (VersionRecord, error) {
	if record.Series.Sid <= 0 || record.Series.Source == "" || record.Series.TimeFrame == "" || record.SourceVersion == "" || record.Revision == 0 || record.AvailableAt < 0 || record.IngestedAt < 0 {
		return VersionRecord{}, errors.New("factor: invalid version identity/visibility")
	}
	return cloneRecord(record)
}

// VersionStore holds one explicitly bounded raw source chunk. Export/reopen
// provides immutable archives; callers load only the SID/time chunk required
// by a run instead of expanding an entire multi-year archive in memory.
type VersionStore struct {
	mu    sync.RWMutex
	limit int
	rows  map[recordKey]VersionRecord
}

func NewVersionStore(maxRecords int) (*VersionStore, error) {
	if maxRecords <= 0 {
		return nil, errors.New("factor: version chunk requires positive record bound")
	}
	return &VersionStore{limit: maxRecords, rows: make(map[recordKey]VersionRecord)}, nil
}

func (s *VersionStore) Put(record VersionRecord) error {
	copy, err := CloneVersionRecord(record)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, exists := s.rows[record.key()]; exists {
		oldHash, err := contentHash(old)
		if err != nil {
			return err
		}
		newHash, err := contentHash(copy)
		if err != nil {
			return err
		}
		if oldHash != newHash {
			return errors.New("factor: conflicting immutable revision")
		}
		return nil
	}
	if len(s.rows) >= s.limit {
		return errors.New("factor: version chunk record bound exceeded")
	}
	s.rows[record.key()] = copy
	return nil
}

// Visible selects the highest then-visible revision per logical source/SID/
// event key. replayAt=0 disables reception replay filtering explicitly.
func (s *VersionStore) Visible(from, to, asOf, replayAt int64) ([]VersionRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	selected := make(map[recordKey]VersionRecord)
	for _, row := range s.rows {
		if row.EventTime < from || row.EventTime > to || row.AvailableAt > asOf || (replayAt != 0 && row.IngestedAt > replayAt) {
			continue
		}
		key := row.key()
		key.Revision = 0
		if previous, exists := selected[key]; !exists || previous.Revision < row.Revision {
			selected[key] = row
		}
	}
	result := make([]VersionRecord, 0, len(selected))
	for _, row := range selected {
		copy, err := cloneRecord(row)
		if err != nil {
			return nil, err
		}
		result = append(result, copy)
	}
	sortRecords(result)
	return result, nil
}

// Records returns every raw revision in this bounded chunk, in logical record
// order. Replay callers retain publication/reception history instead of using
// Visible, which intentionally collapses revisions at one visibility cutoff.
func (s *VersionStore) Records() ([]VersionRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]VersionRecord, 0, len(s.rows))
	for _, row := range s.rows {
		copy, err := cloneRecord(row)
		if err != nil {
			return nil, err
		}
		result = append(result, copy)
	}
	sortRecords(result)
	return result, nil
}

func sortRecords(rows []VersionRecord) {
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

type versionHeader struct {
	Format int
	Count  int
	Hash   string
}

func init() { gob.Register(map[string]any{}); gob.Register([]any{}); gob.Register([]string{}) }

// Export publishes a new immutable file only after its contents are complete.
// Gob keeps concrete Values types; unregistered custom types return an error.
// The stable digest is over logical typed contents, never gob map bytes.
func (s *VersionStore) Export(path string) (string, error) {
	s.mu.RLock()
	rows := make([]VersionRecord, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, row)
	}
	s.mu.RUnlock()
	sortRecords(rows)
	hash, err := contentHash(rows)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".factor-version-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	encoder := gob.NewEncoder(file)
	if err = encoder.Encode(versionHeader{1, len(rows), hash}); err != nil {
		return "", err
	}
	for _, row := range rows {
		if err = encoder.Encode(row); err != nil {
			return "", err
		}
	}
	if err = file.Sync(); err != nil {
		return "", err
	}
	if err = file.Close(); err != nil {
		return "", err
	}
	// Link publishes atomically without overwriting an existing archive.
	if err = os.Link(file.Name(), path); err != nil {
		if os.IsExist(err) {
			existing, openErr := OpenVersionStore(path, s.limit)
			if openErr != nil {
				return "", openErr
			}
			existingRows := make([]VersionRecord, 0, len(existing.rows))
			for _, row := range existing.rows {
				existingRows = append(existingRows, row)
			}
			sortRecords(existingRows)
			existingHash, hashErr := contentHash(existingRows)
			if hashErr != nil {
				return "", hashErr
			}
			if existingHash == hash {
				return hash, nil
			}
			return "", errors.New("factor: immutable archive content conflict")
		}
		return "", err
	}
	// File contents are synced, but directory-entry power-loss durability is
	// not claimed here: Windows does not support the portable directory fsync.
	return hash, nil
}

func OpenVersionStore(path string, maxRecords int) (*VersionStore, error) {
	store, err := NewVersionStore(maxRecords)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	var header versionHeader
	if err = decoder.Decode(&header); err != nil {
		return nil, err
	}
	if header.Format != 1 || header.Count < 0 || header.Count > maxRecords {
		return nil, errors.New("factor: unsupported/oversized version chunk")
	}
	rows := make([]VersionRecord, 0, header.Count)
	for i := 0; i < header.Count; i++ {
		var row VersionRecord
		if err = decoder.Decode(&row); err != nil {
			return nil, err
		}
		if err = store.Put(row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	sortRecords(rows)
	hash, err := contentHash(rows)
	if err != nil {
		return nil, err
	}
	if hash != header.Hash {
		return nil, fmt.Errorf("factor: version chunk hash mismatch")
	}
	return store, nil
}
