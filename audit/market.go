// Package audit builds read-only review artifacts. It has no database writer or
// execution sender; review output never authorizes a live overwrite or repair.
package audit

import (
	"fmt"
	"math/big"
	"reflect"
	"sort"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

type rowKey struct {
	Source, Frame string
	SID           int32
	Time          int64
}

func key(row *orm.DataSeries) rowKey { return rowKey{row.Source, row.TimeFrame, row.Sid, row.TimeMS} }
func clone(row *orm.DataSeries) (factor.VersionRecord, error) {
	if row == nil || row.TimeMS < 0 {
		return factor.VersionRecord{}, fmt.Errorf("audit: invalid series row")
	}
	return factor.CloneVersionRecord(factor.VersionRecord{Series: *row, EventTime: row.TimeMS, Revision: 1, SourceVersion: "refresh-review-v1"})
}
func index(rows []*orm.DataSeries) (map[rowKey]factor.VersionRecord, error) {
	out := make(map[rowKey]factor.VersionRecord, len(rows))
	for _, row := range rows {
		copy, err := clone(row)
		if err != nil {
			return nil, err
		}
		k := key(&copy.Series)
		if _, exists := out[k]; exists {
			return nil, fmt.Errorf("audit: duplicate series identity: %+v", k)
		}
		out[k] = copy
	}
	return out, nil
}

type RowChange struct {
	Source          string   `json:"source"`
	TimeFrame       string   `json:"timeframe"`
	SID             int32    `json:"sid"`
	TimeMS          int64    `json:"time_ms"`
	Kind            string   `json:"kind"`
	BeforeHash      string   `json:"before_hash,omitempty"`
	AfterHash       string   `json:"after_hash"`
	ChangedFields   []string `json:"changed_fields,omitempty"`
	RetainedFields  []string `json:"retained_fields,omitempty"`
	MetadataChanged bool     `json:"metadata_changed,omitempty"`
}
type RefreshReport struct {
	BaselineRows  int         `json:"baseline_rows"`
	CandidateRows int         `json:"candidate_rows"`
	BaselineOnly  int         `json:"baseline_only_rows_not_deleted"`
	Changes       []RowChange `json:"changes"`
}

// StageRefresh owns all values in its result. A candidate's absent fields retain
// their baseline values; explicit NULL remains an intentional candidate value.
// Baseline-only rows are reported, never interpreted as deletion requests.
func StageRefresh(baseline, candidate []*orm.DataSeries) ([]factor.VersionRecord, RefreshReport, error) {
	report := RefreshReport{BaselineRows: len(baseline), CandidateRows: len(candidate)}
	old, err := index(baseline)
	if err != nil {
		return nil, report, err
	}
	fresh, err := index(candidate)
	if err != nil {
		return nil, report, err
	}
	for k := range old {
		if _, ok := fresh[k]; !ok {
			report.BaselineOnly++
		}
	}
	staged := make([]factor.VersionRecord, 0, len(candidate))
	for _, input := range candidate {
		k := key(input)
		row := fresh[k]
		change := RowChange{Source: k.Source, TimeFrame: k.Frame, SID: k.SID, TimeMS: k.Time, Kind: "added"}
		previous, exists := old[k]
		if exists {
			change.Kind = "changed"
			change.MetadataChanged = previous.Series.EndMS != row.Series.EndMS || previous.Series.Closed != row.Series.Closed || previous.Series.IsWarmUp != row.Series.IsWarmUp
			change.BeforeHash, err = factor.ContentHash(previous.Series)
			if err != nil {
				return nil, report, err
			}
			for field, value := range previous.Series.Values {
				if _, present := row.Series.Values[field]; !present {
					if row.Series.Values == nil {
						row.Series.Values = map[string]any{}
					}
					row.Series.Values[field] = value
					change.RetainedFields = append(change.RetainedFields, field)
				}
			}
		}
		for field, value := range row.Series.Values {
			before, present := previous.Series.Values[field]
			beforeHash, hashErr := factor.ContentHash(before)
			if hashErr != nil {
				return nil, report, hashErr
			}
			afterHash, hashErr := factor.ContentHash(value)
			if hashErr != nil {
				return nil, report, hashErr
			}
			if !exists || !present || beforeHash != afterHash {
				change.ChangedFields = append(change.ChangedFields, field)
			}
		}
		sort.Strings(change.ChangedFields)
		sort.Strings(change.RetainedFields)
		change.AfterHash, err = factor.ContentHash(row.Series)
		if err != nil {
			return nil, report, err
		}
		if exists && change.BeforeHash == change.AfterHash && len(change.RetainedFields) > 0 {
			change.Kind = "retained"
		}
		if !exists || change.BeforeHash != change.AfterHash || len(change.RetainedFields) > 0 {
			report.Changes = append(report.Changes, change)
		}
		staged = append(staged, row)
	}
	sort.Slice(report.Changes, func(i, j int) bool {
		a, b := report.Changes[i], report.Changes[j]
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		if a.TimeFrame != b.TimeFrame {
			return a.TimeFrame < b.TimeFrame
		}
		if a.SID != b.SID {
			return a.SID < b.SID
		}
		return a.TimeMS < b.TimeMS
	})
	return staged, report, nil
}

type FlatPolicy struct {
	MinSymbols  int
	MinFraction float64
}
type FlatAlert struct {
	Source          string  `json:"source"`
	TimeFrame       string  `json:"timeframe"`
	TimeMS          int64   `json:"time_ms"`
	FlatSymbols     int     `json:"flat_symbols"`
	ObservedSymbols int     `json:"observed_symbols"`
	SIDs            []int32 `json:"sids"`
}

func exactNumber(value any) (*big.Rat, bool) {
	if value == nil {
		return nil, false
	}
	v := reflect.ValueOf(value)
	result := new(big.Rat)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return result.SetInt64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return result.SetInt(new(big.Int).SetUint64(v.Uint())), true
	case reflect.Float32, reflect.Float64:
		result = result.SetFloat64(v.Float())
		return result, result != nil
	default:
		return nil, false
	}
}

func isFlatZeroVolume(values map[string]any) bool {
	open, valid := exactNumber(values["open"])
	if !valid {
		return false
	}
	for _, field := range []string{"high", "low", "close"} {
		value, valid := exactNumber(values[field])
		if !valid || open.Cmp(value) != 0 {
			return false
		}
	}
	volume, valid := exactNumber(values["volume"])
	return valid && volume.Sign() == 0
}

// DetectFlatMarket emits warnings, not invalidity verdicts. Its denominator is
// the symbols observed in the supplied snapshot, not an asserted venue universe.
func DetectFlatMarket(rows []*orm.DataSeries, policy FlatPolicy) ([]FlatAlert, error) {
	if policy.MinSymbols < 2 || !(policy.MinFraction > 0 && policy.MinFraction <= 1) {
		return nil, fmt.Errorf("audit: explicit flat-market thresholds required")
	}
	indexed, err := index(rows)
	if err != nil {
		return nil, err
	}
	type groupKey struct {
		Source, Frame string
		Time          int64
	}
	groups := map[groupKey]*FlatAlert{}
	for k, record := range indexed {
		g := groupKey{k.Source, k.Frame, k.Time}
		alert := groups[g]
		if alert == nil {
			alert = &FlatAlert{Source: k.Source, TimeFrame: k.Frame, TimeMS: k.Time}
			groups[g] = alert
		}
		alert.ObservedSymbols++
		if isFlatZeroVolume(record.Series.Values) {
			alert.FlatSymbols++
			alert.SIDs = append(alert.SIDs, k.SID)
		}
	}
	var out []FlatAlert
	for _, alert := range groups {
		if alert.FlatSymbols >= policy.MinSymbols && float64(alert.FlatSymbols)/float64(alert.ObservedSymbols) >= policy.MinFraction {
			sort.Slice(alert.SIDs, func(i, j int) bool { return alert.SIDs[i] < alert.SIDs[j] })
			out = append(out, *alert)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		if a.TimeFrame != b.TimeFrame {
			return a.TimeFrame < b.TimeFrame
		}
		return a.TimeMS < b.TimeMS
	})
	return out, nil
}
