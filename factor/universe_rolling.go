package factor

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	DefaultRollingLookbackDays     = 30
	DefaultRollingMinListingDays   = 14
	DefaultRollingReconstituteDays = 30
	rollingDayMillis               = int64(86_400_000)
	maxRollingBound                = 10000
)

// RollingUniverseConfig configures point-in-time rolling liquidity universe reconstitution.
// Candidate symbols from the configured superset are ranked at each reconstitution boundary t_r
// using strictly visible bars <= t_r over the trailing lookback window.
type RollingUniverseConfig struct {
	Mode             string   `json:"mode,omitempty" yaml:"mode,omitempty"`
	TopN             int      `json:"top_n,omitempty" yaml:"top_n,omitempty"`
	LookbackDays     int      `json:"lookback_days,omitempty" yaml:"lookback_days,omitempty"`
	LookbackBars     int      `json:"lookback_bars,omitempty" yaml:"lookback_bars,omitempty"`
	MinListingDays   int      `json:"min_listing_days,omitempty" yaml:"min_listing_days,omitempty"`
	MinAgeBars       int      `json:"min_age_bars,omitempty" yaml:"min_age_bars,omitempty"`
	ReconstituteDays int      `json:"reconstitute_days,omitempty" yaml:"reconstitute_days,omitempty"`
	ReconstituteBars int      `json:"reconstitute_bars,omitempty" yaml:"reconstitute_bars,omitempty"`
	Schedule         string   `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	AlwaysInclude    []string `json:"always_include,omitempty" yaml:"always_include,omitempty"`
}

func CloneRollingUniverseConfig(c *RollingUniverseConfig) *RollingUniverseConfig {
	if c == nil {
		return nil
	}
	cp := *c
	cp.AlwaysInclude = slices.Clone(c.AlwaysInclude)
	return &cp
}

func (c *RollingUniverseConfig) Enabled() bool {
	if c == nil {
		return false
	}
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	if mode == "static" {
		return false
	}
	return c.TopN > 0 || mode == "rolling_liquid_top" || mode == "liquid_top"
}

func NormalizeRollingUniverseConfig(c RollingUniverseConfig) (RollingUniverseConfig, error) {
	c.AlwaysInclude = slices.Clone(c.AlwaysInclude)
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	switch mode {
	case "static":
		if c.TopN != 0 || c.LookbackDays != 0 || c.LookbackBars != 0 || c.MinListingDays != 0 || c.MinAgeBars != 0 || c.ReconstituteDays != 0 || c.ReconstituteBars != 0 || strings.TrimSpace(c.Schedule) != "" || len(c.AlwaysInclude) > 0 {
			return c, errors.New("factor: static universe mode cannot declare rolling parameters")
		}
		c.Mode = "static"
		return c, nil
	case "", "rolling_liquid_top", "liquid_top":
		if c.TopN <= 0 {
			if mode == "" && c.LookbackDays == 0 && c.LookbackBars == 0 && c.MinListingDays == 0 && c.MinAgeBars == 0 && c.ReconstituteDays == 0 && c.ReconstituteBars == 0 && strings.TrimSpace(c.Schedule) == "" && len(c.AlwaysInclude) == 0 {
				return RollingUniverseConfig{}, nil
			}
			return c, errors.New("factor: rolling universe requires positive top_n")
		}
		c.Mode = "rolling_liquid_top"
	default:
		return c, fmt.Errorf("factor: unknown rolling universe mode %q", c.Mode)
	}
	if c.TopN <= 0 || c.TopN > maxRollingBound {
		return c, errors.New("factor: rolling universe top_n must be in [1, 10000]")
	}
	if c.LookbackDays < 0 || c.LookbackBars < 0 || c.LookbackDays > maxRollingBound || c.LookbackBars > maxRollingBound {
		return c, errors.New("factor: rolling universe lookback must be in [1, 10000]")
	}
	if c.LookbackDays > 0 && c.LookbackBars > 0 {
		return c, errors.New("factor: rolling universe lookback_days and lookback_bars conflict")
	}
	barDriven := (c.LookbackBars > 0 || c.ReconstituteBars > 0) && c.LookbackDays == 0 && c.ReconstituteDays == 0 && strings.TrimSpace(c.Schedule) == ""
	if c.LookbackDays == 0 && c.LookbackBars == 0 {
		c.LookbackDays = DefaultRollingLookbackDays
	}

	if c.MinListingDays < -1 || c.MinAgeBars < -1 || c.MinListingDays > maxRollingBound || c.MinAgeBars > maxRollingBound {
		return c, errors.New("factor: invalid rolling universe min_listing_days/min_age_bars")
	}
	if c.MinListingDays != 0 && c.MinAgeBars != 0 {
		return c, errors.New("factor: rolling universe min_listing_days and min_age_bars conflict")
	}
	if c.MinListingDays == 0 && c.MinAgeBars == 0 && !barDriven {
		c.MinListingDays = DefaultRollingMinListingDays
	}

	if c.ReconstituteDays < 0 || c.ReconstituteBars < 0 || c.ReconstituteDays > maxRollingBound || c.ReconstituteBars > maxRollingBound {
		return c, errors.New("factor: invalid rolling universe reconstitute_days/reconstitute_bars")
	}
	sched := strings.ToLower(strings.TrimSpace(c.Schedule))
	c.Schedule = sched
	if sched != "" && sched != "daily" && sched != "weekly" && sched != "monthly" {
		return c, fmt.Errorf("factor: unknown rolling universe schedule %q", c.Schedule)
	}
	reconstCount := 0
	for _, active := range []bool{c.ReconstituteDays > 0, c.ReconstituteBars > 0, sched != ""} {
		if active {
			reconstCount++
		}
	}
	if reconstCount > 1 {
		return c, errors.New("factor: rolling universe reconstitute_days, reconstitute_bars and schedule conflict")
	}
	if reconstCount == 0 {
		c.ReconstituteDays = DefaultRollingReconstituteDays
	}

	if len(c.AlwaysInclude) > 0 {
		cleaned := make([]string, 0, len(c.AlwaysInclude))
		seen := map[string]bool{}
		for _, item := range c.AlwaysInclude {
			trimmed := strings.TrimSpace(item)
			if trimmed == "" {
				return c, errors.New("factor: rolling universe always_include cannot contain blank symbol")
			}
			key := strings.ToUpper(trimmed)
			if !seen[key] {
				seen[key] = true
				cleaned = append(cleaned, trimmed)
			}
		}
		c.AlwaysInclude = cleaned
	} else {
		c.AlwaysInclude = nil
	}
	return c, nil
}

func (c RollingUniverseConfig) LookbackDurationMS(barMS int64) int64 {
	if c.LookbackBars > 0 {
		return int64(c.LookbackBars) * barMS
	}
	days := c.LookbackDays
	if days <= 0 {
		days = DefaultRollingLookbackDays
	}
	return int64(days) * rollingDayMillis
}

func (c RollingUniverseConfig) MinAgeDurationMS(barMS int64) int64 {
	if c.MinListingDays < 0 || c.MinAgeBars < 0 {
		return 0
	}
	if c.MinAgeBars > 0 {
		return int64(c.MinAgeBars) * barMS
	}
	if c.MinListingDays > 0 {
		return int64(c.MinListingDays) * rollingDayMillis
	}
	return 0
}

func (c RollingUniverseConfig) ReconstituteIntervalMS(barMS int64) int64 {
	if c.ReconstituteBars > 0 {
		return int64(c.ReconstituteBars) * barMS
	}
	if c.ReconstituteDays > 0 {
		return int64(c.ReconstituteDays) * rollingDayMillis
	}
	return int64(DefaultRollingReconstituteDays) * rollingDayMillis
}

func (c RollingUniverseConfig) RequiredWarmupBars(barMS int64) int {
	if barMS <= 0 {
		return 0
	}
	span := max(c.LookbackDurationMS(barMS), c.MinAgeDurationMS(barMS))
	if span <= 0 {
		return 0
	}
	bars := int((span + barMS - 1) / barMS)
	return min(bars+1, maxRollingBound*24)
}

func symbolBaseUpper(symbol string) string {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	if idx := strings.IndexAny(s, "/:"); idx > 0 {
		return s[:idx]
	}
	return s
}

func ResolveAlwaysIncludeSIDs(names []string, candidates []int32, sidMap map[int32]string) ([]int32, error) {
	if len(names) == 0 {
		return nil, nil
	}
	candSet := make(map[int32]bool, len(candidates))
	for _, sid := range candidates {
		candSet[sid] = true
	}
	var out []int32
	for _, raw := range names {
		want := strings.ToUpper(strings.TrimSpace(raw))
		if want == "" {
			return nil, errors.New("factor: empty always_include symbol")
		}
		wantBase := symbolBaseUpper(want)
		matched := int32(0)
		for _, sid := range sortedSIDs(candidates) {
			sym := strings.ToUpper(strings.TrimSpace(sidMap[sid]))
			if sym == "" || !candSet[sid] {
				continue
			}
			if sym == want || (!strings.ContainsAny(want, "/:") && symbolBaseUpper(sym) == wantBase) {
				matched = sid
				break
			}
		}
		if matched == 0 {
			return nil, fmt.Errorf("factor: rolling universe always_include symbol %q missing from candidate universe", raw)
		}
		out = append(out, matched)
	}
	return sortedSIDs(out), nil
}

type rollingVolumeSample struct {
	eventTime int64
	turnover  float64
}

// RollingUniverseSelector maintains strictly causal point-in-time listing age and
// trailing quote turnover across candidate symbols and reconstitutes Universe.Investable
// on schedule without looking past the current decision grid.
type RollingUniverseSelector struct {
	cfg           RollingUniverseConfig
	baseVersion   string
	candidates    []int32
	sidMap        map[int32]string
	alwaysInclude []int32
	barMS         int64
	lookbackMS    int64
	minAgeMS      int64
	volumeSource  string
	volumeTF      string

	firstStart map[int32]int64
	samples    map[int32][]rollingVolumeSample
	lastBarAt  map[int32]int64

	anchorGrid  int64
	hasAnchor   bool
	lastRound   int64
	hasRound    bool
	current     []int32
	lastSeenGrid int64
}

func NewRollingUniverseSelector(cfg RollingUniverseConfig, base Universe, sidMap map[int32]string, barMS int64, volumeSource, volumeTF string) (*RollingUniverseSelector, error) {
	norm, err := NormalizeRollingUniverseConfig(cfg)
	if err != nil {
		return nil, err
	}
	if !norm.Enabled() {
		return nil, errors.New("factor: rolling universe selector requires an enabled config")
	}
	if barMS <= 0 {
		return nil, errors.New("factor: rolling universe selector requires positive bar interval")
	}
	all := append(append(slices.Clone(base.Investable), base.Reference...), base.Tracked...)
	candidates := sortedSIDs(all)
	if len(candidates) == 0 {
		return nil, errors.New("factor: rolling universe has no candidate symbols")
	}
	for _, sid := range candidates {
		if sid <= 0 || sidMap[sid] == "" {
			return nil, fmt.Errorf("factor: candidate SID %d missing symbol mapping", sid)
		}
	}
	always, err := ResolveAlwaysIncludeSIDs(norm.AlwaysInclude, candidates, sidMap)
	if err != nil {
		return nil, err
	}
	baseVer := base.Version
	if baseVer == "" {
		baseVer = "rolling"
	}
	return &RollingUniverseSelector{
		cfg:           norm,
		baseVersion:   baseVer,
		candidates:    candidates,
		sidMap:        maps.Clone(sidMap),
		alwaysInclude: always,
		barMS:         barMS,
		lookbackMS:    norm.LookbackDurationMS(barMS),
		minAgeMS:      norm.MinAgeDurationMS(barMS),
		volumeSource:  volumeSource,
		volumeTF:      volumeTF,
		firstStart:    make(map[int32]int64, len(candidates)),
		samples:       make(map[int32][]rollingVolumeSample, len(candidates)),
		lastBarAt:     make(map[int32]int64, len(candidates)),
		lastSeenGrid:  math.MinInt64,
	}, nil
}

func (r *RollingUniverseSelector) Config() RollingUniverseConfig {
	return *CloneRollingUniverseConfig(&r.cfg)
}

func (r *RollingUniverseSelector) Candidates() []int32 {
	return slices.Clone(r.candidates)
}

func recordQuoteTurnover(values map[string]any) (turnover float64, validBar bool) {
	q := Number(values, "quote")
	c := Number(values, "close")
	v := Number(values, "volume")
	if c.Validity == Valid && c.Value > 0 {
		validBar = true
	}
	if q.Validity == Valid && q.Value > 0 {
		return q.Value, true
	}
	if c.Validity == Valid && c.Value > 0 && v.Validity == Valid && v.Value > 0 {
		return c.Value * v.Value, true
	}
	return 0, validBar
}

// Observe ingests records strictly visible at (gridMS, decisionMS, replayMS).
// Records after gridMS or published/received after decisionMS/replayMS are ignored.
func (r *RollingUniverseSelector) Observe(records []VersionRecord, gridMS, decisionMS, replayMS int64) {
	candSet := make(map[int32]bool, len(r.candidates))
	for _, sid := range r.candidates {
		candSet[sid] = true
	}
	for _, row := range records {
		sid := row.Series.Sid
		if !candSet[sid] || !row.Series.Closed {
			continue
		}
		if row.EventTime <= 0 || row.EventTime > gridMS || row.AvailableAt > decisionMS || (replayMS != 0 && row.IngestedAt > replayMS) {
			continue
		}
		if r.volumeSource != "" && row.Series.Source != r.volumeSource {
			continue
		}
		if r.volumeTF != "" && row.Series.TimeFrame != r.volumeTF {
			continue
		}
		turnover, validBar := recordQuoteTurnover(row.Series.Values)
		if !validBar && turnover <= 0 {
			continue
		}
		startMS := row.EventTime - r.barMS
		if row.Series.TimeMS > 0 && row.Series.TimeMS < row.EventTime {
			startMS = row.Series.TimeMS
		}
		if prev, seen := r.firstStart[sid]; !seen || startMS < prev {
			r.firstStart[sid] = startMS
		}
		if row.EventTime > r.lastBarAt[sid] {
			r.lastBarAt[sid] = row.EventTime
		}
		list := r.samples[sid]
		n := len(list)
		if n > 0 && list[n-1].eventTime == row.EventTime {
			list[n-1].turnover = turnover
		} else if n == 0 || row.EventTime > list[n-1].eventTime {
			list = append(list, rollingVolumeSample{eventTime: row.EventTime, turnover: turnover})
		} else {
			idx := sort.Search(len(list), func(i int) bool { return list[i].eventTime >= row.EventTime })
			if idx < len(list) && list[idx].eventTime == row.EventTime {
				list[idx].turnover = turnover
			} else {
				list = slices.Insert(list, idx, rollingVolumeSample{eventTime: row.EventTime, turnover: turnover})
			}
		}
		r.samples[sid] = list
	}
	r.pruneBefore(gridMS - r.lookbackMS)
}

func (r *RollingUniverseSelector) pruneBefore(cutoffMS int64) {
	for sid, list := range r.samples {
		drop := 0
		for drop < len(list) && list[drop].eventTime <= cutoffMS {
			drop++
		}
		if drop > 0 {
			kept := slices.Clone(list[drop:])
			r.samples[sid] = kept
		}
	}
}

func (r *RollingUniverseSelector) scheduleRound(gridMS int64) int64 {
	switch r.cfg.Schedule {
	case "daily":
		t := time.UnixMilli(gridMS).UTC()
		return int64(t.Year()*10000 + int(t.Month())*100 + t.Day())
	case "weekly":
		t := time.UnixMilli(gridMS).UTC()
		y, w := t.ISOWeek()
		return int64(y*100 + w)
	case "monthly":
		t := time.UnixMilli(gridMS).UTC()
		return int64(t.Year()*12 + int(t.Month()))
	default:
		interval := r.cfg.ReconstituteIntervalMS(r.barMS)
		if interval <= 0 {
			interval = r.barMS
		}
		delta := gridMS - r.anchorGrid
		round := delta / interval
		if delta < 0 && delta%interval != 0 {
			round--
		}
		return round
	}
}

// Select returns the point-in-time Universe at gridMS and reports whether
// Investable was reconstituted on this step.
// During pre-start warmup (warming == true), all currently alive candidates stay
// active so time-series indicators warm up without prematurely freezing Top-N.
func (r *RollingUniverseSelector) Select(gridMS int64, alive map[int32]bool, warming bool) (Universe, bool) {
	aliveList := make([]int32, 0, len(r.candidates))
	for _, sid := range r.candidates {
		if alive[sid] {
			aliveList = append(aliveList, sid)
		}
	}
	if warming {
		return r.makeUniverse(aliveList, aliveList), false
	}
	if !r.hasAnchor {
		r.anchorGrid = gridMS
		r.hasAnchor = true
	}
	round := r.scheduleRound(gridMS)
	due := !r.hasRound || round > r.lastRound || len(r.current) == 0
	reconstituted := false
	if due {
		selected := r.rankTopN(gridMS, alive)
		if len(selected) > 0 || !r.hasRound {
			r.current = selected
			if len(selected) > 0 {
				r.lastRound = round
				r.hasRound = true
				reconstituted = true
			}
		}
	}
	r.lastSeenGrid = gridMS
	return r.makeUniverse(r.current, aliveList), reconstituted
}

func (r *RollingUniverseSelector) rankTopN(gridMS int64, alive map[int32]bool) []int32 {
	type scored struct {
		sid    int32
		symbol string
		sum    float64
	}
	cutoff := gridMS - r.lookbackMS
	var ranked []scored
	for _, sid := range r.candidates {
		if !alive[sid] {
			continue
		}
		first, ok := r.firstStart[sid]
		if !ok || gridMS-first < r.minAgeMS {
			continue
		}
		// Require a recent valid volume bar at or within one bar of gridMS so halted/delisted symbols do not rank.
		lastAt, hasBar := r.lastBarAt[sid]
		if !hasBar || gridMS-lastAt > r.barMS {
			continue
		}
		sum := 0.0
		for _, sample := range r.samples[sid] {
			if sample.eventTime > cutoff && sample.eventTime <= gridMS {
				sum += sample.turnover
			}
		}
		if sum <= 0 || math.IsNaN(sum) || math.IsInf(sum, 0) {
			continue
		}
		ranked = append(ranked, scored{sid: sid, symbol: r.sidMap[sid], sum: sum})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].sum != ranked[j].sum {
			return ranked[i].sum > ranked[j].sum
		}
		if ranked[i].symbol != ranked[j].symbol {
			return ranked[i].symbol < ranked[j].symbol
		}
		return ranked[i].sid < ranked[j].sid
	})
	limit := min(r.cfg.TopN, len(ranked))
	selected := make([]int32, 0, limit+len(r.alwaysInclude))
	for i := 0; i < limit; i++ {
		selected = append(selected, ranked[i].sid)
	}
	for _, sid := range r.alwaysInclude {
		if alive[sid] {
			selected = append(selected, sid)
		}
	}
	return sortedSIDs(selected)
}

func (r *RollingUniverseSelector) makeUniverse(investable, tracked []int32) Universe {
	inv := sortedSIDs(investable)
	trk := sortedSIDs(tracked)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%v|%v", inv, trk)))
	return Universe{
		Version:    fmt.Sprintf("%s#roll-%x", r.baseVersion, sum[:6]),
		Investable: inv,
		Reference:  slices.Clone(inv),
		Tradable:   slices.Clone(inv),
		Evaluation: slices.Clone(inv),
		Tracked:    trk,
		Static:     false,
	}
}
