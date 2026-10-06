package factor

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

type RebalanceConfig struct {
	EveryBars       int    `json:"every_bars,omitempty" yaml:"every_bars,omitempty"`
	Anchor          int64  `json:"anchor,omitempty" yaml:"anchor,omitempty"`
	Phase           int    `json:"phase,omitempty" yaml:"phase,omitempty"`
	Duration        string `json:"duration,omitempty" yaml:"duration,omitempty"`
	Calendar        string `json:"calendar,omitempty" yaml:"calendar,omitempty"`
	CalendarVersion string `json:"calendar_version,omitempty" yaml:"calendar_version,omitempty"`
	Timezone        string `json:"timezone,omitempty" yaml:"timezone,omitempty"`
}
type SelectionConfig struct {
	LongK         int            `json:"long_k,omitempty" yaml:"long_k,omitempty"`
	ShortK        int            `json:"short_k,omitempty" yaml:"short_k,omitempty"`
	LongQuantile  float64        `json:"long_quantile,omitempty" yaml:"long_quantile,omitempty"`
	ShortQuantile float64        `json:"short_quantile,omitempty" yaml:"short_quantile,omitempty"`
	RetainRank    int            `json:"retain_rank,omitempty" yaml:"retain_rank,omitempty"`
	Dropout       int            `json:"dropout,omitempty" yaml:"dropout,omitempty"`
	GroupQuota    map[string]int `json:"group_quota,omitempty" yaml:"group_quota,omitempty"`
	MissingScores string         `json:"missing_scores,omitempty" yaml:"missing_scores,omitempty"`
}
type HoldingRule struct {
	MinBars     int    `json:"min_bars,omitempty" yaml:"min_bars,omitempty"`
	MaxBars     int    `json:"max_bars,omitempty" yaml:"max_bars,omitempty"`
	MinDuration string `json:"min_duration,omitempty" yaml:"min_duration,omitempty"`
	MaxDuration string `json:"max_duration,omitempty" yaml:"max_duration,omitempty"`
}

// Pointer overrides distinguish an omitted value from an explicit zero.
type HoldingOverride struct {
	MinBars     *int    `json:"min_bars,omitempty" yaml:"min_bars,omitempty"`
	MaxBars     *int    `json:"max_bars,omitempty" yaml:"max_bars,omitempty"`
	MinDuration *string `json:"min_duration,omitempty" yaml:"min_duration,omitempty"`
	MaxDuration *string `json:"max_duration,omitempty" yaml:"max_duration,omitempty"`
}
type HoldingConfig struct {
	MinBars      int                        `json:"min_bars,omitempty" yaml:"min_bars,omitempty"`
	MaxBars      int                        `json:"max_bars,omitempty" yaml:"max_bars,omitempty"`
	MinDuration  string                     `json:"min_duration,omitempty" yaml:"min_duration,omitempty"`
	MaxDuration  string                     `json:"max_duration,omitempty" yaml:"max_duration,omitempty"`
	ByAsset      map[string]HoldingOverride `json:"by_asset,omitempty" yaml:"by_asset,omitempty"`
	Adopt        string                     `json:"adopt,omitempty" yaml:"adopt,omitempty"`
	CooldownBars int                        `json:"cooldown_bars,omitempty" yaml:"cooldown_bars,omitempty"`
}
type TransitionRule struct {
	ExitSteps int     `json:"exit_steps,omitempty" yaml:"exit_steps,omitempty"`
	Ratio     float64 `json:"ratio,omitempty" yaml:"ratio,omitempty"`
}
type TransitionConfig struct {
	Mode            string                    `json:"mode,omitempty" yaml:"mode,omitempty"`
	ExitSteps       int                       `json:"exit_steps,omitempty" yaml:"exit_steps,omitempty"`
	Basis           string                    `json:"basis,omitempty" yaml:"basis,omitempty"`
	PeriodBars      int                       `json:"period_bars,omitempty" yaml:"period_bars,omitempty"`
	Startup         string                    `json:"startup,omitempty" yaml:"startup,omitempty"`
	Sizing          string                    `json:"sizing,omitempty" yaml:"sizing,omitempty"`
	OnReselect      string                    `json:"on_reselect,omitempty" yaml:"on_reselect,omitempty"`
	Ratio           float64                   `json:"ratio,omitempty" yaml:"ratio,omitempty"`
	FinalThreshold  float64                   `json:"final_threshold,omitempty" yaml:"final_threshold,omitempty"`
	Alpha           float64                   `json:"alpha,omitempty" yaml:"alpha,omitempty"`
	EntryWindowBars int                       `json:"entry_window_bars,omitempty" yaml:"entry_window_bars,omitempty"`
	ByAsset         map[string]TransitionRule `json:"by_asset,omitempty" yaml:"by_asset,omitempty"`
}
type AllocationConfig struct {
	Method        string             `json:"method,omitempty" yaml:"method,omitempty"`
	ReserveRatio  float64            `json:"reserve_ratio,omitempty" yaml:"reserve_ratio,omitempty"`
	FixedNotional float64            `json:"fixed_notional,omitempty" yaml:"fixed_notional,omitempty"`
	VolTarget     float64            `json:"vol_target,omitempty" yaml:"vol_target,omitempty"`
	AssetCap      float64            `json:"asset_cap,omitempty" yaml:"asset_cap,omitempty"`
	GroupCaps     map[string]float64 `json:"group_caps,omitempty" yaml:"group_caps,omitempty"`
	TurnoverLimit float64            `json:"turnover_limit,omitempty" yaml:"turnover_limit,omitempty"`
	NetCap        float64            `json:"net_cap,omitempty" yaml:"net_cap,omitempty"`
	BetaCap       float64            `json:"beta_cap,omitempty" yaml:"beta_cap,omitempty"`
}
type PortfolioPolicyConfig struct {
	Policy        string           `json:"policy"`
	PolicyParams  json.RawMessage  `json:"policy_params,omitempty"`
	Rebalance     RebalanceConfig  `json:"rebalance"`
	Selection     SelectionConfig  `json:"selection"`
	Holding       HoldingConfig    `json:"holding"`
	Transition    TransitionConfig `json:"transition"`
	Allocation    AllocationConfig `json:"allocation"`
	LongNotional  float64          `json:"long_notional"`
	ShortNotional float64          `json:"short_notional"`
}

func ClonePortfolioPolicyConfig(c PortfolioPolicyConfig) PortfolioPolicyConfig {
	c.PolicyParams = append(json.RawMessage(nil), c.PolicyParams...)
	c.Selection.GroupQuota = maps.Clone(c.Selection.GroupQuota)
	c.Holding.ByAsset = maps.Clone(c.Holding.ByAsset)
	for name, o := range c.Holding.ByAsset {
		if o.MinBars != nil {
			v := *o.MinBars
			o.MinBars = &v
		}
		if o.MaxBars != nil {
			v := *o.MaxBars
			o.MaxBars = &v
		}
		if o.MinDuration != nil {
			v := *o.MinDuration
			o.MinDuration = &v
		}
		if o.MaxDuration != nil {
			v := *o.MaxDuration
			o.MaxDuration = &v
		}
		c.Holding.ByAsset[name] = o
	}
	c.Transition.ByAsset = maps.Clone(c.Transition.ByAsset)
	c.Allocation.GroupCaps = maps.Clone(c.Allocation.GroupCaps)
	return c
}
func positiveDuration(v string) error {
	if v == "" {
		return nil
	}
	d, e := time.ParseDuration(v)
	if e != nil || d <= 0 {
		return errors.New("factor: duration must be positive")
	}
	return nil
}
func validateHolding(r HoldingRule) error {
	if r.MinBars < 0 || r.MaxBars < 0 || r.MaxBars > 0 && r.MinBars > r.MaxBars {
		return errors.New("factor: invalid holding bars")
	}
	if r.MinBars > 0 && r.MinDuration != "" || r.MaxBars > 0 && r.MaxDuration != "" {
		return errors.New("factor: holding bars and duration conflict")
	}
	if e := positiveDuration(r.MinDuration); e != nil {
		return e
	}
	if e := positiveDuration(r.MaxDuration); e != nil {
		return e
	}
	if r.MinDuration != "" && r.MaxDuration != "" {
		a, _ := time.ParseDuration(r.MinDuration)
		b, _ := time.ParseDuration(r.MaxDuration)
		if a > b {
			return errors.New("factor: minimum holding exceeds maximum")
		}
	}
	return nil
}
func NormalizePortfolioPolicyConfig(c PortfolioPolicyConfig) (PortfolioPolicyConfig, error) {
	c = ClonePortfolioPolicyConfig(c)
	if c.Policy == "" {
		c.Policy = "lifecycle-v1"
	}
	if c.Policy != "lifecycle-v1" {
		if len(c.PolicyParams) > 0 && !json.Valid(c.PolicyParams) {
			return c, errors.New("factor: invalid custom policy parameters")
		}
		return c, nil
	}
	if c.LongNotional < 0 || c.ShortNotional < 0 || c.LongNotional+c.ShortNotional <= 0 {
		return c, errors.New("factor: policy requires positive side budget")
	}
	r := &c.Rebalance
	if r.EveryBars < 0 || r.Phase < 0 {
		return c, errors.New("factor: negative rebalance interval/phase")
	}
	count := 0
	for _, v := range []bool{r.EveryBars > 0, r.Duration != "", r.Calendar != ""} {
		if v {
			count++
		}
	}
	if count > 1 {
		return c, errors.New("factor: rebalance bars, duration and calendar conflict")
	}
	if count == 0 {
		r.EveryBars = 1
	}
	if r.EveryBars > 0 && r.Phase >= r.EveryBars {
		return c, errors.New("factor: rebalance phase outside interval")
	}
	if err := positiveDuration(r.Duration); err != nil {
		return c, err
	}
	if r.Calendar != "" {
		if r.Anchor != 0 || r.Phase != 0 {
			return c, errors.New("factor: civil calendar does not use bar anchor/phase; use custom schedule")
		}
		if r.Calendar != "daily" && r.Calendar != "weekly" && r.Calendar != "monthly" {
			return c, errors.New("factor: unknown calendar schedule")
		}
		if r.CalendarVersion == "" {
			return c, errors.New("factor: calendar version required")
		}
		if r.Timezone == "" {
			r.Timezone = "UTC"
		}
		if _, err := time.LoadLocation(r.Timezone); err != nil {
			return c, err
		}
	}
	s := &c.Selection
	if s.LongK < 0 || s.ShortK < 0 || s.RetainRank < 0 || s.Dropout < 0 || s.LongQuantile < 0 || s.LongQuantile > 1 || s.ShortQuantile < 0 || s.ShortQuantile > 1 || s.LongQuantile > 0 && s.LongK > 0 || s.ShortQuantile > 0 && s.ShortK > 0 {
		return c, errors.New("factor: invalid selector")
	}
	for _, q := range s.GroupQuota {
		if q < 0 {
			return c, errors.New("factor: negative group quota")
		}
	}
	if s.MissingScores == "" {
		s.MissingScores = "skip"
	}
	if s.MissingScores != "skip" && s.MissingScores != "shrink" && s.MissingScores != "cash" {
		return c, errors.New("factor: unknown missing score behavior")
	}
	h := &c.Holding
	if err := validateHolding(HoldingRule{h.MinBars, h.MaxBars, h.MinDuration, h.MaxDuration}); err != nil {
		return c, err
	}
	if h.CooldownBars < 0 || h.Adopt != "" && h.Adopt != "adopt" && h.Adopt != "reject" {
		return c, errors.New("factor: invalid adoption/cooldown")
	}
	for name, override := range h.ByAsset {
		if name == "" {
			return c, errors.New("factor: empty asset override")
		}
		rule := HoldingRule{h.MinBars, h.MaxBars, h.MinDuration, h.MaxDuration}
		if override.MinBars != nil {
			rule.MinBars = *override.MinBars
			rule.MinDuration = ""
		}
		if override.MaxBars != nil {
			rule.MaxBars = *override.MaxBars
			rule.MaxDuration = ""
		}
		if override.MinDuration != nil {
			rule.MinDuration = *override.MinDuration
			rule.MinBars = 0
		}
		if override.MaxDuration != nil {
			rule.MaxDuration = *override.MaxDuration
			rule.MaxBars = 0
		}
		if override.MinBars != nil && override.MinDuration != nil || override.MaxBars != nil && override.MaxDuration != nil {
			return c, errors.New("factor: asset override bars and duration conflict")
		}
		if err := validateHolding(rule); err != nil {
			return c, err
		}
	}
	t := &c.Transition
	if t.Mode == "" {
		t.Mode = "direct"
	}
	if t.Mode != "linear-exit" && t.ExitSteps != 0 || t.Mode != "geometric" && (t.Ratio != 0 || t.FinalThreshold != 0) || t.Mode != "target-step" && t.Alpha != 0 || t.Mode != "cohort" && (t.PeriodBars != 0 || t.Startup != "" || t.Sizing != "" || t.EntryWindowBars != 0) {
		return c, errors.New("factor: transition field unused by selected mode")
	}
	if len(t.ByAsset) > 0 && t.Mode != "linear-exit" && t.Mode != "geometric" {
		return c, errors.New("factor: per-asset transition unused by selected mode")
	}
	switch t.Mode {
	case "direct", "cohort", "target-step":
		if t.Basis != "" {
			return c, errors.New("factor: transition basis unused in selected mode")
		}
	case "linear-exit", "geometric":
		if t.Basis != "quantity" && t.Basis != "weight" {
			return c, errors.New("factor: exit requires quantity or weight basis")
		}
	default:
		return c, fmt.Errorf("factor: unknown transition %s", t.Mode)
	}
	if t.Mode == "linear-exit" && t.ExitSteps <= 0 {
		return c, errors.New("factor: linear exit requires positive steps")
	}
	if t.Mode == "geometric" {
		if t.Ratio <= 0 || t.Ratio >= 1 || t.FinalThreshold <= 0 || t.FinalThreshold >= 1 {
			return c, errors.New("factor: geometric requires ratio and terminal threshold in (0,1)")
		}
	}
	if t.Mode == "target-step" && (t.Alpha <= 0 || t.Alpha > 1) {
		return c, errors.New("factor: target-step alpha must be in (0,1]")
	}
	if t.ExitSteps < 0 || t.PeriodBars < 0 || t.EntryWindowBars < 0 {
		return c, errors.New("factor: negative transition interval")
	}
	if t.Mode != "linear-exit" && t.Mode != "geometric" && t.OnReselect != "" {
		return c, errors.New("factor: reselection field unused by selected transition")
	}
	if t.OnReselect == "" && (t.Mode == "linear-exit" || t.Mode == "geometric") {
		t.OnReselect = "restore"
	}
	if t.OnReselect != "" && !strings.Contains("|restore|resume|finish|new-cohort|", "|"+t.OnReselect+"|") {
		return c, errors.New("factor: invalid reselection behavior")
	}
	if t.Mode == "cohort" {
		if r.EveryBars <= 0 || t.PeriodBars <= 0 || t.PeriodBars%r.EveryBars != 0 {
			return c, errors.New("factor: cohort period must be an integer multiple of every_bars")
		}
		if h.MinBars > t.PeriodBars || h.MinDuration != "" {
			return c, errors.New("factor: cohort/minimum holding conflict")
		}
		if t.Startup == "" {
			t.Startup = "gradual"
		}
		if t.Startup != "gradual" && t.Startup != "seed-all" {
			return c, errors.New("factor: invalid cohort startup")
		}
		if t.Sizing == "" {
			t.Sizing = "entry-nav"
		}
		if t.Sizing != "entry-nav" && t.Sizing != "current-nav" {
			return c, errors.New("factor: invalid cohort sizing")
		}
		if t.EntryWindowBars == 0 {
			t.EntryWindowBars = r.EveryBars
		}
	}
	for _, v := range t.ByAsset {
		if v.ExitSteps < 0 || v.Ratio < 0 || v.Ratio >= 1 || t.Mode != "linear-exit" && v.ExitSteps != 0 || t.Mode != "geometric" && v.Ratio != 0 {
			return c, errors.New("factor: invalid asset exit steps")
		}
	}
	a := &c.Allocation
	if a.Method == "" {
		a.Method = "equal"
	}
	if a.Method != "fixed-notional" && a.FixedNotional != 0 || a.Method != "vol-target" && a.VolTarget != 0 {
		return c, errors.New("factor: allocation sizing field unused by selected method")
	}
	switch a.Method {
	case "equal", "score", "inverse-volatility":
	case "fixed-notional":
		if a.FixedNotional <= 0 {
			return c, errors.New("factor: fixed notional must be positive")
		}
	case "vol-target":
		if a.VolTarget <= 0 {
			return c, errors.New("factor: vol target must be positive")
		}
	default:
		return c, errors.New("factor: unknown allocation method")
	}
	if a.ReserveRatio < 0 || a.ReserveRatio >= 1 || a.AssetCap < 0 || a.TurnoverLimit < 0 || a.NetCap < 0 || a.BetaCap < 0 {
		return c, errors.New("factor: invalid allocation constraints")
	}
	for _, v := range a.GroupCaps {
		if v < 0 {
			return c, errors.New("factor: negative group cap")
		}
	}
	// Marshal rejects every NaN/Inf, including deeply nested custom values.
	if _, err := json.Marshal(c); err != nil {
		return c, err
	}
	return c, nil
}
