package factor

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"
)

type PositionEvidence struct {
	Quantity          string                 `json:"quantity"`
	PendingQuantity   string                 `json:"pending_quantity,omitempty"`
	FirstFillTime     int64                  `json:"first_fill_time,omitempty"`
	IncreasingPending bool                   `json:"increasing_pending,omitempty"`
	PendingUnknown    bool                   `json:"pending_unknown,omitempty"`
	Quantum           string                 `json:"quantum,omitempty"`
	FillEvents        []PositionFillEvidence `json:"fill_events,omitempty"`
}
type PositionFillEvidence struct {
	Quantity     string `json:"quantity"`
	LedgerCursor uint64 `json:"ledger_cursor"`
	PlanSequence uint64 `json:"plan_sequence"`
	AtMS         int64  `json:"at_ms"`
}
type PortfolioEvidence struct {
	Positions    map[int32]PositionEvidence
	Marks        map[int32]string
	StateVersion uint64
	LedgerCursor uint64
	State        json.RawMessage
	PlanSequence uint64
	Previous     *PortfolioTarget
}
type PortfolioContext struct {
	Frame                Frame
	Universe             Universe
	Ideal                *TargetPortfolio
	Spec                 PortfolioSpec
	GridTime             int64
	BarMillis            int64
	ScoreName            string
	Positions            map[int32]PositionEvidence
	Marks                map[int32]float64
	Groups               map[int32]string
	Volatility           map[int32]float64
	Beta                 map[int32]float64
	AssetNames           map[int32]string
	SIDMappingVersion    string
	StateVersion         uint64
	LedgerCursor         uint64
	ForceExit            map[int32]string
	Previous             *PortfolioTarget
	PreserveIdealWeights bool
	// Visible rule products are below explicit by_asset overrides. Callers
	// must freeze only PIT-visible versions in this read-only context.
	HoldingRules        map[int32]HoldingRule
	TransitionRules     map[int32]TransitionRule
	RebalanceDue        *bool
	CapitalLimit        float64
	RiskOnly            bool
	previousAllocations map[int32]Allocation
}
type PortfolioProposal struct {
	Target        *PortfolioTarget
	NextState     json.RawMessage
	Reasons       []Diagnostic
	AcceptanceID  string
	ReconcileSIDs []int32
	PlanSequence  uint64
	DecisionTime  int64
	ExpireAt      int64
}
type PortfolioPolicy interface {
	Propose(PortfolioContext, json.RawMessage) (PortfolioProposal, error)
}
type PortfolioPolicyFactory func(PortfolioPolicyConfig) (PortfolioPolicy, error)

func ClonePositionEvidence(positions map[int32]PositionEvidence) map[int32]PositionEvidence {
	owned := maps.Clone(positions)
	for sid, p := range owned {
		p.FillEvents = slices.Clone(p.FillEvents)
		owned[sid] = p
	}
	return owned
}
func ClonePortfolioEvidence(e PortfolioEvidence) PortfolioEvidence {
	e.Positions = ClonePositionEvidence(e.Positions)
	e.Marks = maps.Clone(e.Marks)
	e.State = append(json.RawMessage(nil), e.State...)
	return e
}
func ClonePortfolioContext(c PortfolioContext) PortfolioContext {
	c.Frame = CloneFrame(c.Frame)
	c.Universe = CloneUniverse(c.Universe)
	c.Positions = ClonePositionEvidence(c.Positions)
	c.Marks = maps.Clone(c.Marks)
	c.Groups = maps.Clone(c.Groups)
	c.Volatility = maps.Clone(c.Volatility)
	c.Beta = maps.Clone(c.Beta)
	c.AssetNames = maps.Clone(c.AssetNames)
	c.ForceExit = maps.Clone(c.ForceExit)
	c.HoldingRules = maps.Clone(c.HoldingRules)
	c.TransitionRules = maps.Clone(c.TransitionRules)
	if c.RebalanceDue != nil {
		v := *c.RebalanceDue
		c.RebalanceDue = &v
	}
	return c
}

type LifecyclePolicy struct {
	config PortfolioPolicyConfig
	hash   string
}

func NewLifecyclePolicy(config PortfolioPolicyConfig) (PortfolioPolicy, error) {
	c, err := NormalizePortfolioPolicyConfig(config)
	if err != nil {
		return nil, err
	}
	if c.Policy != "lifecycle-v1" {
		return nil, errors.New("factor: lifecycle factory requires lifecycle-v1")
	}
	hash, err := contentHash(c)
	return &LifecyclePolicy{c, hash}, err
}

type AssetLifecycle struct {
	FirstFillTime  int64      `json:"first_fill_time,omitempty"`
	Direction      int        `json:"direction"`
	Exiting        bool       `json:"exiting,omitempty"`
	ExitStep       int        `json:"exit_step,omitempty"`
	AnchorQuantity string     `json:"anchor_quantity,omitempty"`
	AnchorWeight   float64    `json:"anchor_weight,omitempty"`
	Last           Allocation `json:"last"`
	CooldownUntil  int64      `json:"cooldown_until,omitempty"`
	Forced         bool       `json:"forced,omitempty"`
	Paused         bool       `json:"paused,omitempty"`
}
type CohortContribution struct {
	SID            int32             `json:"sid"`
	Planned        string            `json:"planned"`
	Filled         string            `json:"filled"`
	Weight         float64           `json:"weight"`
	Outstanding    bool              `json:"outstanding,omitempty"`
	EntrySequences []uint64          `json:"entry_sequences,omitempty"`
	EntryPlanned   map[uint64]string `json:"entry_planned,omitempty"`
}
type PortfolioCohort struct {
	ID            string               `json:"id"`
	Created       int64                `json:"created"`
	Expires       int64                `json:"expires"`
	EntryUntil    int64                `json:"entry_until"`
	Contributions []CohortContribution `json:"contributions"`
	PlanSequence  uint64               `json:"plan_sequence"`
}
type LifecycleState struct {
	Schema            int                       `json:"schema"`
	AccountID         string                    `json:"account_id"`
	StrategyID        string                    `json:"strategy_id"`
	Policy            string                    `json:"policy"`
	ConfigHash        string                    `json:"config_hash"`
	SIDMappingVersion string                    `json:"sid_mapping_version"`
	AssetIdentities   map[int32]string          `json:"asset_identities,omitempty"`
	LastGrid          int64                     `json:"last_grid"`
	LastRiskGrid      int64                     `json:"last_risk_grid,omitempty"`
	LastRound         int64                     `json:"last_round"`
	HasRound          bool                      `json:"has_round"`
	Sequence          uint64                    `json:"sequence"`
	LedgerCursor      uint64                    `json:"ledger_cursor"`
	Assets            map[int32]*AssetLifecycle `json:"assets"`
	Cohorts           []PortfolioCohort         `json:"cohorts,omitempty"`
}

const MaxPortfolioStateBytes = 1 << 20
const MaxPortfolioActiveAssets = 4096

func DecodeLifecycleState(raw json.RawMessage) (LifecycleState, error) {
	var s LifecycleState
	if len(raw) > MaxPortfolioStateBytes {
		return s, errors.New("factor: portfolio checkpoint exceeds size limit")
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &s); err != nil {
			return s, err
		}
		if s.Schema != 1 {
			return s, errors.New("factor: unsupported portfolio state schema")
		}
	}
	if s.Assets == nil {
		s.Assets = map[int32]*AssetLifecycle{}
	}
	if s.AssetIdentities == nil {
		s.AssetIdentities = map[int32]string{}
	}
	if len(s.Assets) > MaxPortfolioActiveAssets {
		return s, errors.New("factor: too many active lifecycle assets")
	}
	return s, nil
}
func (p *LifecyclePolicy) Config() PortfolioPolicyConfig { return ClonePortfolioPolicyConfig(p.config) }
func scheduleRound(c RebalanceConfig, grid, bar int64) (int64, error) {
	if bar <= 0 {
		return 0, errors.New("factor: positive policy base bar required")
	}
	if c.Calendar != "" {
		loc, err := time.LoadLocation(c.Timezone)
		if err != nil {
			return 0, err
		}
		t := time.UnixMilli(grid).In(loc)
		switch c.Calendar {
		case "daily":
			return int64(t.Year()*10000 + int(t.Month())*100 + t.Day()), nil
		case "weekly":
			y, w := t.ISOWeek()
			return int64(y*100 + w), nil
		case "monthly":
			return int64(t.Year()*12 + int(t.Month())), nil
		}
	}
	interval := int64(c.EveryBars) * bar
	if c.Duration != "" {
		d, e := time.ParseDuration(c.Duration)
		if e != nil {
			return 0, e
		}
		interval = d.Milliseconds()
	}
	if interval <= 0 {
		return 0, errors.New("factor: schedule below millisecond resolution")
	}
	value := grid - c.Anchor - int64(c.Phase)*bar
	round := value / interval
	if value < 0 && value%interval != 0 {
		round--
	}
	return round, nil
}

// RebalanceDue uses an absolute anchor; archive chunk starts never enter it.
func RebalanceDue(c RebalanceConfig, grid, bar, lastRound int64, hasRound bool) (bool, int64, error) {
	round, e := scheduleRound(c, grid, bar)
	return !hasRound || round > lastRound, round, e
}
func signed(value float64) int {
	if value > 0 {
		return 1
	}
	if value < 0 {
		return -1
	}
	return 0
}
func positionQuantity(e PositionEvidence) (float64, error) {
	if e.Quantity == "" {
		return 0, nil
	}
	return decimalFloat(e.Quantity)
}
func pendingQuantity(e PositionEvidence) (float64, error) {
	if e.PendingQuantity == "" {
		return 0, nil
	}
	return decimalFloat(e.PendingQuantity)
}
func allocationWeight(a Allocation, c PortfolioContext, sid int32) (float64, error) {
	v, err := decimalFloat(a.Value)
	if err != nil {
		return 0, err
	}
	if a.Basis == NAVFraction {
		return v, nil
	}
	mark := c.Marks[sid]
	if mark <= 0 && v != 0 {
		return 0, fmt.Errorf("factor: missing quantity valuation for SID %d", sid)
	}
	return v * mark / c.Spec.Budget.NAV, nil
}
func (p *LifecyclePolicy) holding(c PortfolioContext, sid int32) HoldingRule {
	h := p.config.Holding
	r := HoldingRule{h.MinBars, h.MaxBars, h.MinDuration, h.MaxDuration}
	if rule, ok := c.HoldingRules[sid]; ok {
		r = rule
	}
	if o, ok := h.ByAsset[c.AssetNames[sid]]; ok {
		if o.MinBars != nil {
			r.MinBars = *o.MinBars
			r.MinDuration = ""
		}
		if o.MaxBars != nil {
			r.MaxBars = *o.MaxBars
			r.MaxDuration = ""
		}
		if o.MinDuration != nil {
			r.MinDuration = *o.MinDuration
			r.MinBars = 0
		}
		if o.MaxDuration != nil {
			r.MaxDuration = *o.MaxDuration
			r.MaxBars = 0
		}
	}
	return r
}
func holdingMillis(bars int, duration string, bar int64) int64 {
	if duration != "" {
		d, _ := time.ParseDuration(duration)
		return d.Milliseconds()
	}
	return int64(bars) * bar
}

func cohortOwnsUnsettledSID(c PortfolioContext, s LifecycleState, sid int32) bool {
	for _, cohort := range s.Cohorts {
		for _, contribution := range cohort.Contributions {
			if contribution.SID == sid && (cohort.Expires > c.GridTime || contribution.Outstanding || mustQuantity(contribution.Filled) != 0) {
				return true
			}
		}
	}
	return false
}

// A missing SID in complete position evidence is settled flat. Forget its
// completed lifecycle only after every live data scope and batch releases it.
func pruneReleasedLifecycle(c PortfolioContext, s *LifecycleState) []int32 {
	scoped := map[int32]bool{}
	for sid := range c.AssetNames {
		scoped[sid] = true
	}
	for _, pool := range [][]int32{c.Universe.Investable, c.Universe.Reference, c.Universe.Tradable, c.Universe.Evaluation, c.Universe.Tracked} {
		for _, sid := range pool {
			scoped[sid] = true
		}
	}
	var removed []int32
	for _, sid := range mapSIDs(s.Assets) {
		a := s.Assets[sid]
		if a.Last.Value != "0" || c.GridTime < a.CooldownUntil || scoped[sid] || cohortOwnsUnsettledSID(c, *s, sid) {
			continue
		}
		e := c.Positions[sid]
		q, err := positionQuantity(e)
		if err != nil {
			continue
		}
		pending, err := pendingQuantity(e)
		if err != nil || q != 0 || pending != 0 || e.PendingUnknown || e.IncreasingPending {
			continue
		}
		delete(s.Assets, sid)
		delete(s.AssetIdentities, sid)
		removed = append(removed, sid)
	}
	return removed
}
func (p *LifecyclePolicy) finish(c PortfolioContext, s LifecycleState, target map[int32]Allocation, reasons []Diagnostic, emit bool) (PortfolioProposal, error) {
	for sid := range s.AssetIdentities {
		if _, active := s.Assets[sid]; !active {
			delete(s.AssetIdentities, sid)
		}
	}
	for sid := range s.Assets {
		if name := c.AssetNames[sid]; name != "" {
			s.AssetIdentities[sid] = name
		}
	}
	if len(s.Assets) > MaxPortfolioActiveAssets {
		return PortfolioProposal{}, errors.New("factor: active policy state limit exceeded")
	}
	s.Sequence = c.Spec.PlanSequence
	s.LedgerCursor = c.LedgerCursor
	if c.RiskOnly {
		s.LastRiskGrid = c.GridTime
	} else {
		s.LastGrid = c.GridTime
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return PortfolioProposal{}, err
	}
	if len(raw) > MaxPortfolioStateBytes {
		return PortfolioProposal{}, errors.New("factor: policy checkpoint exceeds byte budget")
	}
	result := PortfolioProposal{NextState: raw, Reasons: slices.Clone(reasons), PlanSequence: c.Spec.PlanSequence, DecisionTime: c.Spec.DecisionTime, ExpireAt: c.Spec.ExpireAt}
	for _, reason := range reasons {
		if reason.Code == "exit-await-reconcile" {
			var sid int32
			if _, err := fmt.Sscanf(reason.Detail, "SID %d", &sid); err == nil {
				result.ReconcileSIDs = append(result.ReconcileSIDs, sid)
			}
		}
	}
	result.ReconcileSIDs = sortedSIDs(result.ReconcileSIDs)
	if emit {
		spec := c.Spec
		spec.Mode = Full
		if c.RiskOnly && p.config.Transition.Mode != "cohort" {
			spec.Mode = Patch
		}
		spec.Diagnostics = append(slices.Clone(spec.Diagnostics), reasons...)
		result.Target, err = NewPortfolioTarget(spec, target)
		if err != nil {
			return result, err
		}
	}
	result.AcceptanceID, err = contentHash(struct {
		Spec  PortfolioSpec
		State json.RawMessage
	}{c.Spec, raw})
	return result, err
}
func (p *LifecyclePolicy) Propose(c PortfolioContext, raw json.RawMessage) (PortfolioProposal, error) {
	if c.GridTime == 0 {
		c.GridTime = c.Frame.GridTime
	}
	if c.GridTime == 0 {
		c.GridTime = c.Spec.DecisionTime
	}
	if c.ScoreName == "" {
		c.ScoreName = "score"
	}
	if c.BarMillis <= 0 {
		return PortfolioProposal{}, errors.New("factor: positive base bar required")
	}
	for _, rule := range c.HoldingRules {
		if err := validateHolding(rule); err != nil {
			return PortfolioProposal{}, err
		}
	}
	for sid, rule := range c.TransitionRules {
		if rule.ExitSteps < 0 || rule.Ratio < 0 || rule.Ratio >= 1 {
			return PortfolioProposal{}, fmt.Errorf("factor: invalid visible transition for SID %d", sid)
		}
	}
	for sid := range c.AssetNames {
		rule := p.holding(c, sid)
		if err := validateHolding(rule); err != nil {
			return PortfolioProposal{}, err
		}
		minimum := holdingMillis(rule.MinBars, rule.MinDuration, c.BarMillis)
		maximum := holdingMillis(rule.MaxBars, rule.MaxDuration, c.BarMillis)
		if maximum > 0 && minimum > maximum {
			return PortfolioProposal{}, fmt.Errorf("factor: SID %d minimum holding exceeds maximum", sid)
		}
		if p.config.Transition.Mode == "cohort" && minimum > int64(p.config.Transition.PeriodBars)*c.BarMillis {
			return PortfolioProposal{}, fmt.Errorf("factor: SID %d minimum holding conflicts with cohort period", sid)
		}
	}
	s, err := DecodeLifecycleState(raw)
	if err != nil {
		return PortfolioProposal{}, err
	}
	c.previousAllocations = map[int32]Allocation{}
	for sid, a := range s.Assets {
		c.previousAllocations[sid] = a.Last
	}
	if s.Schema != 0 {
		if s.AccountID != c.Spec.AccountID || s.StrategyID != c.Spec.StrategyID || s.ConfigHash != p.hash || s.Policy != p.config.Policy || s.SIDMappingVersion != c.SIDMappingVersion {
			return PortfolioProposal{}, errors.New("factor: incompatible lifecycle checkpoint; explicit migration required")
		}
		if !c.RiskOnly && c.GridTime <= s.LastGrid || c.RiskOnly && c.GridTime <= s.LastRiskGrid && c.LedgerCursor <= s.LedgerCursor {
			id, err := contentHash(struct {
				Spec  PortfolioSpec
				State json.RawMessage
			}{c.Spec, raw})
			return PortfolioProposal{NextState: append(json.RawMessage(nil), raw...), Reasons: []Diagnostic{{"duplicate-grid", "accepted grid already processed"}}, AcceptanceID: id, PlanSequence: c.Spec.PlanSequence, DecisionTime: c.Spec.DecisionTime, ExpireAt: c.Spec.ExpireAt}, err
		}
		for sid, name := range s.AssetIdentities {
			if current := c.AssetNames[sid]; current != "" && name != "" && current != name {
				return PortfolioProposal{}, fmt.Errorf("factor: SID %d identity changed; migration required", sid)
			}
		}
	} else {
		s.Schema = 1
		s.AccountID = c.Spec.AccountID
		s.StrategyID = c.Spec.StrategyID
		s.Policy = p.config.Policy
		s.ConfigHash = p.hash
		s.SIDMappingVersion = c.SIDMappingVersion
	}
	for sid, name := range c.AssetNames {
		if _, active := s.Assets[sid]; active && name != "" {
			s.AssetIdentities[sid] = name
		}
	}
	pruneReleasedLifecycle(c, &s)
	due, round, err := RebalanceDue(p.config.Rebalance, c.GridTime, c.BarMillis, s.LastRound, s.HasRound)
	if err != nil {
		return PortfolioProposal{}, err
	}
	if c.RebalanceDue != nil {
		due = *c.RebalanceDue
	}
	if c.RiskOnly {
		due = false
	}
	if !c.RiskOnly && c.Ideal == nil && len(p.config.Selection.GroupQuota) > 0 {
		for _, point := range policyScores(c.Frame, c.Universe, c.ScoreName) {
			if _, ok := c.Groups[point.sid]; !ok {
				return PortfolioProposal{}, fmt.Errorf("factor: grouped selection requires visible group for SID %d", point.sid)
			}
		}
	}
	for _, sid := range mapSIDs(c.Positions) {
		e := c.Positions[sid]
		q, err := positionQuantity(e)
		if err != nil {
			return PortfolioProposal{}, err
		}
		pending, err := pendingQuantity(e)
		if err != nil {
			return PortfolioProposal{}, err
		}
		if q == 0 && pending == 0 && !e.PendingUnknown && !e.IncreasingPending {
			if a := s.Assets[sid]; a != nil && a.Last.Value == "0" && c.GridTime >= a.CooldownUntil && !cohortOwnsUnsettledSID(c, s, sid) {
				delete(s.Assets, sid)
			}
			continue
		}
		a := s.Assets[sid]
		if a == nil {
			a = &AssetLifecycle{Direction: signed(q), Last: Allocation{AbsoluteQuantity, floatDecimal(q)}}
			s.Assets[sid] = a
		}
		if q != 0 && a.FirstFillTime == 0 {
			a.FirstFillTime = e.FirstFillTime
			if a.FirstFillTime == 0 {
				if p.config.Holding.Adopt == "adopt" {
					a.FirstFillTime = c.GridTime
				} else if h := p.holding(c, sid); h.MinBars > 0 || h.MaxBars > 0 || h.MinDuration != "" || h.MaxDuration != "" {
					return PortfolioProposal{}, fmt.Errorf("factor: existing SID %d lacks verified fill age", sid)
				}
			}
		}
		if a.Direction == 0 {
			a.Direction = signed(q)
		}
	}
	if p.config.Transition.Mode == "cohort" {
		return p.proposeCohort(c, s, due, round)
	}
	var reasons []Diagnostic
	desired := map[int32]float64{}
	selectionValid := !c.RiskOnly
	if c.RiskOnly {
	} else if c.Ideal != nil {
		desired = c.Ideal.Targets()
	} else {
		ideal, diag, e := SelectPortfolioScore(c.Frame, c.Universe, c.Spec, p.config, c.ScoreName, c.Groups)
		if e != nil {
			return PortfolioProposal{}, e
		}
		reasons = append(reasons, diag...)
		if ideal == nil {
			selectionValid = false
		} else {
			desired = ideal.Targets()
		}
	}
	if selectionValid {
		scores := map[int32]float64{}
		for _, v := range policyScores(c.Frame, c.Universe, c.ScoreName) {
			scores[v.sid] = v.score
		}
		if !c.PreserveIdealWeights {
			desired, err = AllocateSelected(desired, p.config, c.Spec.Budget.NAV, scores, c.Volatility)
			if err != nil {
				return PortfolioProposal{}, err
			}
		}
		p.applyRetention(c, s, desired)
	}
	targets := map[int32]Allocation{}
	for sid, a := range s.Assets {
		targets[sid] = a.Last
	}
	hard := map[int32]bool{}
	protected := map[int32]bool{}
	investable := map[int32]bool{}
	for _, sid := range c.Universe.Investable {
		investable[sid] = true
	}
	for _, sid := range mapSIDs(s.Assets) {
		a := s.Assets[sid]
		e := c.Positions[sid]
		q, _ := positionQuantity(e)
		age := int64(0)
		if a.FirstFillTime > 0 {
			age = c.GridTime - a.FirstFillTime
		}
		h := p.holding(c, sid)
		maximum := holdingMillis(h.MaxBars, h.MaxDuration, c.BarMillis)
		forced := c.ForceExit[sid] != "" || !investable[sid] || maximum > 0 && a.FirstFillTime > 0 && age >= maximum
		if forced {
			if !a.Forced {
				a.CooldownUntil = c.GridTime + int64(p.config.Holding.CooldownBars+1)*c.BarMillis
			}
			a.Forced = true
			a.Exiting = true
			a.Last = Allocation{AbsoluteQuantity, "0"}
			targets[sid] = a.Last
			delete(desired, sid)
			hard[sid] = true
			reasons = append(reasons, Diagnostic{"forced-exit", fmt.Sprintf("SID %d: risk, universe removal or maximum age", sid)})
			continue
		}
		if a.Forced && (q != 0 || e.PendingUnknown) {
			targets[sid] = Allocation{AbsoluteQuantity, "0"}
			delete(desired, sid)
			hard[sid] = true
			continue
		}
		if (a.Exiting || a.Paused) && a.Last.Basis == AbsoluteQuantity {
			quantity := e.Quantity
			if quantity == "" {
				quantity = "0"
			}
			clamped, err := ClampQuantityMagnitude(a.Last.Value, quantity)
			if err != nil {
				return PortfolioProposal{}, err
			}
			if clamped != a.Last.Value {
				a.Last.Value = clamped
				targets[sid] = a.Last
				hard[sid] = true
				delete(desired, sid)
				reasons = append(reasons, Diagnostic{"exit-risk-clamp", fmt.Sprintf("SID %d confirmed risk reduction cannot be bought back", sid)})
				continue
			}
		}
		point, scorePresent := c.Frame.Values[c.ScoreName][sid]
		missingHeldScore := c.Ideal == nil && (!scorePresent || point.Validity != Valid || math.IsNaN(point.Value) || math.IsInf(point.Value, 0))
		if !due || !selectionValid || missingHeldScore {
			continue
		}
		want, selected := desired[sid]
		if selected && signed(want) != a.Direction && a.Direction != 0 {
			selected = false
			delete(desired, sid)
			reasons = append(reasons, Diagnostic{"reverse-await-flat", fmt.Sprintf("SID %d closes before reversing", sid)})
		}
		if selected && a.Exiting {
			switch p.config.Transition.OnReselect {
			case "restore":
				a.Exiting = false
				a.Paused = false
				a.ExitStep = 0
				a.AnchorQuantity = ""
			case "resume":
				a.Exiting = false
				a.Paused = true
				delete(desired, sid)
				targets[sid] = a.Last
				continue
			case "finish", "new-cohort":
				selected = false
				delete(desired, sid)
			}
		}
		if selected && a.Paused {
			delete(desired, sid)
			targets[sid] = a.Last
			continue
		}
		if !selected {
			a.Paused = false
		}
		if selected {
			continue
		}
		minimum := holdingMillis(h.MinBars, h.MinDuration, c.BarMillis)
		if !a.Exiting && (minimum > 0 && a.FirstFillTime == 0 || age < minimum) {
			protected[sid] = true
			delete(desired, sid)
			continue
		}
		if p.config.Transition.Mode == "direct" || p.config.Transition.Mode == "target-step" {
			if p.config.Transition.Mode == "direct" {
				a.Last = Allocation{AbsoluteQuantity, "0"}
				a.Exiting = true
				targets[sid] = a.Last
			}
			continue
		}
		if !a.Exiting {
			if p.config.Transition.Basis == "quantity" && (e.IncreasingPending || e.PendingUnknown) {
				reasons = append(reasons, Diagnostic{"exit-await-reconcile", fmt.Sprintf("SID %d increasing intent must be reconciled before exit anchor", sid)})
				continue
			}
			a.Exiting = true
			a.ExitStep = 0
			a.AnchorQuantity = e.Quantity
			if a.AnchorQuantity == "" {
				a.AnchorQuantity = "0"
			}
			a.AnchorWeight, _ = allocationWeight(a.Last, c, sid)
		}
		a.ExitStep++
		steps := p.config.Transition.ExitSteps
		if rule, ok := c.TransitionRules[sid]; ok && rule.ExitSteps > 0 {
			steps = rule.ExitSteps
		}
		if rule, ok := p.config.Transition.ByAsset[c.AssetNames[sid]]; ok && rule.ExitSteps > 0 {
			steps = rule.ExitSteps
		}
		ratio := 0.0
		if p.config.Transition.Mode == "linear-exit" {
			ratio = math.Max(0, 1-float64(a.ExitStep)/float64(steps))
		} else {
			r := p.config.Transition.Ratio
			if rule, ok := c.TransitionRules[sid]; ok && rule.Ratio > 0 {
				r = rule.Ratio
			}
			if rule, ok := p.config.Transition.ByAsset[c.AssetNames[sid]]; ok && rule.Ratio > 0 {
				r = rule.Ratio
			}
			ratio = math.Pow(r, float64(a.ExitStep))
			if ratio <= p.config.Transition.FinalThreshold {
				ratio = 0
			}
		}
		if p.config.Transition.Basis == "quantity" {
			quantity := "0"
			if ratio > 0 {
				if p.config.Transition.Mode == "linear-exit" {
					quantity, err = ScaleQuantity(a.AnchorQuantity, max(0, steps-a.ExitStep), steps, e.Quantum)
				} else {
					quantity, err = ScaleQuantity(floatDecimal(mustQuantity(a.AnchorQuantity)*ratio), 1, 1, e.Quantum)
				}
				if err != nil {
					return PortfolioProposal{}, err
				}
			}
			ceiling := e.Quantity
			if ceiling == "" {
				ceiling = "0"
			}
			ceilings := []string{ceiling}
			if a.Last.Basis == AbsoluteQuantity {
				ceilings = append(ceilings, a.Last.Value)
			}
			quantity, err = ClampQuantityMagnitude(quantity, ceilings...)
			if err != nil {
				return PortfolioProposal{}, err
			}
			a.Last = Allocation{AbsoluteQuantity, quantity}
		} else {
			a.Last = Allocation{NAVFraction, floatDecimal(a.AnchorWeight * ratio)}
		}
		targets[sid] = a.Last
	}
	emit := len(hard) > 0 || due && selectionValid
	if due && selectionValid {
		if p.config.Transition.Mode == "target-step" {
			for _, sid := range mapSIDs(s.Assets) {
				if protected[sid] {
					continue
				}
				a := s.Assets[sid]
				previous, e := allocationWeight(a.Last, c, sid)
				if e != nil {
					return PortfolioProposal{}, e
				}
				desired[sid] = previous + p.config.Transition.Alpha*(desired[sid]-previous)
			}
		}
		p.allocateRemainder(c, s, desired, targets, hard, &reasons)
		s.LastRound = round
		s.HasRound = true
	}
	if c.RiskOnly {
		riskTargets := map[int32]Allocation{}
		for sid := range hard {
			riskTargets[sid] = targets[sid]
		}
		targets = riskTargets
	}
	if emit {
		if err := p.constrain(c, s, targets, hard, &reasons); err != nil {
			return PortfolioProposal{}, err
		}
		for sid, a := range targets {
			life := s.Assets[sid]
			if life == nil {
				value := mustQuantity(a.Value)
				life = &AssetLifecycle{Direction: signed(value)}
				s.Assets[sid] = life
			}
			life.Last = a
		}
	}
	return p.finish(c, s, targets, reasons, emit)
}
func mustQuantity(value string) float64 {
	if value == "" {
		return 0
	}
	v, _ := decimalFloat(value)
	return v
}
