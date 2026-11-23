package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"

	"github.com/banbox/banexg/utils"
)

// AdmissionPolicy is the account-level time-series entry admission limit set,
// enforced by the account owner when a create command is accepted. It
// mirrors the robot-side max_open_orders / max_simul_open: the robot keeps
// its local check as an early filter, the owner is the authority across every
// robot sharing the account. Zero means unlimited.
//
//   - MaxOpenOrders bounds the account's open time-series lots (held, or with
//     an active unfilled entry), over all strategies and robots.
//   - MaxSimulOpen bounds the new entries accepted in one bar round. The owner
//     does not know bars: a round is the request's timeframe window aligned
//     from TimeSeriesMetadata.CreatedMS (the robot clock the robot aligns its
//     own round with). The newest round seen is kept; a request from an older
//     round counts in it, so a lagging robot can never reopen a spent round.
//     A request without a timeframe window ("ws") is not round-limited.
//
// Only lots and creates carrying TimeSeriesMetadata are counted; rebalance /
// factor lots are outside max_open_orders, as on the robot side.
type AdmissionPolicy struct {
	MaxOpenOrders int
	MaxSimulOpen  int
}

func (p AdmissionPolicy) enabled() bool { return p.MaxOpenOrders > 0 || p.MaxSimulOpen > 0 }

func (p AdmissionPolicy) validate() error {
	const limit = 1_000_000
	if p.MaxOpenOrders < 0 || p.MaxOpenOrders > limit || p.MaxSimulOpen < 0 || p.MaxSimulOpen > limit {
		return errors.New("execution: invalid admission policy")
	}
	return nil
}

func minLimit(a, b int) int {
	if a <= 0 {
		return b
	}
	if b <= 0 {
		return a
	}
	return min(a, b)
}

// AdmissionStatus is the durable admission state of one account.
//
// Each robot registers its own configured limits under a stable registrant
// identity. The effective policy is the most restrictive non-zero value of
// every registrant per field, so two robots with different limits can never
// widen each other's account limit. A registrant replaces only its own entry
// (a changed config takes effect on its restart); an empty policy removes it.
// A retired robot keeps constraining the account until it deregisters.
type AdmissionStatus struct {
	Policy        AdmissionPolicy
	Registrants   map[string]AdmissionPolicy
	WindowStartMS int64
	WindowEntries int
}

const admissionCheckpoint = "admission-v1"

func (st *AdmissionStatus) recompute() {
	st.Policy = AdmissionPolicy{}
	for _, p := range st.Registrants {
		st.Policy.MaxOpenOrders = minLimit(st.Policy.MaxOpenOrders, p.MaxOpenOrders)
		st.Policy.MaxSimulOpen = minLimit(st.Policy.MaxSimulOpen, p.MaxSimulOpen)
	}
}

func (s *Store) admissionState(ctx context.Context) (AdmissionStatus, error) {
	var state AdmissionStatus
	body, err := s.StrategyCheckpoint(ctx, "account-risk", admissionCheckpoint)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	err = json.Unmarshal(body, &state)
	return state, err
}

func admissionCheckpointOf(state AdmissionStatus) (StrategyCheckpoint, error) {
	body, err := json.Marshal(state)
	return StrategyCheckpoint{Strategy: "account-risk", Name: admissionCheckpoint, Payload: body}, err
}

// SetAdmissionPolicy registers or replaces one registrant's account limits.
func (b *SharedAccountBorrow) SetAdmissionPolicy(caller context.Context, registrant string, policy AdmissionPolicy) error {
	if !canonicalID(registrant) || len(registrant) > 512 {
		return errors.New("execution: invalid admission registrant")
	}
	if err := policy.validate(); err != nil {
		return err
	}
	return b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, caller)
		defer done()
		state, err := s.store.admissionState(ctx)
		if err != nil {
			return err
		}
		current, exists := state.Registrants[registrant]
		if policy.enabled() && exists && current == policy || !policy.enabled() && !exists {
			return nil
		}
		registrants := make(map[string]AdmissionPolicy, len(state.Registrants)+1)
		for name, p := range state.Registrants {
			registrants[name] = p
		}
		if policy.enabled() {
			registrants[registrant] = policy
		} else {
			delete(registrants, registrant)
		}
		state.Registrants = registrants
		state.recompute()
		checkpoint, err := admissionCheckpointOf(state)
		if err != nil {
			return err
		}
		return s.store.SaveStrategyCheckpoint(ctx, checkpoint.Strategy, checkpoint.Name, checkpoint.Payload)
	})
}

// AdmissionStatus returns the durable admission state.
func (b *SharedAccountBorrow) AdmissionStatus(caller context.Context) (AdmissionStatus, error) {
	var status AdmissionStatus
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, caller)
		defer done()
		var err error
		status, err = s.store.admissionState(ctx)
		return err
	})
	return status, err
}

// admissionRound returns the aligned round start of a create, or 0 when the
// request has no timeframe window.
func admissionRound(m *TimeSeriesMetadata) int64 {
	if m == nil || m.TimeFrame == "ws" {
		return 0
	}
	seconds, err := utils.TFToSecSafe(m.TimeFrame)
	if err != nil || seconds <= 0 {
		return 0
	}
	span := int64(seconds) * 1000
	at := m.CreatedMS
	if at >= 100000000000 {
		at = utils.AlignTfMSecs(at, span)
	} else {
		at = at / span * span
	}
	return max(int64(1), at)
}

// countOpenTimeSeries counts distinct time-series lots that are held or have an
// active unfilled entry, stopping once limit is reached.
func (s *SharedAccount) countOpenTimeSeries(ctx context.Context, snapshot AccountSnapshot, definitions []PendingDefinition, limit int) (int, error) {
	type key struct {
		strategy StrategyID
		lot      VirtualLotID
	}
	seen := map[key]bool{}
	var keys []key
	add := func(k key) {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	for _, lot := range snapshot.Lots {
		if lot.SignedSteps != 0 {
			add(key{lot.Strategy, lot.ID})
		}
	}
	for _, d := range definitions {
		o := d.Order
		if !o.Canceled && o.Entry.State != Expired && o.Entry.State != Canceled && o.Entry.QuantitySteps > o.Entry.FilledSteps {
			add(key{o.Strategy, o.Lot})
		}
	}
	sort.Slice(keys, func(a, b int) bool {
		if keys[a].strategy != keys[b].strategy {
			return keys[a].strategy < keys[b].strategy
		}
		return keys[a].lot < keys[b].lot
	})
	count := 0
	for _, k := range keys {
		if !canonicalID(string(k.strategy)) || !canonicalID(string(k.lot)) {
			continue
		}
		_, err := s.store.StrategyCheckpoint(ctx, k.strategy, TimeSeriesMetadataName(k.lot))
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if count++; limit > 0 && count >= limit {
			break
		}
	}
	return count, nil
}

// admitTimeSeriesCreate enforces the account admission policy for a new
// time-series entry. It returns the checkpoint to commit with the command when
// the round counter changed, or a typed admission_limit rejection.
func (s *SharedAccount) admitTimeSeriesCreate(ctx context.Context, c PendingCommand, snapshot AccountSnapshot, definitions []PendingDefinition) ([]StrategyCheckpoint, error) {
	if c.Metadata == nil {
		return nil, nil
	}
	state, err := s.store.admissionState(ctx)
	if err != nil {
		return nil, err
	}
	policy := state.Policy
	if !policy.enabled() {
		return nil, nil
	}
	if policy.MaxOpenOrders > 0 {
		open, err := s.countOpenTimeSeries(ctx, snapshot, definitions, policy.MaxOpenOrders)
		if err != nil {
			return nil, err
		}
		if open >= policy.MaxOpenOrders {
			return nil, rejectedAt(RejectAdmissionLimit, "execution: account max_open_orders reached", rejectionScope{Strategy: c.Strategy, Lot: c.Lot, Instrument: c.Create.Instrument, Kind: EntryIntent})
		}
	}
	round := admissionRound(c.Metadata)
	if policy.MaxSimulOpen <= 0 || round == 0 {
		return nil, nil
	}
	if round > state.WindowStartMS {
		state.WindowStartMS, state.WindowEntries = round, 0
	}
	if state.WindowEntries >= policy.MaxSimulOpen {
		return nil, rejectedAt(RejectAdmissionLimit, "execution: account max_simul_open reached", rejectionScope{Strategy: c.Strategy, Lot: c.Lot, Instrument: c.Create.Instrument, Kind: EntryIntent})
	}
	state.WindowEntries++
	checkpoint, err := admissionCheckpointOf(state)
	if err != nil {
		return nil, err
	}
	return []StrategyCheckpoint{checkpoint}, nil
}

// SetAdmissionPolicy registers one registrant's admission limits on an account.
func (h *Hub) SetAdmissionPolicy(ctx context.Context, key AccountKey, registrant string, policy AdmissionPolicy) error {
	return h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.SetAdmissionPolicy(ctx, registrant, policy)
	})
}

// AdmissionStatus returns the account admission state.
func (h *Hub) AdmissionStatus(ctx context.Context, key AccountKey) (AdmissionStatus, error) {
	var status AdmissionStatus
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		status, err = account.AdmissionStatus(ctx)
		return err
	})
	return status, err
}
