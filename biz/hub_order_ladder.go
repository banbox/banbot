package biz

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// Hub-side take-profit ladders and OCO entry groups.
//
// The account owner only knows one take-profit per lot and has no notion of an
// OCO group. The robot therefore orchestrates both on top of the existing
// pending commands, from poll():
//
//   - Ladder: the first rung is the lot's take-profit at creation. When the
//     owner reports that rung executed (ProtectionDone + settled exit while
//     inventory remains) the next rung is installed with set_protections.
//   - OCO: when a group member's entry has any fill, cancel_entry is sent for
//     every other unfilled member.
//
// Both are reconciliations of "what the journal says should be installed"
// against "what the owner reports", so a lost or refused command is simply
// recomputed on a later poll. Commands are frozen in hubOrderJournal.Followups
// (journal save before send, stable ID, writer epoch/sequence) but are kept out
// of hubOrderJournal.Commands: the generic resend path treats any send failure
// as fatal for the robot, which is right for entries and wrong for derived
// follow-ups.
//
// Failure policy: a follow-up that fails is retried with the SAME frozen body
// (a lost acknowledgement then returns the original receipt, never a second
// reduction) after 0.5s and 1s; after hubFollowupAttempts failed sends it is
// abandoned (the owner cannot have applied it, because a replay of an applied
// ID succeeds) and the reconciliation builds a fresh command with a fresh
// revision after an exponential back-off capped at hubFollowupMaxBackoff. An
// owner refusal never stops the robot.

const (
	hubFollowupAttempts   = 3
	hubFollowupMaxBackoff = 30 * time.Second
	hubFollowupLadder     = "/lad"
	hubFollowupOCO        = "/oco"
)

// hubLadder is the journaled progress of one lot's take-profit ladder.
type hubLadder struct {
	Raw       []strat.TakeProfitTarget // as requested, kept for re-resolution
	Rungs     []strat.TakeProfitTarget // resolved prices, nearest first
	Resolved  bool                     `json:",omitempty"` // resolved from the real average fill
	Index     int                      `json:",omitempty"` // active rung
	Remaining float64                  // share of the initial position still open
	Short     bool                     `json:",omitempty"`
	BEPending bool                     `json:",omitempty"` // stop-loss still has to be tightened to break-even
	Skipped   int                      `json:",omitempty"` // rungs merged into their successor (below one quantity step)
}

// hubLotPlan carries the robot-side orchestration state of one lot.
type hubLotPlan struct {
	OCOGroup     string     `json:",omitempty"`
	OCOCancelled bool       `json:",omitempty"` // cancel_entry accepted (or sibling already inactive)
	Ladder       *hubLadder `json:",omitempty"`
	Cmd          string     `json:",omitempty"` // in-flight Followups key
}

type hubFollowup struct {
	Pending  *execution.PendingCommand
	Kind     string
	BE       bool `json:",omitempty"` // the command carried the break-even stop tightening
	Attempts int  `json:",omitempty"`
}

type hubBackoff struct {
	fails int
	until time.Time
}

// hubLadderReference is the price a not yet filled entry is expected to fill at.
func hubLadderReference(req *strat.EnterReq, mid float64) float64 {
	switch {
	case req.Limit > 0:
		return req.Limit
	case req.Stop > 0:
		return req.Stop
	}
	return mid
}

func newHubLadder(req *strat.EnterReq, ref float64) (*hubLadder, error) {
	if len(req.TakeProfits) == 0 {
		return nil, nil
	}
	l := &hubLadder{Raw: append([]strat.TakeProfitTarget(nil), req.TakeProfits...), Remaining: 1, Short: req.Short}
	if !l.resolve(ref) {
		return nil, errors.New("biz: take-profit ladder rung resolves to a non-positive price")
	}
	return l, nil
}

// resolve mirrors resolveSharedTPLadder: Val/Rate are relative to ref, rungs
// are ordered nearest first. A non-positive price leaves the ladder unchanged.
func (l *hubLadder) resolve(ref float64) bool {
	if ref <= 0 || math.IsNaN(ref) || math.IsInf(ref, 0) {
		return false
	}
	dir := 1.0
	if l.Short {
		dir = -1
	}
	rungs := append([]strat.TakeProfitTarget(nil), l.Raw...)
	for i := range rungs {
		tp := &rungs[i]
		if tp.ExitRate == 0 && len(rungs) == 1 {
			tp.ExitRate = 1
		}
		if tp.Price <= 0 {
			if tp.Val > 0 {
				tp.Price = ref + tp.Val*dir
			} else if tp.Rate > 0 {
				tp.Price = ref * (1 + tp.Rate*dir)
			}
		}
		if tp.Price <= 0 || math.IsNaN(tp.Price) || math.IsInf(tp.Price, 0) {
			return false
		}
	}
	sort.SliceStable(rungs, func(a, b int) bool {
		if l.Short {
			return rungs[a].Price > rungs[b].Price
		}
		return rungs[a].Price < rungs[b].Price
	})
	l.Rungs = rungs
	return true
}

// tpSpec is the take-profit protection for the active rung. Its Rate is the
// fraction of the REMAINING position, exactly as sharedProtectionSpecsForOrder
// computes cutFrac. A finished ladder yields an empty (unconfigured) spec.
func (l *hubLadder) tpSpec() execution.PositionProtection {
	spec := execution.PositionProtection{Kind: execution.TakeProfitProtection}
	if l.Index < 0 || l.Index >= len(l.Rungs) || l.Remaining <= 1e-9 {
		return spec
	}
	rung := l.Rungs[l.Index]
	cut := rung.ExitRate / l.Remaining
	if cut >= 0.999 || l.Index == len(l.Rungs)-1 && sharedTPTotalRate(l.Raw) >= 0.999 {
		cut = 1
	}
	spec.Level = decimal.NewFromFloat(rung.Price)
	spec.Limit = decimal.NewFromFloat(rung.Limit)
	spec.Rate = decimal.NewFromFloat(cut)
	return spec
}

// advance completes the active rung, as UpdateByDataSeries does in the shared path.
func (l *hubLadder) advance() {
	rung := l.Rungs[l.Index]
	l.BEPending = rung.MoveSLToBreakeven
	l.Remaining = math.Max(0, l.Remaining-rung.ExitRate)
	l.Index++
}

func newHubLotPlan(req *strat.EnterReq, mid float64) (*hubLotPlan, error) {
	if len(req.TakeProfits) == 0 && req.OCOGroup == "" {
		return nil, nil
	}
	plan := &hubLotPlan{OCOGroup: req.OCOGroup}
	var err error
	if plan.Ladder, err = newHubLadder(req, hubLadderReference(req, mid)); err != nil {
		return nil, err
	}
	return plan, nil
}

func protectionSpec(specs []execution.PositionProtection, kind execution.ProtectionKind) execution.PositionProtection {
	for _, s := range specs {
		if s.Kind == kind {
			return s
		}
	}
	return execution.PositionProtection{Kind: kind}
}

func sameTPSpec(a, b execution.PositionProtection) bool {
	return a.Level.Equal(b.Level) && a.Limit.Equal(b.Limit) && a.Rate.Equal(b.Rate)
}

// tightenStop returns spec with its level moved to at least the entry average
// in the protective direction. A tighter existing stop is kept (as a level).
func tightenStop(spec execution.PositionProtection, prepared, avg decimal.Decimal, short bool) execution.PositionProtection {
	cur := spec.Level
	if prepared.IsPositive() {
		cur = prepared
	}
	if !cur.IsPositive() && spec.Distance.IsPositive() {
		if short {
			cur = avg.Add(spec.Distance)
		} else {
			cur = avg.Sub(spec.Distance)
		}
	}
	level := cur
	if short && (!cur.IsPositive() || avg.LessThan(cur)) || !short && avg.GreaterThan(cur) {
		level = avg
	}
	spec.Level, spec.Distance = level, decimal.Zero
	return spec
}

func (m *HubOrderMgr) lotAverage(lot *hubProjectedLot) decimal.Decimal {
	i, ok := m.config.Bridge.Instruments[lot.Record.Metadata.Symbol]
	if !ok || lot.Entered <= 0 {
		return decimal.Zero
	}
	return lot.EntryCost.Div(decimal.NewFromInt(lot.Entered).Mul(i.QuantityStep).Mul(i.ContractSize))
}

// ladderStep advances a copy of the ladder from owner facts. It reports whether
// the owner's take-profit differs from what the ladder wants (or a break-even
// tightening is outstanding), i.e. whether a set_protections is needed.
func (m *HubOrderMgr) ladderStep(l hubLadder, lot *hubProjectedLot, p execution.PendingDefinition, held int64) (hubLadder, bool) {
	owner := protectionSpec(p.Specs, execution.TakeProfitProtection)
	done := p.Order.ProtectionDone[string(execution.TakeProfitProtection)] && held != 0 && p.Order.Exit != nil && p.Order.Exit.State == execution.Filled
	switch {
	case l.Index < len(l.Rungs) && done && sameTPSpec(owner, l.tpSpec()):
		// The rung we installed has executed. Without the equality check an
		// advance whose command was lost would count the same execution twice.
		l.Resolved = true
		l.advance()
	case !l.Resolved && done:
		l.Resolved = true
	case !l.Resolved && lot.Entered > 0 && !done:
		if avg := m.lotAverage(lot).InexactFloat64(); avg > 0 {
			resolved := l
			if resolved.resolve(avg) {
				l = resolved
			}
		}
		l.Resolved = true
	}
	if l.Resolved && held != 0 {
		l.Rungs = append([]strat.TakeProfitTarget(nil), l.Rungs...) // l is a value copy; do not mutate the journaled slice
		l.skipTinyRungs(held)
	}
	if l.BEPending {
		// Reconcile against the owner's actual stop, never against what was sent:
		// a refused or dropped command leaves the flag set until the stop is tight.
		if avg := m.lotAverage(lot); avg.IsPositive() && stopTightened(protectionSpec(p.Specs, execution.StopLossProtection), p.Order.ProtectionLevels[string(execution.StopLossProtection)], avg, l.Short) {
			l.BEPending = false
		}
	}
	need := l.BEPending || l.Index < len(l.Rungs) && !sameTPSpec(owner, l.tpSpec())
	return l, need
}

// skipTinyRungs handles a non-final rung whose reduction would be zero whole
// quantity steps: the owner floors held_steps*rate and treats a zero reduction as
// "take-profit not configured", which would leave the lot unprotected on that
// rung. Rule: the rung is never installed; its share (and break-even request) is
// merged into the next rung and the ladder moves on WITHOUT reducing Remaining,
// so the next rung closes skipped+own share of the initial position and the
// cumulative total the strategy asked for is unchanged. The last rung closes the
// whole remainder (at least one step) and is never skipped.
func (l *hubLadder) skipTinyRungs(held int64) {
	if held < 0 {
		held = -held
	}
	for l.Index < len(l.Rungs)-1 && l.Remaining > 1e-9 {
		rate := l.tpSpec().Rate
		steps, err := execution.QuantitySteps(decimal.NewFromInt(held).Mul(rate), decimal.NewFromInt(1))
		if err != nil || steps > 0 {
			return
		}
		next := &l.Rungs[l.Index+1]
		skipped := l.Rungs[l.Index]
		next.ExitRate += skipped.ExitRate
		next.MoveSLToBreakeven = next.MoveSLToBreakeven || skipped.MoveSLToBreakeven
		l.Index++
		l.Skipped++
	}
}

// ladderBEBlocked reports that a break-even tightening is owed but cannot be
// expressed yet (entry average unknown). Freezing a follow-up then would claim
// the tightening and clear BEPending without it.
func ladderBEBlocked(l hubLadder, avg decimal.Decimal) bool {
	return l.BEPending && !avg.IsPositive()
}

// stopTightened reports whether the owner's stop is already at or beyond the
// entry average in the protective direction.
func stopTightened(spec execution.PositionProtection, prepared, avg decimal.Decimal, short bool) bool {
	cur := spec.Level
	if prepared.IsPositive() {
		cur = prepared
	}
	if !cur.IsPositive() {
		return false
	}
	if short {
		return cur.LessThanOrEqual(avg)
	}
	return cur.GreaterThanOrEqual(avg)
}

// ladderSpecs rebuilds the COMPLETE protection set (set_protections replaces
// it): stop-loss and trailing are carried over from base, take-profit comes
// from the ladder and the stop-loss is tightened when a break-even is pending.
func (m *HubOrderMgr) ladderSpecs(l hubLadder, base []execution.PositionProtection, prepared map[string]decimal.Decimal, avg decimal.Decimal) []execution.PositionProtection {
	var out []execution.PositionProtection
	seen := map[execution.ProtectionKind]bool{}
	for _, s := range base {
		seen[s.Kind] = true
		switch s.Kind {
		case execution.TakeProfitProtection:
			s = l.tpSpec()
		case execution.StopLossProtection:
			level := prepared[string(execution.StopLossProtection)]
			if l.BEPending && avg.IsPositive() {
				s = tightenStop(s, level, avg, l.Short)
			} else if level.IsPositive() {
				s.Level, s.Distance = level, decimal.Zero
			}
		}
		out = append(out, s)
	}
	if !seen[execution.TakeProfitProtection] {
		out = append(out, l.tpSpec())
	}
	if !seen[execution.StopLossProtection] && l.BEPending && avg.IsPositive() {
		out = append(out, tightenStop(execution.PositionProtection{Kind: execution.StopLossProtection}, decimal.Zero, avg, l.Short))
	}
	return out
}

func hubFollowupID(commandID string) bool {
	return strings.HasSuffix(commandID, hubFollowupLadder) || strings.HasSuffix(commandID, hubFollowupOCO)
}

type hubOCOKey struct{ strategy, symbol, group string }

func ocoOpen(p execution.PendingDefinition) bool {
	e := p.Order.Entry
	return !p.Order.Canceled && e.State != execution.Canceled && e.State != execution.Expired && e.State != execution.Filled
}

// orchestrateLocked runs the ladder / OCO reconciliation after a fully replayed
// projection page. snap is that page's account snapshot. It returns only
// errors that must stop the robot (journal persistence, writer sequence); an
// owner refusal or a failed send is logged, retried and eventually recomputed.
func (m *HubOrderMgr) orchestrateLocked(snap execution.AccountSnapshot) error {
	if len(m.journal.Plans) == 0 {
		return nil
	}
	held := map[string]int64{}
	for _, l := range snap.Lots {
		held[hubLotKey(l.Strategy, l.ID)] = l.SignedSteps
	}
	keys := make([]string, 0, len(m.journal.Plans))
	for k := range m.journal.Plans {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	filled := func(k string) bool {
		lot, p := m.journal.Lots[k], m.pending[k]
		return lot != nil && (lot.Entered > 0 || held[k] != 0 || p.Order.Entry.FilledSteps > 0)
	}
	won := map[hubOCOKey]bool{}
	for _, k := range keys {
		if plan := m.journal.Plans[k]; plan.OCOGroup != "" && filled(k) {
			md := m.journal.Lots[k].Record.Metadata
			won[hubOCOKey{md.StrategyName, md.Symbol, plan.OCOGroup}] = true
		}
	}
	now := time.Now()
	for _, k := range keys {
		plan, lot := m.journal.Plans[k], m.journal.Lots[k]
		if lot == nil {
			continue
		}
		p, active := m.pending[k]
		if !active {
			if plan.Cmd != "" && lot.Terminal {
				delete(m.journal.Followups, plan.Cmd)
				plan.Cmd = ""
			}
			continue
		}
		if plan.Cmd != "" {
			if err := m.retryFollowupLocked(k, plan, p, now); err != nil {
				return err
			}
			continue
		}
		if b := m.backoff[k]; b != nil && now.Before(b.until) {
			continue
		}
		md := lot.Record.Metadata
		if plan.OCOGroup != "" && !plan.OCOCancelled && won[hubOCOKey{md.StrategyName, md.Symbol, plan.OCOGroup}] && !filled(k) {
			if !ocoOpen(p) {
				plan.OCOCancelled = true // already cancelled/expired: nothing to send
				if err := m.saveLocked(); err != nil {
					return err
				}
				continue
			}
			if err := m.sendOCOCancelLocked(k, plan, lot, now); err != nil {
				return err
			}
			continue
		}
		if plan.Ladder != nil && !lot.Terminal {
			if next, need := m.ladderStep(*plan.Ladder, lot, p, held[k]); need {
				if err := m.sendLadderLocked(k, plan, lot, now); err != nil {
					return err
				}
			} else if plan.Ladder.BEPending && !next.BEPending {
				// The owner's stop is already at break-even (e.g. a user edit landed).
				plan.Ladder.BEPending = false
				if err := m.saveLocked(); err != nil {
					return err
				}
			}
		}
	}
	return m.pruneLocked()
}

// freshLocked re-reads the authoritative definition and holdings. Follow-ups are
// frozen from it, never from the possibly older projection page.
func (m *HubOrderMgr) freshLocked(k string, lot *hubProjectedLot) (execution.PendingDefinition, int64, bool, error) {
	definitions, err := m.config.Client.PendingOrders(m.ctx, m.config.Account)
	if err != nil {
		return execution.PendingDefinition{}, 0, false, err
	}
	snap, err := m.config.Client.Snapshot(m.ctx, m.config.Account)
	if err != nil {
		return execution.PendingDefinition{}, 0, false, err
	}
	var held int64
	for _, l := range snap.Lots {
		if hubLotKey(l.Strategy, l.ID) == k {
			held = l.SignedSteps
		}
	}
	for _, d := range definitions {
		if hubLotKey(d.Order.Strategy, d.Order.Lot) == k {
			return d, held, true, nil
		}
	}
	return execution.PendingDefinition{}, held, false, nil
}

func (m *HubOrderMgr) failFollowupLocked(k string) {
	if m.backoff == nil {
		m.backoff = map[string]*hubBackoff{}
	}
	b := m.backoff[k]
	if b == nil {
		b = &hubBackoff{}
		m.backoff[k] = b
	}
	b.fails++
	d := min(hubFollowupMaxBackoff, 500*time.Millisecond<<min(b.fails-1, 10))
	b.until = time.Now().Add(d)
}

// freezeFollowupLocked assigns identity/sequence, journals the command, and
// sends it. The journal is saved before the send.
func (m *HubOrderMgr) freezeFollowupLocked(k string, plan *hubLotPlan, lot *hubProjectedLot, p execution.PendingDefinition, kind string, build func(*execution.PendingCommand), commit func()) error {
	strategy := lot.Record.Strategy
	id, epoch, seq, err := m.nextLocked(strategy)
	if err != nil {
		return err
	}
	command := &execution.PendingCommand{ID: id + kind, Strategy: strategy, StrategyEpoch: epoch, StrategySequence: seq, Lot: lot.Record.Lot, ExpectedRevision: p.CommandRevision}
	build(command)
	if m.journal.Followups == nil {
		m.journal.Followups = map[string]*hubFollowup{}
	}
	f := &hubFollowup{Pending: command, Kind: kind}
	m.journal.Followups[command.ID] = f
	plan.Cmd = command.ID
	commit()
	f.BE = kind == hubFollowupLadder && plan.Ladder != nil && plan.Ladder.BEPending
	if err = m.saveLocked(); err != nil {
		return err
	}
	return m.sendFollowupLocked(k, plan, f, time.Now())
}

func (m *HubOrderMgr) sendLadderLocked(k string, plan *hubLotPlan, lot *hubProjectedLot, now time.Time) error {
	p, held, ok, err := m.freshLocked(k, lot)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	next, need := m.ladderStep(*plan.Ladder, lot, p, held)
	if !need || held == 0 && lot.Entered == 0 {
		return nil
	}
	avg := m.lotAverage(lot)
	if ladderBEBlocked(next, avg) {
		// Do not freeze a command that claims the break-even tightening while the
		// average is unknown; the flag survives until it is.
		m.deps.Logger().Warn("hub ladder break-even postponed: entry average unknown", zap.String("lot", string(lot.Record.Lot)))
		m.failFollowupLocked(k)
		return nil
	}
	if next.Skipped > plan.Ladder.Skipped {
		m.deps.Logger().Warn("hub ladder rung below one quantity step merged into the next rung", zap.String("lot", string(lot.Record.Lot)), zap.Int("rung", next.Index), zap.Int64("held_steps", held))
	}
	specs := m.ladderSpecs(next, p.Specs, p.Order.ProtectionLevels, avg)
	progress := next.Index != plan.Ladder.Index
	m.deps.Logger().Info("hub take-profit ladder update", zap.String("lot", string(lot.Record.Lot)), zap.Int("rung", next.Index), zap.Bool("advanced", progress), zap.Float64("remaining_share", next.Remaining))
	return m.freezeFollowupLocked(k, plan, lot, p, hubFollowupLadder, func(c *execution.PendingCommand) {
		c.Action, c.Protections = execution.PendingSetProtections, specs
	}, func() { *plan.Ladder = next })
}

func (m *HubOrderMgr) sendOCOCancelLocked(k string, plan *hubLotPlan, lot *hubProjectedLot, now time.Time) error {
	p, held, ok, err := m.freshLocked(k, lot)
	if err != nil {
		return err
	}
	if !ok || held != 0 || lot.Entered > 0 || p.Order.Entry.FilledSteps > 0 || !ocoOpen(p) {
		return nil
	}
	md := lot.Record.Metadata
	md.ExitTag = ExitTagOCOCancel
	m.deps.Logger().Info("hub OCO sibling cancelled", zap.String("symbol", md.Symbol), zap.String("group", plan.OCOGroup), zap.Int64("order_id", md.OrderID))
	return m.freezeFollowupLocked(k, plan, lot, p, hubFollowupOCO, func(c *execution.PendingCommand) {
		c.Action, c.Metadata = execution.PendingCancelEntry, &md
	}, func() {})
}

func (m *HubOrderMgr) finishFollowupLocked(k string, plan *hubLotPlan, f *hubFollowup) error {
	delete(m.journal.Followups, plan.Cmd)
	plan.Cmd = ""
	switch f.Kind {
	case hubFollowupOCO:
		plan.OCOCancelled = true
	case hubFollowupLadder:
		if f.BE && plan.Ladder != nil {
			plan.Ladder.BEPending = false
		}
	}
	delete(m.backoff, k)
	return m.saveLocked()
}

// sendFollowupLocked sends the frozen command. It never returns an owner or
// transport refusal; those are counted and handled by retry/abandon.
func (m *HubOrderMgr) sendFollowupLocked(k string, plan *hubLotPlan, f *hubFollowup, now time.Time) error {
	_, err := m.config.Client.ApplyPendingCommand(m.ctx, m.config.Account, *f.Pending)
	if err == nil {
		return m.finishFollowupLocked(k, plan, f)
	}
	if m.ctx.Err() != nil {
		return nil
	}
	f.Attempts++
	m.deps.Logger().Warn("hub follow-up command failed", zap.String("command", f.Pending.ID), zap.String("action", string(f.Pending.Action)), zap.Int("attempt", f.Attempts), zap.Error(err))
	if f.Attempts >= hubFollowupAttempts {
		// A replay of an applied ID succeeds, so a repeated failure means the
		// owner did not apply it (refused, stale revision or sequence). The
		// reconciliation recomputes from fresh owner state after a back-off.
		m.deps.Logger().Warn("hub follow-up command abandoned; it will be recomputed", zap.String("command", f.Pending.ID))
		delete(m.journal.Followups, plan.Cmd)
		plan.Cmd = ""
		m.failFollowupLocked(k)
	} else {
		if m.backoff == nil {
			m.backoff = map[string]*hubBackoff{}
		}
		m.backoff[k] = &hubBackoff{fails: f.Attempts, until: now.Add(250 * time.Millisecond << f.Attempts)}
	}
	return m.saveLocked()
}

func (m *HubOrderMgr) retryFollowupLocked(k string, plan *hubLotPlan, p execution.PendingDefinition, now time.Time) error {
	f := m.journal.Followups[plan.Cmd]
	if f == nil || f.Pending == nil {
		plan.Cmd = ""
		return m.saveLocked()
	}
	if b := m.backoff[k]; b != nil && now.Before(b.until) {
		return nil
	}
	return m.sendFollowupLocked(k, plan, f, now)
}

// pruneLocked drops plans that can no longer act: closed lots, and plans whose
// entry command was refused. An OCO member is kept while a sibling is unresolved.
func (m *HubOrderMgr) pruneLocked() error {
	changed := false
	for k, plan := range m.journal.Plans {
		lot := m.journal.Lots[k]
		drop := false
		if lot == nil {
			drop = true
			for _, c := range m.journal.Commands {
				if c.Pending != nil && c.Pending.Create != nil && hubLotKey(c.Pending.Strategy, c.Pending.Lot) == k && !c.Rejected {
					drop = false
				}
			}
		} else if plan.OCOCancelled && lot.Entered == 0 && plan.Cmd == "" {
			// A cancelled OCO loser never fills and is never archived.
			drop = true
		} else if lot.Archived && plan.Cmd == "" {
			drop = true
			if plan.OCOGroup != "" {
				md := lot.Record.Metadata
				for k2, other := range m.journal.Plans {
					if k2 == k || other.OCOGroup != plan.OCOGroup || other.OCOCancelled {
						continue
					}
					if l2 := m.journal.Lots[k2]; l2 != nil && l2.Record.Metadata.StrategyName == md.StrategyName && l2.Record.Metadata.Symbol == md.Symbol && l2.Entered == 0 && !l2.Terminal {
						drop = false
					}
				}
			}
		}
		if drop {
			delete(m.journal.Plans, k)
			changed = true
		}
	}
	if changed {
		return m.saveLocked()
	}
	return nil
}

// hubRejectLadderPlan documents the up-front rejection used by enterLocked.
func hubRejectLadderPlan(m *HubOrderMgr, name, symbol, tag string, err error) error {
	m.deps.Logger().Warn("entry rejected: take-profit ladder / OCO group cannot be expressed on the Hub path",
		zap.String("strategy", name), zap.String("symbol", symbol), zap.String("tag", tag), zap.Error(err))
	return fmt.Errorf("%w: %v", errSharedEntryRejected, err)
}

// ladderSpecsFromBase replaces the take-profit of a creation spec set by the first rung.
func ladderSpecsFromBase(l hubLadder, base []execution.PositionProtection) []execution.PositionProtection {
	var m HubOrderMgr
	return m.ladderSpecs(l, base, nil, decimal.Zero)
}
