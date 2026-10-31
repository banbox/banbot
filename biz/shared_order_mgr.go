package biz

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
	"github.com/shopspring/decimal"
)

type SharedStrategyBinding struct {
	ID               execution.StrategyID
	StakeNAVFraction decimal.Decimal
	MaxNotional      decimal.Decimal
}

// SharedOrderBridgeConfig fixes strategy attribution and instrument units.
// Quote must return an actually visible bid/ask, not an OHLC future range.
type SharedOrderBridgeConfig struct {
	OrderPolicy       execution.OrderPolicy
	DefaultLimitPrice func(context.Context, string, execution.OrderSide, int) (float64, error)
	Version           string
	Instruments       map[string]execution.Instrument
	Strategies        map[string]SharedStrategyBinding
	Risk              execution.PortfolioRisk
	Quote             func(string, int64) (execution.VisibleQuote, error)
	QuoteContext      func(context.Context, string, int64) (execution.VisibleQuote, error)
	IntentTTLMS       int64
}

func (c *SharedOrderBridgeConfig) Validate() error {
	if c == nil || c.Version == "" || len(c.Instruments) == 0 || len(c.Strategies) == 0 || c.Quote == nil && c.QuoteContext == nil || c.IntentTTLMS <= 0 {
		return errors.New("biz: incomplete shared legacy bridge declaration")
	}
	if err := c.OrderPolicy.Validate(); err != nil {
		return err
	}
	seen := map[execution.StrategyID]bool{}
	for name, b := range c.Strategies {
		if name == "" || b.ID == "" || seen[b.ID] || !b.MaxNotional.IsPositive() || b.StakeNAVFraction.IsNegative() || b.StakeNAVFraction.GreaterThan(decimal.NewFromInt(1)) {
			return errors.New("biz: invalid shared strategy attribution/budget")
		}
		seen[b.ID] = true
		if cap, ok := c.Risk.StrategyGrossLimits[b.ID]; !ok || !cap.IsPositive() {
			return errors.New("biz: shared legacy strategy risk policy missing")
		}
	}
	seenInstruments := map[string]bool{}
	for symbol, i := range c.Instruments {
		if symbol == "" {
			return errors.New("biz: empty shared instrument symbol")
		}
		if err := i.Validate(); err != nil {
			return err
		}
		if seenInstruments[i.ID] {
			return errors.New("biz: duplicate shared instrument identity")
		}
		seenInstruments[i.ID] = true
	}
	return nil
}

type sharedTSOrder struct {
	execution.PendingOrder
	ID               int64
	StrategyName     string
	Symbol           string
	SID              int32
	TimeFrame        string
	Request          strat.EnterReq
	SourceTaskID     int64
	SourceEntrySteps *int64 `json:",omitempty"`
	SourceInfo       map[string]any
	SourceExit       *ormo.ExOrder
	SourceLotFees    decimal.Decimal
	CreatedMS        int64
	InitPrice        float64
	ExitTag          string
	FeedExpiry       bool
	CreatedFeedBar   int64
}
type sharedTSCheckpoint struct {
	Version        string
	Serial         int64
	StorageVersion int                       `json:",omitempty"`
	ActiveOrders   []string                  `json:",omitempty"`
	Orders         map[string]*sharedTSOrder `json:",omitempty"`
	Feeds          map[string]sharedFeedProgress
	Admission      sharedEntryAdmission
	Commands       map[string]sharedTSCommand        `json:",omitempty"`
	Accepted       []execution.StrategyAcceptedEvent `json:"-"`
	store          *execution.Store
	ctx            context.Context
	pendingOrders  map[sharedPendingKey]execution.PendingOrder
	carriedPending map[sharedPendingKey]execution.PendingDefinition
}

type sharedPendingKey struct {
	Strategy execution.StrategyID
	Lot      execution.VirtualLotID
}

type sharedTSCommand struct {
	Hash   string
	Orders []int64
}

var errSharedCommandReplay = errors.New("biz: shared command already accepted")

func sharedCommand(state *sharedTSCheckpoint, id string, request any) (sharedTSCommand, error) {
	if id == "" {
		return sharedTSCommand{}, nil
	}
	body, err := json.Marshal(request)
	if err != nil {
		return sharedTSCommand{}, err
	}
	hash := sha256.Sum256(body)
	command := sharedTSCommand{Hash: hex.EncodeToString(hash[:])}
	previous, found, err := state.loadCommand(id)
	if err != nil {
		return command, err
	}
	if found {
		if previous.Hash != command.Hash {
			return command, errors.New("biz: command ID reused with different content")
		}
		return previous, errSharedCommandReplay
	}
	if state.Commands == nil {
		state.Commands = map[string]sharedTSCommand{}
	}
	return command, nil
}

type sharedEntryAdmission struct {
	BarMS         int64
	SimulOpen     int
	Strategies    map[string]int
	AccountLimits *sharedEntryLimits
	PolicyLimits  map[string]sharedEntryLimits
}

type sharedEntryLimits struct {
	MaxOpen  int
	MaxSimul int
}

var errSharedEntryRejected = errors.New("biz: shared entry rejected by admission")

type sharedFeedProgress struct {
	EndMS int64
	Bars  int64
}

type sharedJobIdentity struct{ Strategy, Symbol, TimeFrame string }

func sharedFeedKey(sid int32, tf string) string { return fmt.Sprintf("%d/%s", sid, tf) }

func sharedProtectionSpecs(req strat.EnterReq) []execution.PositionProtection {
	return []execution.PositionProtection{
		{Kind: execution.StopLossProtection, Level: decimal.NewFromFloat(req.StopLoss), Distance: decimal.NewFromFloat(req.StopLossVal), Limit: decimal.NewFromFloat(req.StopLossLimit), Rate: decimal.NewFromFloat(req.StopLossRate)},
		{Kind: execution.TakeProfitProtection, Level: decimal.NewFromFloat(req.TakeProfit), Distance: decimal.NewFromFloat(req.TakeProfitVal), Limit: decimal.NewFromFloat(req.TakeProfitLimit), Rate: decimal.NewFromFloat(req.TakeProfitRate)},
		{Kind: execution.TrailingProtection, TrailingPercent: decimal.NewFromFloat(req.CallbackPct), ActivationPrice: decimal.NewFromFloat(req.ActivationPrice)},
	}
}

// SharedOrderMgr implements the existing request facade without inheriting a
// Local/LiveOrderMgr network path. All fills and cash remain in SharedAccount.
type SharedOrderMgr struct {
	mu                sync.Mutex
	deps              RuntimeDeps
	account           *SharedAccountBorrow
	config            *SharedOrderBridgeConfig
	callback          FnOdCb
	jobs              map[sharedJobIdentity]*strat.StratJob
	projecting        bool
	projectionPending bool
	lastError         error
}

func NewSharedOrderMgr(deps RuntimeDeps, account *SharedAccountBorrow, config *SharedOrderBridgeConfig, callback FnOdCb) (*SharedOrderMgr, error) {
	if account == nil || deps.Orders == nil || deps.Clock == nil {
		return nil, errors.New("biz: shared manager requires bound account, order projection and clock")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	manager := &SharedOrderMgr{deps: deps, account: account, config: config, callback: callback, jobs: map[sharedJobIdentity]*strat.StratJob{}}
	if err := account.WithState(func(s *SharedAccount) error {
		state, err := loadSharedCheckpoint(s.Store(), account.Context(), config.Version)
		if err != nil {
			return err
		}
		if len(state.Orders) == 0 {
			return nil
		}
		definitions, err := manager.pendingDefinitions(&state)
		if err != nil {
			return err
		}
		feeds := make(map[execution.PendingFeedID]execution.PendingFeedProgress, len(state.Feeds))
		for id, progress := range state.Feeds {
			feeds[execution.PendingFeedID(id)] = execution.PendingFeedProgress{EndMS: progress.EndMS, Bars: progress.Bars}
		}
		legacy := make(map[sharedPendingKey]*sharedTSOrder, len(state.Orders))
		for _, old := range state.Orders {
			legacy[sharedPendingKey{old.Strategy, old.Lot}] = old
		}
		var bindings []execution.LegacyPendingFeedBinding
		for n := range definitions {
			order := &definitions[n].Order
			old := legacy[sharedPendingKey{order.Strategy, order.Lot}]
			if old == nil || !old.FeedExpiry || old.Request.StopBars <= 0 || order.EntryFeedExpiry != nil {
				continue
			}
			if old.CreatedFeedBar < 0 || old.CreatedFeedBar > math.MaxInt64-int64(old.Request.StopBars) {
				return errors.New("biz: invalid legacy entry feed deadline")
			}
			bindings = append(bindings, execution.LegacyPendingFeedBinding{Strategy: order.Strategy, Lot: order.Lot,
				Expiry: execution.PendingFeedExpiry{Feed: execution.PendingFeedID(sharedFeedKey(old.SID, old.TimeFrame)), ExpiresAtBar: old.CreatedFeedBar + int64(old.Request.StopBars)}})
		}
		return s.SeedPendingBookWithFeeds(account.Context(), definitions, feeds, bindings)
	}); err != nil {
		return nil, err
	}
	return manager, nil
}

// BindJobs restores projections into configured jobs without trading or replaying
// historical callbacks. Subsequent real Trader callbacks use this same facade.
func (m *SharedOrderMgr) BindJobs(jobs []*strat.StratJob) error {
	pending := map[sharedJobIdentity]*strat.StratJob{}
	for _, job := range jobs {
		if job == nil || job.Strat == nil || job.Symbol == nil {
			return errors.New("biz: invalid shared legacy job")
		}
		binding, ok := m.config.Strategies[job.Strat.Name]
		if !ok || binding.ID == "" {
			return errors.New("biz: legacy job strategy not declared")
		}
		key := sharedJobIdentity{job.Strat.Name, job.Symbol.Symbol, job.TimeFrame}
		if job.TimeFrame == "" || pending[key] != nil {
			return errors.New("biz: duplicate or incomplete shared legacy job identity")
		}
		pending[key] = job
	}
	m.mu.Lock()
	for key, job := range pending {
		m.jobs[key] = job
	}
	m.mu.Unlock()
	return m.project(true)
}

func initSharedOrderMgr(deps RuntimeDeps, callback FnOdCb) {
	manager, err := NewSharedOrderMgr(deps, deps.SharedExecution, deps.SharedOrderBridge, callback)
	if err != nil {
		panic(err)
	}
	if existing := deps.Trading.OrderManager(deps.DefaultAccount); existing != nil {
		if _, ok := existing.(*SharedOrderMgr); !ok {
			panic("biz: legacy manager must stop before shared cutover")
		}
		return
	}
	if err := manager.project(true); err != nil {
		panic(err)
	}
	deps.Trading.SetOrderManager(deps.DefaultAccount, manager)
	deps.Orders.SetSoftwareEditListener(func(od *ormo.InOutOrder, action string) { manager.EditOrder(od, action) })
	unsubscribe, err := deps.SharedExecution.SubscribeCommitted(func() {
		if deps.Callbacks != nil {
			if !deps.Callbacks.EnterCallback() {
				return
			}
			defer deps.Callbacks.LeaveCallback()
		}
		if err := manager.project(false); err != nil {
			manager.failClosed(err, "private-projection")
		}
	})
	if err != nil {
		panic(err)
	}
	if lifecycle, ok := deps.Callbacks.(data.LifecycleRegistrar); ok {
		lifecycle.OnClose(unsubscribe)
	}
}
func sharedBridgeError(err error) *errs.Error {
	if err == nil {
		return nil
	}
	return errs.New(core.ErrBadConfig, err)
}
func sharedOrderKey(strategy execution.StrategyID, id int64) string {
	return fmt.Sprintf("%s/%d", strategy, id)
}

func (m *SharedOrderMgr) mutate(change func(*SharedAccount, *sharedTSCheckpoint, execution.AccountSnapshot, int64, int64) error) error {
	return m.account.WithState(func(s *SharedAccount) error {
		now := m.deps.Clock.TimeMS()
		state, err := loadSharedCheckpoint(s.Store(), m.account.Context(), m.config.Version)
		if err != nil {
			return err
		}
		snapshot, err := s.Store().Snapshot(context.Background())
		if err != nil {
			return err
		}
		if err := m.restoreTakeovers(m.account.Context(), s.Store(), &state, snapshot); err != nil {
			return err
		}
		state.Serial++
		if err := change(s, &state, snapshot, now, state.Serial); err != nil {
			if errors.Is(err, errSharedCommandReplay) {
				return nil
			}
			return err
		}
		now = s.SendTime(now)
		snapshot, err = s.Store().Snapshot(context.Background())
		if err != nil {
			return err
		}
		request, err := m.coordinate(s, &state, snapshot, now)
		if err != nil {
			return err
		}
		checkpoint, err := splitSharedCheckpoint(state, snapshot)
		if err != nil {
			return err
		}
		if !s.Reconciled() {
			return errors.New("biz: shared account is not reconciled")
		}
		var updates []execution.StrategyRebalance
		for _, binding := range m.config.Strategies {
			update := execution.StrategyRebalance{Strategy: binding.ID, PlanID: request.PlanID, DecisionMS: request.DecisionMS, ExpiresMS: request.ExpiresMS, Mode: execution.StrategyTargetsPatch}
			for _, r := range request.Requests {
				owned := execution.InstrumentRebalance{Instrument: r.Instrument, Quote: r.Quote}
				for _, target := range r.Targets {
					if target.Strategy == binding.ID {
						owned.Targets = append(owned.Targets, target)
					}
				}
				for _, reservation := range r.ConditionalReservations {
					if reservation.Intent.Strategy == binding.ID {
						owned.ConditionalReservations = append(owned.ConditionalReservations, reservation)
					}
				}
				for _, intent := range r.IntentConstraints {
					if intent.Strategy == binding.ID {
						owned.IntentConstraints = append(owned.IntentConstraints, intent)
					}
				}
				if len(owned.Targets) > 0 {
					update.Requests = append(update.Requests, owned)
				}
			}
			updates = append(updates, update)
		}
		definitions, err := m.pendingDefinitions(&state)
		if err != nil {
			return err
		}
		pendingCheckpoint, err := execution.PendingBookCheckpoint(definitions)
		if err != nil {
			return err
		}
		prepared, err := s.PrepareStrategiesWithCheckpoints(updates, m.account.Context(), []execution.StrategyCheckpoint{checkpoint, pendingCheckpoint})
		if err != nil {
			return err
		}
		return s.SendPrepared(prepared, now, m.account.Context())
	})
}

func (m *SharedOrderMgr) quote(s *SharedAccount, id string, now int64) (execution.VisibleQuote, error) {
	quote, err := s.ReadQuote(m.account.Context(), id, now)
	if err == nil {
		now = s.SendTime(now)
		if !quote.Bid.IsPositive() || quote.Ask.LessThan(quote.Bid) || quote.AtMS <= 0 || quote.AtMS > quote.ReceivedMS || quote.ReceivedMS > now || quote.ValidUntilMS <= now {
			return execution.VisibleQuote{}, errors.New("biz: execution quote is not visible at IO completion")
		}
	}
	return quote, err
}
func (m *SharedOrderMgr) contextFor(s *SharedAccount, snapshot execution.AccountSnapshot, name string, instrument execution.Instrument, lot execution.VirtualLotID, intent execution.VirtualIntentID, now, bar int64) (IntentBridgeContext, error) {
	b, ok := m.config.Strategies[name]
	if !ok {
		return IntentBridgeContext{}, errors.New("biz: strategy is not declared for shared account")
	}
	quote, err := m.quote(s, instrument.ID, now)
	if err != nil {
		return IntentBridgeContext{}, err
	}
	now = s.SendTime(now)
	if !quote.Bid.IsPositive() || quote.Ask.LessThan(quote.Bid) || quote.ReceivedMS > now || quote.AtMS > quote.ReceivedMS || quote.ValidUntilMS <= now {
		return IntentBridgeContext{}, errors.New("biz: execution quote is not visible")
	}
	nav := snapshot.SyntheticStrategyCash[b.ID]
	for _, l := range snapshot.Lots {
		if l.Strategy == b.ID {
			q, err := m.quote(s, l.Instrument.ID, now)
			if err != nil {
				return IntentBridgeContext{}, err
			}
			now = s.SendTime(now)
			nav = nav.Add(l.Unrealized(q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2))))
		}
	}
	if quote.ValidUntilMS <= now {
		return IntentBridgeContext{}, errors.New("biz: execution quote expired during NAV collection")
	}
	return IntentBridgeContext{Account: s.AccountKey(), Strategy: b.ID, StrategyName: name, Lot: lot, Intent: intent, Instrument: instrument.ID, QuantityStep: instrument.QuantityStep, ContractSize: instrument.ContractSize, ReferencePrice: quote.Bid.Add(quote.Ask).Div(decimal.NewFromInt(2)), StrategyNAV: nav, StakeNAVFraction: b.StakeNAVFraction, MaxNotional: b.MaxNotional, NowMS: now, Bar: bar}, nil
}

func (m *SharedOrderMgr) EnterOrder(exs *orm.ExSymbol, tf string, req *strat.EnterReq) (*ormo.InOutOrder, *errs.Error) {
	return m.enterOrder(exs, tf, req, nil, false)
}

func (m *SharedOrderMgr) enterOrder(exs *orm.ExSymbol, tf string, req *strat.EnterReq, sourceInfo map[string]any, relay bool) (*ormo.InOutOrder, *errs.Error) {
	if exs == nil || req == nil {
		return nil, sharedBridgeError(errors.New("biz: missing shared entry symbol/request"))
	}
	if m.deps.Core != nil && m.deps.Core.EnvReal {
		defaults := makeRuntimeOrderConfig(m.deps, m.deps.DefaultAccount)
		if req.Leverage != 0 || defaults.accountLeverage != 0 {
			return nil, sharedBridgeError(errors.New("biz: live execution does not support requested leverage changes"))
		}
	}
	instrument, ok := m.config.Instruments[exs.Symbol]
	if !ok {
		return nil, sharedBridgeError(errors.New("biz: undeclared shared instrument"))
	}
	var id int64
	err := m.mutate(func(s *SharedAccount, state *sharedTSCheckpoint, snapshot execution.AccountSnapshot, now, serial int64) error {
		name := req.StratName
		if name == "" && len(m.config.Strategies) == 1 {
			for n := range m.config.Strategies {
				name = n
			}
		}
		binding, ok := m.config.Strategies[name]
		if !ok {
			return errors.New("biz: ambiguous shared entry strategy")
		}
		copy := *req
		copy.StratName = name
		commandKey := ""
		if req.CommandID != "" {
			commandKey = string(binding.ID) + "/entry/" + req.CommandID
		}
		command, err := sharedCommand(state, commandKey, struct {
			Symbol, TimeFrame string
			Request           strat.EnterReq
			SourceInfo        map[string]any
		}{exs.Symbol, tf, copy, sourceInfo})
		if errors.Is(err, errSharedCommandReplay) {
			id = command.Orders[0]
			return err
		}
		if err != nil {
			return err
		}
		defaults := makeRuntimeOrderConfig(m.deps, m.deps.DefaultAccount)
		defaultStyle := copy.OrderType == core.OrderTypeEmpty && defaults.orderType != ""
		if defaultStyle {
			copy.OrderType = slices.Index(core.OrderTypeEnums, defaults.orderType)
			if copy.OrderType < core.OrderTypeEmpty || copy.OrderType > core.OrderTypeLimitMaker {
				return errors.New("biz: unsupported configured shared order style")
			}
		}
		if copy.Leverage == 0 {
			gate := OrderMgr{runtimeDeps: true, runtimeCfg: defaults}
			copy.Leverage = gate.accountLeverage()
		}
		allowed, err := m.allowEntry(state, snapshot, exs, tf, &copy)
		if err != nil {
			return err
		}
		if !allowed {
			return errSharedEntryRejected
		}
		id = serial
		lot := execution.VirtualLotID(fmt.Sprintf("legacy/%s/%d", binding.ID, id))
		q, err := m.quote(s, instrument.ID, now)
		if err != nil {
			return err
		}
		if core.IsLimitOrder(copy.OrderType) && (relay || defaultStyle && copy.Limit == 0) {
			// Relay and omitted configured limits use the current passive side.
			// Observe inside the owner and after the command replay check so a
			// retry retains the first committed limit and request identity.
			price := q.Bid
			if copy.Short {
				price = q.Ask
			}
			if !relay && m.config.DefaultLimitPrice != nil {
				side := execution.Buy
				if copy.Short {
					side = execution.Sell
				}
				value, err := s.ReadLimitPrice(m.account.Context(), m.config.DefaultLimitPrice, instrument.ID, side, defaults.limitVolSecs)
				if err != nil {
					return err
				}
				if value <= 0 {
					return errors.New("biz: execution default limit price unavailable")
				}
				price = decimal.NewFromFloat(value)
			}
			copy.Limit = price.InexactFloat64()
		}
		if !relay && copy.Limit > 0 && copy.StopBars == 0 {
			copy.StopBars = defaults.stopEnterBars
		}
		c, err := m.contextFor(s, snapshot, name, instrument, lot, execution.VirtualIntentID(fmt.Sprintf("legacy-entry/%s/%d", binding.ID, id)), now, q.Bar)
		if err != nil {
			return err
		}
		bridge, err := BridgeEntryReq(c, &copy)
		if err != nil {
			return err
		}
		state.Orders[sharedOrderKey(binding.ID, id)] = &sharedTSOrder{ID: id, StrategyName: name, PendingOrder: execution.PendingOrder{Strategy: binding.ID, Lot: lot, Entry: bridge.Intent, Protections: map[string]execution.EligibleIntent{}, ProtectionDone: map[string]bool{}}, Symbol: exs.Symbol, SID: exs.ID, TimeFrame: tf, Request: bridge.Original}
		record := state.Orders[sharedOrderKey(binding.ID, id)]
		if q.Bar == 0 && copy.StopBars > 0 {
			if err := s.BindPendingFeedExpiry(m.account.Context(), &record.PendingOrder, execution.PendingFeedID(sharedFeedKey(exs.ID, tf)), int64(copy.StopBars)); err != nil {
				return err
			}
		}
		record.CreatedMS, record.InitPrice = c.NowMS, q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2)).InexactFloat64()
		record.SourceInfo = map[string]any{}
		for key, value := range req.Infos {
			record.SourceInfo[key] = value
		}
		for key, value := range sourceInfo {
			record.SourceInfo[key] = value
		}
		if commandKey != "" {
			command.Orders = []int64{id}
			state.Commands[commandKey] = command
		}
		acceptedID := req.CommandID
		if acceptedID == "" {
			acceptedID = fmt.Sprintf("legacy-entry/%d", serial)
		}
		state.Accepted = append(state.Accepted, execution.StrategyAcceptedEvent{Strategy: binding.ID, Lot: lot, Kind: execution.EntryIntent, CommandID: acceptedID, AtMS: now})
		return nil
	})
	if errors.Is(err, errSharedEntryRejected) {
		return nil, nil
	}
	if err != nil {
		return nil, sharedBridgeError(err)
	}
	if err := m.project(false); err != nil {
		return nil, sharedBridgeError(err)
	}
	orders, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
	lock.Lock()
	od := orders[id]
	lock.Unlock()
	if od == nil {
		// A reentrant or concurrent projection only schedules event delivery.
		// Publish this committed row now without replaying callbacks or advancing
		// their cursor; waiting for the active projection could deadlock its caller.
		if err := m.projectSnapshot(false, id); err != nil {
			return nil, sharedBridgeError(err)
		}
		lock.Lock()
		od = orders[id]
		lock.Unlock()
	}
	if od == nil {
		return nil, sharedBridgeError(errors.New("biz: committed shared entry projection missing"))
	}
	m.pruneClosedFacades()
	return od, nil
}

func (m *SharedOrderMgr) allowEntry(state *sharedTSCheckpoint, snapshot execution.AccountSnapshot, exs *orm.ExSymbol, tf string, req *strat.EnterReq) (bool, error) {
	// Admission state is staged with the request checkpoint and serialized by
	// the account owner. Rejected or failed requests do not spend bar capacity.
	counts := make(map[string]int)
	held := make(map[[2]string]int64)
	for _, lot := range snapshot.Lots {
		held[[2]string{string(lot.Strategy), string(lot.ID)}] = lot.SignedSteps
	}
	openNum := 0
	for _, record := range state.Orders {
		pending := !record.Canceled && record.Entry.State != execution.Expired && record.Entry.State != execution.Canceled && record.Entry.QuantitySteps > record.Entry.FilledSteps
		if held[[2]string{string(record.Strategy), string(record.Lot)}] != 0 || pending {
			openNum++
			counts[record.StrategyName]++
		}
	}
	policies := make(map[string]*config.RunPolicyConfig)
	if m.deps.Config != nil && m.deps.Config.View() != nil {
		for _, policy := range m.deps.Config.View().RunPolicy {
			if policy != nil {
				policies[policy.Name] = policy
			}
		}
	}
	m.mu.Lock()
	for key, job := range m.jobs {
		if key.Symbol == exs.Symbol && key.TimeFrame == tf && job.Strat != nil && job.Strat.Policy != nil {
			policies[key.Strategy] = job.Strat.Policy
		}
	}
	m.mu.Unlock()
	previous := state.Admission
	gate := OrderMgr{Account: m.deps.DefaultAccount, clock: m.deps.Clock, runtimeCore: m.deps.Core, runtimeDeps: true, walletDeps: m.deps, runtimeCfg: makeRuntimeOrderConfig(m.deps, m.deps.DefaultAccount), BarMS: previous.BarMS, simulOpen: previous.SimulOpen, simulOpenSt: previous.Strategies}
	limits := sharedEntryLimits{MaxOpen: gate.maxOpenOrders(), MaxSimul: gate.maxSimulOpen()}
	if previous.AccountLimits != nil && *previous.AccountLimits != limits {
		return false, errors.New("biz: shared account TS admission limits changed; explicit policy update required")
	}
	policyLimits := previous.PolicyLimits
	if policyLimits == nil {
		policyLimits = make(map[string]sharedEntryLimits)
	}
	declared := policies[req.StratName]
	if declared == nil && m.deps.Strategies != nil {
		if strategy := m.deps.Strategies.Get(exs.Symbol, req.StratName); strategy != nil {
			declared = strategy.Policy
		}
	}
	if saved, exists := policyLimits[req.StratName]; exists {
		if declared != nil && saved != (sharedEntryLimits{MaxOpen: declared.MaxOpen, MaxSimul: declared.MaxSimulOpen}) {
			return false, errors.New("biz: shared strategy TS admission limits changed; explicit policy update required")
		}
		policies[req.StratName] = &config.RunPolicyConfig{MaxOpen: saved.MaxOpen, MaxSimulOpen: saved.MaxSimul}
	} else if declared != nil {
		policyLimits[req.StratName] = sharedEntryLimits{MaxOpen: declared.MaxOpen, MaxSimul: declared.MaxSimulOpen}
	}
	if gate.simulOpenSt == nil {
		gate.simulOpenSt = make(map[string]int)
	}
	barMS := m.deps.Clock.TimeMS()
	if secs, err := utils.TFToSecSafe(tf); err == nil && secs > 0 {
		if barMS >= 100000000000 {
			barMS = utils.AlignTfMSecs(barMS, int64(secs)*1000)
		} else {
			barMS = barMS / (int64(secs) * 1000) * (int64(secs) * 1000)
		}
	}
	// Short fixture clocks and irregular events still have a valid first round.
	barMS = max(int64(1), barMS)
	allowed, _ := gate.allowOrderEnterWithCounts(exs, tf, []*strat.EnterReq{req}, openNum, counts, policies, barMS)
	if len(allowed) == 0 {
		return false, nil
	}
	state.Admission = sharedEntryAdmission{BarMS: gate.BarMS, SimulOpen: gate.simulOpen, Strategies: gate.simulOpenSt, AccountLimits: &limits, PolicyLimits: policyLimits}
	return true, nil
}

func (m *SharedOrderMgr) ExitOpenOrders(pairs string, req *strat.ExitReq) ([]*ormo.InOutOrder, *errs.Error) {
	return m.exitOpenOrders(pairs, req, "")
}

func (m *SharedOrderMgr) exitOpenOrders(pairs string, req *strat.ExitReq, timeframe string) ([]*ormo.InOutOrder, *errs.Error) {
	if req == nil {
		return nil, sharedBridgeError(errors.New("biz: missing shared exit request"))
	}
	if err := validateLegacyNumbers(req); err != nil {
		return nil, sharedBridgeError(err)
	}
	if (req.OrderType != core.OrderTypeEmpty && req.OrderType != core.OrderTypeMarket && req.OrderType != core.OrderTypeLimit && req.OrderType != core.OrderTypeLimitMaker) || req.ExitRate > 1 || req.OrderID < 0 || (core.IsLimitOrder(req.OrderType) && req.Limit <= 0) || (req.Dirt != core.OdDirtBoth && req.Dirt != core.OdDirtLong && req.Dirt != core.OdDirtShort) || req.FilledOnly && req.UnFillOnly {
		return nil, sharedBridgeError(errors.New("biz: invalid or unsupported shared exit request"))
	}
	var ids []int64
	err := m.mutate(func(s *SharedAccount, state *sharedTSCheckpoint, snapshot execution.AccountSnapshot, now, serial int64) error {
		commandKey := ""
		if req.CommandID != "" {
			commandKey = req.StratName + "/exit/" + req.CommandID
		}
		command, commandErr := sharedCommand(state, commandKey, struct {
			Pairs, TimeFrame string
			Request          strat.ExitReq
		}{pairs, timeframe, *req})
		if errors.Is(commandErr, errSharedCommandReplay) {
			ids = command.Orders
			return commandErr
		}
		if commandErr != nil {
			return commandErr
		}
		// Legacy direct-order exits bypass the close delay. Derive that flag
		// after checking the original command content; never mutate the caller.
		forceExit := req.Force || req.OrderID > 0
		defaults := makeRuntimeOrderConfig(m.deps, m.deps.DefaultAccount)
		pairSet := map[string]bool{}
		for _, symbol := range strings.Split(pairs, ",") {
			if symbol != "" {
				pairSet[symbol] = true
			}
		}
		var matches []*sharedTSOrder
		views := map[int64]*ormo.InOutOrder{}
		capacity := func(od *sharedTSOrder) (int64, int64) {
			var filled int64
			for _, lot := range snapshot.Lots {
				if lot.Strategy == od.Strategy && lot.ID == od.Lot {
					filled = lot.SignedSteps
					if filled < 0 {
						filled = -filled
					}
				}
			}
			pending := max(int64(0), od.Entry.QuantitySteps-filled)
			if od.Canceled {
				pending = 0
			}
			return filled, pending
		}
		remaining := decimal.Zero
		for _, od := range state.Orders {
			if len(pairSet) > 0 && !pairSet[od.Symbol] || req.StratName != "" && req.StratName != od.StrategyName || timeframe != "" && timeframe != od.TimeFrame || req.OrderID != 0 && req.OrderID != od.ID || req.EnterTag != "" && req.EnterTag != od.Request.Tag || req.Dirt == core.OdDirtLong && od.Request.Short || req.Dirt == core.OdDirtShort && !od.Request.Short {
				continue
			}
			filled, pending := capacity(od)
			signedFilled := filled
			if od.Request.Short {
				signedFilled = -signedFilled
			}
			if filled+pending == 0 || req.FilledOnly && filled == 0 || req.UnFillOnly && pending == 0 || od.Exit != nil && signedFilled != od.Desired {
				continue
			}
			instrument := m.config.Instruments[od.Symbol]
			amount := decimal.NewFromInt(filled + pending).Mul(instrument.QuantityStep)
			remaining = remaining.Add(amount)
			views[od.ID] = &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: od.ID, EnterAt: od.CreatedMS, InitPrice: od.InitPrice}, Enter: &ormo.ExOrder{Amount: amount.InexactFloat64(), Filled: decimal.NewFromInt(filled).Mul(instrument.QuantityStep).InexactFloat64()}}
			matches = append(matches, od)
		}
		useRate := req.ExitRate > 0 && req.ExitRate < 1
		if useRate {
			remaining = remaining.Mul(decimal.NewFromFloat(req.ExitRate))
		} else if req.Amount > 0 {
			remaining = decimal.NewFromFloat(req.Amount)
		}
		isTakeProfit := false
		if req.Limit > 0 && core.IsLimitOrder(req.OrderType) && len(matches) > 0 {
			for _, od := range matches[1:] {
				if od.Symbol != matches[0].Symbol {
					return errors.New("biz: ExitReq.Limit invalid for multi pairs")
				}
			}
			quote, err := m.quote(s, m.config.Instruments[matches[0].Symbol].ID, now)
			if err != nil {
				return err
			}
			price := quote.Bid
			if req.Dirt == core.OdDirtShort {
				price = quote.Ask
			}
			isTakeProfit = decimal.NewFromFloat(req.Limit).Sub(price).Mul(decimal.NewFromInt(int64(req.Dirt))).IsPositive()
		}
		sort.Slice(matches, func(a, b int) bool {
			return compareExitOpenOrders(views[matches[a].ID], views[matches[b].ID], isTakeProfit || req.FilledOnly) < 0
		})
		for _, od := range matches {
			if !remaining.IsPositive() {
				break
			}
			if !forceExit {
				filled, _ := capacity(od)
				if filled > 0 && od.TimeFrame != "ws" {
					entryAt, err := sharedLotEntryTime(s.Store(), od)
					if err != nil {
						return err
					}
					if float64(now-entryAt) <= float64(utils.TFToSecs(od.TimeFrame)*1000)*0.9 {
						continue
					}
				}
			}
			localReq := *req
			localReq.Force = forceExit
			localReq.Amount = remaining.InexactFloat64()
			localReq.ExitRate = 0
			instrument := m.config.Instruments[od.Symbol]
			q, err := m.quote(s, instrument.ID, now)
			if err != nil {
				return err
			}
			if localReq.OrderType == core.OrderTypeEmpty && defaults.orderType != "" {
				localReq.OrderType = slices.Index(core.OrderTypeEnums, defaults.orderType)
				if localReq.OrderType < core.OrderTypeEmpty || localReq.OrderType > core.OrderTypeLimitMaker {
					return errors.New("biz: unsupported configured shared exit style")
				}
				if core.IsLimitOrder(localReq.OrderType) && localReq.Limit == 0 {
					price := q.Ask
					if od.Request.Short {
						price = q.Bid
					}
					if m.config.DefaultLimitPrice != nil {
						side := execution.Sell
						if od.Request.Short {
							side = execution.Buy
						}
						value, err := s.ReadLimitPrice(m.account.Context(), m.config.DefaultLimitPrice, instrument.ID, side, defaults.limitVolSecs)
						if err != nil {
							return err
						}
						if value <= 0 {
							return errors.New("biz: execution default exit limit price unavailable")
						}
						price = decimal.NewFromFloat(value)
					}
					localReq.Limit = price.InexactFloat64()
				}
			}
			var signed int64
			for _, lot := range snapshot.Lots {
				if lot.Strategy == od.Strategy && lot.ID == od.Lot {
					signed = lot.SignedSteps
				}
			}
			filled := signed
			if filled < 0 {
				filled = -filled
			}
			pending := max(int64(0), od.Entry.QuantitySteps-filled)
			if od.Canceled {
				pending = 0
			}
			c, err := m.contextFor(s, snapshot, od.StrategyName, instrument, od.Lot, execution.VirtualIntentID(fmt.Sprintf("legacy-exit/%s/%d", od.Lot, serial)), now, q.Bar)
			if err != nil {
				return err
			}
			bridge, err := BridgeExitReq(c, &localReq, LegacyIntentLot{Selection: execution.LotSelection{Account: c.Account, Strategy: c.Strategy, Lot: c.Lot, Instrument: c.Instrument, FilledSteps: filled, PendingEntrySteps: pending}, OrderID: od.ID, EnterTag: od.Request.Tag, Short: od.Request.Short})
			if err != nil {
				return err
			}
			if !bridge.Matched {
				continue
			}
			if bridge.Reduction == nil && bridge.CancelEntrySteps == 0 {
				continue
			}
			if bridge.CancelEntrySteps > 0 || (bridge.Reduction != nil && pending > 0) {
				if err := m.cancelPending(s, snapshot, od); err != nil {
					return err
				}
				snapshot, err = s.Store().Snapshot(context.Background())
				if err != nil {
					return err
				}
				signed = 0
				for _, lot := range snapshot.Lots {
					if lot.Strategy == od.Strategy && lot.ID == od.Lot {
						signed = lot.SignedSteps
					}
				}
				filled = signed
				if filled < 0 {
					filled = -filled
				}
				od.Entry.FilledSteps = max(od.Entry.FilledSteps, filled)
				pending = max(int64(0), od.Entry.QuantitySteps-filled)
				c, err = m.contextFor(s, snapshot, od.StrategyName, instrument, od.Lot, execution.VirtualIntentID(fmt.Sprintf("legacy-exit/%s/%d", od.Lot, serial)), now, q.Bar)
				if err != nil {
					return err
				}
				bridge, err = BridgeExitReq(c, &localReq, LegacyIntentLot{Selection: execution.LotSelection{Account: c.Account, Strategy: c.Strategy, Lot: c.Lot, Instrument: c.Instrument, FilledSteps: filled, PendingEntrySteps: pending}, OrderID: od.ID, EnterTag: od.Request.Tag, Short: od.Request.Short})
				if err != nil {
					return err
				}
			}
			if bridge.CancelEntrySteps > 0 {
				remainingSteps := od.Entry.QuantitySteps - bridge.CancelEntrySteps
				// A fully unfilled cancellation retains its source quantity; the
				// terminal state, not an invalid zero-sized intent, prevents reuse.
				if remainingSteps > 0 {
					od.Entry.QuantitySteps = remainingSteps
				}
				od.Desired = filled
				if od.Request.Short {
					od.Desired = -od.Desired
				}
				if remainingSteps <= filled {
					od.Canceled = true
					od.Entry.State = execution.Canceled
				}
				if err := m.cancelPending(s, snapshot, od); err != nil {
					return err
				}
			}
			if bridge.Reduction != nil {
				if pending > bridge.CancelEntrySteps {
					if err := m.cancelPending(s, snapshot, od); err != nil {
						return err
					}
					od.ResumeEntrySteps = pending - bridge.CancelEntrySteps
				}
				od.Exit = bridge.Reduction
				od.LastProtection = ""
				od.SourceExit, od.NativeExitOrders = nil, nil
				reduction := bridge.Reduction.QuantitySteps
				od.Desired = filled - reduction
				if signed < 0 {
					od.Desired = -od.Desired
				}
				od.ExitTag = req.Tag
				od.Canceled = true
				od.Entry.State = execution.Canceled
			}
			used := bridge.CancelEntrySteps
			if bridge.Reduction != nil {
				used += bridge.Reduction.QuantitySteps
			}
			remaining = remaining.Sub(decimal.NewFromInt(used).Mul(instrument.QuantityStep))
			ids = append(ids, od.ID)
			acceptedID := req.CommandID
			if acceptedID == "" {
				acceptedID = fmt.Sprintf("legacy-exit/%d", serial)
			}
			state.Accepted = append(state.Accepted, execution.StrategyAcceptedEvent{Strategy: od.Strategy, Lot: od.Lot, Kind: execution.ExitIntent, CommandID: acceptedID, AtMS: now})
		}
		if commandKey != "" {
			command.Orders = append([]int64(nil), ids...)
			state.Commands[commandKey] = command
		}
		return nil
	})
	if err != nil {
		return nil, sharedBridgeError(err)
	}
	if err := m.project(false); err != nil {
		return nil, sharedBridgeError(err)
	}
	var result []*ormo.InOutOrder
	orders, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
	for _, id := range ids {
		lock.Lock()
		missing := orders[id] == nil
		lock.Unlock()
		if missing {
			if err := m.projectSnapshot(false, id); err != nil {
				return nil, sharedBridgeError(err)
			}
		}
	}
	lock.Lock()
	for _, id := range ids {
		if od := orders[id]; od != nil {
			result = append(result, od)
		}
	}
	lock.Unlock()
	m.pruneClosedFacades()
	return result, nil
}

func (m *SharedOrderMgr) cancelPending(s *SharedAccount, snapshot execution.AccountSnapshot, od *sharedTSOrder) error {
	return m.cancelKind(s, snapshot, od, execution.EntryIntent)
}

func sharedLotEntryTime(store *execution.Store, od *sharedTSOrder) (int64, error) {
	side := execution.Buy
	if od.Request.Short {
		side = execution.Sell
	}
	at, err := store.LotEntryTime(context.Background(), od.Strategy, od.Lot, side)
	if at == 0 {
		at = od.CreatedMS
	}
	return at, err
}

func (m *SharedOrderMgr) cancelKind(s *SharedAccount, snapshot execution.AccountSnapshot, od *sharedTSOrder, kind execution.IntentKind) error {
	for _, order := range snapshot.Orders {
		matched := false
		for _, a := range order.Intent.Allocations {
			if a.Strategy == od.Strategy && a.Lot == od.Lot && a.Kind == kind {
				matched = true
			}
		}
		if matched {
			if err := s.ExecutorFor(m.account.Context()).Cancel(order.Intent.ID, m.deps.Clock.TimeMS()); err != nil {
				return err
			}
			refreshed, err := s.Store().Order(context.Background(), order.Intent.ID)
			if err != nil {
				return err
			}
			if refreshed.State != execution.OrderCanceled && refreshed.State != execution.OrderFilled && refreshed.State != execution.OrderRejected {
				return errors.New("biz: pending entry cancellation unconfirmed")
			}
		}
	}
	return nil
}
func (m *SharedOrderMgr) ExitOrder(od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error) {
	if od == nil || req == nil {
		return nil, sharedBridgeError(errors.New("biz: missing shared lot exit"))
	}
	copy := *req
	copy.OrderID = od.ID
	copy.StratName = od.Strategy
	ods, err := m.ExitOpenOrders(od.Symbol, &copy)
	if len(ods) > 0 {
		return ods[0], err
	}
	return od, err
}
func (m *SharedOrderMgr) ExitAndFill(orders []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
	for _, od := range orders {
		if _, err := m.ExitOrder(od, req); err != nil {
			return err
		}
	}
	return nil
}
func (m *SharedOrderMgr) RelayOrders(orders []*ormo.InOutOrder) *errs.Error {
	for _, source := range orders {
		if source == nil || source.IOrder == nil || source.Enter == nil {
			return sharedBridgeError(errors.New("biz: incomplete relay source"))
		}
		amount := source.Enter.Amount
		if source.Exit != nil {
			amount -= source.Exit.Filled
		}
		if amount <= 0 {
			continue
		}
		style := slices.Index(core.OrderTypeEnums, source.Enter.OrderType)
		if style < core.OrderTypeEmpty || style > core.OrderTypeLimitMaker {
			return sharedBridgeError(errors.New("biz: unsupported relay order style"))
		}
		req := &strat.EnterReq{CommandID: fmt.Sprintf("relay/%d/%d", source.TaskID, source.ID), Tag: source.EnterTag, StratName: source.Strategy, Short: source.Short, Amount: amount, Leverage: source.Leverage, OrderType: style}
		if core.IsLimitOrder(style) {
			req.Limit = source.Enter.Price
		}
		if trigger := source.GetStopLoss(); trigger != nil && trigger.ExitTrigger != nil {
			req.StopLoss, req.StopLossLimit, req.StopLossRate, req.StopLossTag = trigger.Price, trigger.Limit, trigger.Rate, trigger.Tag
		}
		if trigger := source.GetTakeProfit(); trigger != nil && trigger.ExitTrigger != nil {
			req.TakeProfit, req.TakeProfitLimit, req.TakeProfitRate, req.TakeProfitTag = trigger.Price, trigger.Limit, trigger.Rate, trigger.Tag
		}
		req.CallbackPct, req.ActivationPrice = source.GetInfoFloat64(ormo.OdInfoCallbackPct), source.GetInfoFloat64(ormo.OdInfoActivePrice)
		info := map[string]any{}
		for key, value := range source.Info {
			if !strings.HasPrefix(key, "shared_") {
				info[key] = value
			}
		}
		od, err := m.enterOrder(&orm.ExSymbol{ID: int32(source.Sid), Symbol: source.Symbol}, source.Timeframe, req, info, true)
		if err != nil {
			return err
		}
		if od == nil {
			continue
		}
	}
	return nil
}
func (m *SharedOrderMgr) EditOrder(od *ormo.InOutOrder, action string) {
	if od == nil {
		return
	}
	err := m.mutate(func(s *SharedAccount, state *sharedTSCheckpoint, snapshot execution.AccountSnapshot, _ int64, serial int64) error {
		binding, ok := m.config.Strategies[od.Strategy]
		if !ok {
			return errors.New("biz: shared edit strategy is undeclared")
		}
		record := state.Orders[sharedOrderKey(binding.ID, od.ID)]
		if record == nil {
			return errors.New("biz: shared edit lot is unmapped")
		}
		switch action {
		case ormo.OdInfoStopLoss:
			trigger := od.GetStopLoss()
			record.Request.StopLossVal = 0
			record.Request.StopLoss, record.Request.StopLossLimit, record.Request.StopLossRate, record.Request.StopLossTag = 0, 0, 0, ""
			if trigger != nil && trigger.ExitTrigger != nil {
				record.Request.StopLoss, record.Request.StopLossLimit, record.Request.StopLossRate, record.Request.StopLossTag = trigger.Price, trigger.Limit, trigger.Rate, trigger.Tag
			}
			delete(record.Protections, "stop_loss")
			delete(record.ProtectionLevels, "stop_loss")
			delete(record.ProtectionDone, "stop_loss")
		case ormo.OdInfoTakeProfit:
			trigger := od.GetTakeProfit()
			record.Request.TakeProfitVal = 0
			record.Request.TakeProfit, record.Request.TakeProfitLimit, record.Request.TakeProfitRate, record.Request.TakeProfitTag = 0, 0, 0, ""
			if trigger != nil && trigger.ExitTrigger != nil {
				record.Request.TakeProfit, record.Request.TakeProfitLimit, record.Request.TakeProfitRate, record.Request.TakeProfitTag = trigger.Price, trigger.Limit, trigger.Rate, trigger.Tag
			}
			delete(record.Protections, "take_profit")
			delete(record.ProtectionLevels, "take_profit")
			delete(record.ProtectionDone, "take_profit")
		case ormo.OdActionLimitEnter:
			if od.Enter == nil || od.Enter.Price <= 0 {
				return errors.New("biz: shared limit edit missing entry")
			}
			if err := m.cancelPending(s, snapshot, record); err != nil {
				return err
			}
			record.Request.Limit = od.Enter.Price
			record.Entry.Conditions.Limit = decimal.NewFromFloat(od.Enter.Price)
			record.Entry.ID = execution.VirtualIntentID(fmt.Sprintf("legacy-entry-edit/%s/%d", record.Lot, serial))
			record.Entry.State = execution.PendingCondition
		case ormo.OdActionLimitExit:
			if od.Exit == nil || od.Exit.Price <= 0 || record.Exit == nil {
				return errors.New("biz: shared limit edit missing exit")
			}
			if err := m.cancelKind(s, snapshot, record, execution.ExitIntent); err != nil {
				return err
			}
			refreshed, err := s.Store().Snapshot(m.account.Context())
			if err != nil {
				return err
			}
			var remaining int64
			for _, lot := range refreshed.Lots {
				if lot.Strategy == record.Strategy && lot.ID == record.Lot {
					remaining = lot.SignedSteps - record.Desired
				}
			}
			if remaining < 0 {
				remaining = -remaining
			}
			record.Exit.QuantitySteps, record.Exit.FilledSteps = remaining, 0
			record.Exit.Conditions.Limit = decimal.NewFromFloat(od.Exit.Price)
			record.Exit.ID = execution.VirtualIntentID(fmt.Sprintf("legacy-exit-edit/%s/%d", record.Lot, serial))
			record.Exit.State = execution.PendingCondition
			record.NativeExitOrders = nil
		case ormo.OdActionTrailing:
			record.Request.CallbackPct = od.GetInfoFloat64(ormo.OdInfoCallbackPct)
			record.Request.ActivationPrice = od.GetInfoFloat64(ormo.OdInfoActivePrice)
			delete(record.Protections, "trailing")
			delete(record.ProtectionDone, "trailing")
		default:
			return fmt.Errorf("biz: unsupported shared edit action %s", action)
		}
		return validateLegacyNumbers(&record.Request)
	})
	if err == nil {
		err = m.project(false)
	}
	if err != nil {
		m.failClosed(err, fmt.Sprintf("edit/%d/%s", od.ID, action))
	}
}

func (m *SharedOrderMgr) failClosed(cause error, source string) {
	hash := sha256.Sum256([]byte(source + "/" + cause.Error()))
	freezeErr := m.account.FreezeFailure("shared-failure/"+hex.EncodeToString(hash[:16]), m.deps.Clock.TimeMS())
	m.mu.Lock()
	m.lastError = errors.Join(cause, freezeErr)
	m.mu.Unlock()
}
func (m *SharedOrderMgr) LastError() error     { m.mu.Lock(); defer m.mu.Unlock(); return m.lastError }
func (m *SharedOrderMgr) CleanUp() *errs.Error { m.account.Release(); return nil }
func (m *SharedOrderMgr) OnEnvEnd(evt *orm.DataSeries) *errs.Error {
	if evt == nil {
		return nil
	}
	exs := orm.ResolveSeriesExSymbolWithSymbolState(m.deps.Symbols, evt)
	if exs == nil || exs.Symbol == "" {
		return sharedBridgeError(errors.New("biz: environment end requires a resolved symbol"))
	}
	_, err := m.ExitOpenOrders(exs.Symbol, &strat.ExitReq{
		Tag:  core.ExitTagEnvEnd,
		Dirt: core.OdDirtBoth,
	})
	return err
}

// UpdateByDataSeries evaluates software protections at one actually observable
// execution quote; OHLC high/low ranges never trigger fills or protection.
func (m *SharedOrderMgr) UpdateByDataSeries(_ []*ormo.InOutOrder, evt *orm.DataSeries) *errs.Error {
	if evt == nil {
		return nil
	}
	if _, err := m.account.ObservePendingFeed(m.account.Context(), execution.PendingFeedObservation{
		Feed: execution.PendingFeedID(sharedFeedKey(evt.Sid, evt.TimeFrame)), EndMS: evt.EndMS, Closed: evt.Closed, WarmUp: evt.IsWarmUp,
	}); err != nil {
		return sharedBridgeError(err)
	}
	if err := m.account.WithState(func(s *SharedAccount) error {
		now := m.deps.Clock.TimeMS()
		for _, instrument := range m.config.Instruments {
			quote, err := m.quote(s, instrument.ID, now)
			if err != nil {
				return err
			}
			if err := s.AdvancePaperQuote(m.account.Context(), instrument.ID, quote, s.SendTime(now)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return sharedBridgeError(err)
	}
	err := m.mutate(func(s *SharedAccount, state *sharedTSCheckpoint, snapshot execution.AccountSnapshot, now, serial int64) error {
		quotes := make(map[string]execution.VisibleQuote)
		var requests []execution.PendingProtectionRequest
		for _, od := range state.Orders {
			if od.SID != evt.Sid {
				continue
			}
			instrument, ok := m.config.Instruments[od.Symbol]
			if !ok {
				return fmt.Errorf("biz: pending order instrument %s is not configured", od.Symbol)
			}
			q, err := m.quote(s, instrument.ID, now)
			if err != nil {
				return err
			}
			now = s.SendTime(now)
			quotes[instrument.ID] = q
			requests = append(requests, execution.PendingProtectionRequest{Order: &od.PendingOrder, Instrument: instrument, Specs: sharedProtectionSpecs(od.Request), Policy: m.config.OrderPolicy})
		}
		triggered, err := s.AdvancePendingProtections(m.account.Context(), requests, quotes, now)
		if err != nil {
			return err
		}
		for _, trigger := range triggered {
			for _, od := range state.Orders {
				if od.Strategy != trigger.Strategy || od.Lot != trigger.Lot {
					continue
				}
				od.SourceExit = nil
				od.ExitTag = string(trigger.Kind)
				if trigger.Kind == execution.StopLossProtection && od.Request.StopLossTag != "" {
					od.ExitTag = od.Request.StopLossTag
				}
				if trigger.Kind == execution.TakeProfitProtection && od.Request.TakeProfitTag != "" {
					od.ExitTag = od.Request.TakeProfitTag
				}
			}
		}
		return nil
	})
	return sharedBridgeError(errors.Join(err, m.project(false)))
}

type sharedProjectionFeeAllocation struct {
	side execution.OrderSide
	kind execution.IntentKind
}

// Resolve only one bounded page of fee-only rows from immutable native order
// allocations. The page is discarded after replay, rather than retaining a
// correction map for the entire pending history.
func sharedProjectionCorrections(store *execution.Store, page []execution.CommittedEvent, through int64) (map[int64]sharedProjectionFeeAllocation, error) {
	corrections := map[int64]sharedProjectionFeeAllocation{}
	for _, event := range page {
		if event.Checkpoint > through {
			break
		}
		var order *execution.StoredOrder
		for _, entry := range event.Ledger {
			if entry.Kind != "FeeCorrection" {
				continue
			}
			if order == nil {
				var report execution.FillReport
				if err := json.Unmarshal(event.Payload, &report); err != nil {
					return nil, err
				}
				stored, err := store.Order(context.Background(), report.OrderID)
				if err != nil {
					return nil, err
				}
				order = &stored
			}
			var resolved sharedProjectionFeeAllocation
			for _, allocation := range order.Intent.Allocations {
				if allocation.Strategy != entry.Strategy || allocation.Lot != entry.Lot {
					continue
				}
				if allocation.Side != order.Intent.Side || allocation.Kind != execution.EntryIntent && allocation.Kind != execution.ExitIntent {
					return nil, errors.New("biz: invalid fee correction allocation")
				}
				if resolved.kind != "" && resolved.kind != allocation.Kind {
					return nil, errors.New("biz: ambiguous fee correction allocation")
				}
				resolved = sharedProjectionFeeAllocation{allocation.Side, allocation.Kind}
			}
			if resolved.kind == "" {
				return nil, errors.New("biz: fee correction allocation missing")
			}
			corrections[entry.ID] = resolved
		}
	}
	return corrections, nil
}

// project reconstructs compatibility rows from committed attribution. Restore
// advances past old events without invoking trade-generating strategy callbacks.
func (m *SharedOrderMgr) project(restore bool) error {
	m.mu.Lock()
	if m.projecting {
		m.projectionPending = true
		m.mu.Unlock()
		return nil
	}
	m.projecting = true
	m.projectionPending = false
	m.mu.Unlock()
	released := false
	defer func() {
		if !released {
			m.mu.Lock()
			m.projecting = false
			m.projectionPending = false
			m.mu.Unlock()
		}
	}()
	for {
		err := m.projectSnapshot(restore, 0)
		m.mu.Lock()
		if err != nil || !m.projectionPending {
			m.projecting = false
			m.projectionPending = false
			released = true
			m.mu.Unlock()
			return err
		}
		m.projectionPending = false
		m.mu.Unlock()
		// A callback may commit another order/fill while this captured snapshot is
		// being replayed. Drain that tail from a fresh snapshot after this pass.
		restore = false
	}
}

func (m *SharedOrderMgr) projectSnapshot(restore bool, orderID int64) error {
	var state sharedTSCheckpoint
	var snapshot execution.AccountSnapshot
	var startCursor int64
	var projected []*ormo.InOutOrder
	projectedFees := map[int64]decimal.Decimal{}
	projectedExitFees := map[int64]decimal.Decimal{}
	var reconciled bool
	type projectionDelta struct {
		held, buy, sell   int64
		fees              decimal.Decimal
		buyFees, sellFees decimal.Decimal
	}
	unread := map[[2]string]projectionDelta{}
	var strategyIDs []string
	for _, binding := range m.config.Strategies {
		strategyIDs = append(strategyIDs, string(binding.ID))
	}
	sort.Strings(strategyIDs)
	identity, _ := json.Marshal(strategyIDs)
	identityHash := sha256.Sum256(identity)
	name := "legacy-projection/" + m.config.Version + "/" + hex.EncodeToString(identityHash[:8])
	// A software-only pending entry can expire without a venue order or fill
	// event. Its compact checkpoint has already dropped the terminal key, but
	// the current open facade must still observe that final metadata once.
	var facadeKeys []string
	if orderID == 0 {
		orders, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
		lock.Lock()
		for _, od := range orders {
			if od == nil || od.Status >= ormo.InOutStatusFullExit {
				continue
			}
			if binding, ok := m.config.Strategies[od.Strategy]; ok {
				facadeKeys = append(facadeKeys, sharedOrderKey(binding.ID, od.ID))
			}
		}
		lock.Unlock()
	}
	err := m.account.WithState(func(s *SharedAccount) error {
		reconciled = s.Reconciled()
		var err error
		state, err = loadSharedCheckpoint(s.Store(), m.account.Context(), m.config.Version)
		if err != nil {
			return err
		}
		for _, key := range facadeKeys {
			if _, err := state.loadOrder(key); err != nil {
				return err
			}
		}
		if orderID != 0 {
			for _, binding := range m.config.Strategies {
				_, err := state.loadOrder(sharedOrderKey(binding.ID, orderID))
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
			}
		}
		snapshot, err = s.Store().Snapshot(context.Background())
		if err != nil {
			return err
		}
		if err := m.restoreTakeovers(m.account.Context(), s.Store(), &state, snapshot); err != nil {
			return err
		}
		cursor, err := s.Store().ProjectionCursor(context.Background(), name)
		if err != nil {
			return err
		}
		startCursor = cursor
		if restore {
			if err := s.Store().AdvanceProjection(context.Background(), name, snapshot.Checkpoint); err != nil {
				return err
			}
		} else if orderID == 0 {
			// First pass computes the exact pending baseline while retaining only a
			// page and per-lot totals. The captured checkpoint bounds both passes.
			for cursor < snapshot.Checkpoint {
				page, err := s.Store().EventsAfter(context.Background(), cursor, 512)
				if err != nil {
					return err
				}
				if len(page) == 0 {
					return errors.New("biz: committed projection page missing")
				}
				corrections, err := sharedProjectionCorrections(s.Store(), page, snapshot.Checkpoint)
				if err != nil {
					return err
				}
				for _, event := range page {
					if event.Checkpoint > snapshot.Checkpoint {
						break
					}
					if err := m.loadProjectionOrders(&state, event); err != nil {
						return err
					}
					for _, entry := range event.Ledger {
						key := [2]string{string(entry.Strategy), string(entry.Lot)}
						delta := unread[key]
						delta.held += entry.QuantityDelta
						delta.fees = delta.fees.Add(entry.Fee)
						if entry.QuantityDelta > 0 {
							delta.buy += entry.QuantityDelta
							delta.buyFees = delta.buyFees.Add(entry.Fee)
						} else if entry.QuantityDelta < 0 {
							delta.sell -= entry.QuantityDelta
							delta.sellFees = delta.sellFees.Add(entry.Fee)
						} else if corrections[entry.ID].side == execution.Buy {
							delta.buyFees = delta.buyFees.Add(entry.Fee)
						} else if corrections[entry.ID].side == execution.Sell {
							delta.sellFees = delta.sellFees.Add(entry.Fee)
						}
						unread[key] = delta
					}
				}
				cursor = min(snapshot.Checkpoint, page[len(page)-1].Checkpoint)
			}
		}
		for _, record := range state.Orders {
			if orderID != 0 && record.ID != orderID {
				continue
			}
			binding, ok := m.config.Strategies[record.StrategyName]
			if !ok || binding.ID != record.Strategy {
				continue
			}
			lot, err := s.Store().Lot(context.Background(), record.Strategy, record.Lot)
			if errors.Is(err, sql.ErrNoRows) {
				lot = execution.VirtualLot{Instrument: m.config.Instruments[record.Symbol]}
			} else if err != nil {
				return err
			}
			amount := decimal.NewFromInt(record.Entry.QuantitySteps).Mul(lot.Instrument.QuantityStep).InexactFloat64()
			filled := lot.SignedSteps
			if filled < 0 {
				filled = -filled
			}
			entered, err := sharedEnteredSteps(s.Store(), m.account.Context(), record, m.config.Instruments[record.Symbol])
			if err != nil {
				return err
			}
			entered = max(entered, filled)
			quantity := decimal.NewFromInt(entered).Mul(lot.Instrument.QuantityStep).InexactFloat64()
			od := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: record.ID, TaskID: m.deps.Orders.GetTaskID(m.deps.DefaultAccount), Symbol: record.Symbol, Sid: int64(record.SID), Timeframe: record.TimeFrame, Short: record.Request.Short, Strategy: record.StrategyName, EnterTag: record.Request.Tag, ExitTag: record.ExitTag, Leverage: record.Request.Leverage, Profit: lot.RealizedPnL.Sub(lot.Fees).InexactFloat64()}, Enter: &ormo.ExOrder{Enter: true, Symbol: record.Symbol, Amount: amount, Filled: quantity, Fee: lot.Fees.InexactFloat64(), FeeQuote: lot.Fees.InexactFloat64()}, Info: map[string]interface{}{"shared_lot": string(record.Lot), "shared_checkpoint": snapshot.Checkpoint}}
			od.Enter.Side = banexg.OdSideBuy
			od.InitPrice, od.EnterAt, od.Enter.CreateAt, od.Enter.Price = record.InitPrice, record.CreatedMS, record.CreatedMS, record.Request.Limit
			if entered > 0 {
				od.Enter.UpdateAt, err = sharedLotEntryTime(s.Store(), record)
				if err != nil {
					return err
				}
			}
			od.Stop = record.Request.Stop
			od.Enter.OrderType = core.OrderTypeEnums[record.Request.OrderType]
			if record.SourceTaskID != 0 {
				od.TaskID = record.SourceTaskID
			}
			for key, value := range record.SourceInfo {
				od.Info[key] = value
			}
			od.Info["shared_lot"], od.Info["shared_checkpoint"] = string(record.Lot), snapshot.Checkpoint
			od.Info["execution_protection"] = record.ProtectionPlacement
			// Restore the read facade from durable protection definitions without
			// firing edit callbacks or submitting orders during projection.
			for _, kind := range []string{"stop_loss", "take_profit"} {
				price, limit, rate, tag, key := record.Request.StopLoss, record.Request.StopLossLimit, record.Request.StopLossRate, record.Request.StopLossTag, ormo.OdInfoStopLoss
				if kind == "take_profit" {
					price, limit, rate, tag, key = record.Request.TakeProfit, record.Request.TakeProfitLimit, record.Request.TakeProfitRate, record.Request.TakeProfitTag, ormo.OdInfoTakeProfit
				}
				if trigger, ok := record.Protections[kind]; ok {
					limit = trigger.Conditions.Limit.InexactFloat64()
					if kind == "stop_loss" {
						price = trigger.Conditions.Stop.InexactFloat64()
					} else if level, ok := record.ProtectionLevels[kind]; ok {
						price = level.InexactFloat64()
					}
				}
				if price > 0 {
					od.Info[key] = &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: price, Limit: limit, Rate: rate, Tag: tag}, Hit: record.ProtectionDone[kind] || record.Protections[kind].Triggered}
				}
			}
			od.Info[ormo.OdInfoCallbackPct] = record.Request.CallbackPct
			od.Info[ormo.OdInfoActivePrice] = record.Request.ActivationPrice
			if record.Request.Short {
				od.Enter.Side = banexg.OdSideSell
			}
			if filled > 0 {
				od.Status = ormo.InOutStatusFullEnter
				if filled < record.Entry.QuantitySteps {
					od.Status = ormo.InOutStatusPartEnter
				}
				od.Enter.Average = lot.CostBasis.Div(decimal.NewFromInt(filled).Mul(lot.Instrument.QuantityStep).Mul(lot.Instrument.ContractSize)).InexactFloat64()
			}
			if record.Canceled && filled < entered {
				od.Status = ormo.InOutStatusPartExit
				od.Exit = &ormo.ExOrder{Symbol: record.Symbol, Enter: false, Amount: quantity, Filled: decimal.NewFromInt(entered - filled).Mul(lot.Instrument.QuantityStep).InexactFloat64()}
			}
			if record.Exit != nil && od.Exit == nil {
				od.Exit = &ormo.ExOrder{Symbol: record.Symbol, Enter: false, Amount: decimal.NewFromInt(record.Exit.QuantitySteps).Mul(lot.Instrument.QuantityStep).InexactFloat64(), Price: record.Exit.Conditions.Limit.InexactFloat64()}
			}
			if record.Canceled && filled == 0 {
				od.Status = ormo.InOutStatusFullExit
				od.Exit = &ormo.ExOrder{Symbol: record.Symbol, Enter: false, Amount: amount, Filled: amount}
				if entered == 0 {
					od.Status = ormo.InOutStatusDelete
					od.Exit.Filled = 0
				}
			}
			if od.Exit != nil {
				od.Exit.Side = banexg.OdSideSell
				if record.Request.Short {
					od.Exit.Side = banexg.OdSideBuy
				}
				if record.Exit != nil {
					od.Exit.Price = record.Exit.Conditions.Limit.InexactFloat64()
					od.Exit.OrderType = banexg.OdTypeMarket
					if record.Exit.Conditions.Limit.IsPositive() {
						od.Exit.OrderType = banexg.OdTypeLimit
					}
					if record.Exit.Conditions.PostOnly {
						od.Exit.OrderType = banexg.OdTypeLimitMaker
					}
				}
			}
			if record.SourceExit != nil {
				exit := *record.SourceExit
				exit.Filled = decimal.NewFromInt(max(int64(0), entered-filled)).Mul(lot.Instrument.QuantityStep).InexactFloat64()
				feeDelta := lot.Fees.Sub(record.SourceLotFees)
				projectedExitFees[od.ID] = decimal.NewFromFloat(exit.FeeQuote).Add(feeDelta)
				exit.FeeQuote = projectedExitFees[od.ID].InexactFloat64()
				if exit.FeeType == "" || exit.FeeType == lot.Instrument.SettlementCurrency {
					exit.Fee = decimal.NewFromFloat(exit.Fee).Add(feeDelta).InexactFloat64()
				}
				if record.Exit != nil && record.Exit.State == execution.Canceled || exit.Filled >= exit.Amount {
					exit.Status = ormo.OdStatusClosed
				}
				od.Exit = &exit
			}
			projectedFees[od.ID] = lot.Fees
			od.Info["shared_signed_steps"] = lot.SignedSteps
			od.Info["shared_entry_steps"] = entered
			od.BindState(m.deps.Orders)
			projected = append(projected, od)
		}
		return nil
	})
	if err != nil {
		return err
	}
	orders, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
	lock.Lock()
	for _, od := range projected {
		if existing := orders[od.ID]; existing != nil {
			*existing = *od
		} else {
			orders[od.ID] = od
		}
	}
	lock.Unlock()
	if orderID != 0 {
		return nil
	}
	if reconciled {
		m.deps.Orders.SetSyncStamp(m.deps.DefaultAccount, max(int64(1), m.deps.Clock.TimeMS()))
	}
	m.mu.Lock()
	jobs := make([]*strat.StratJob, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, job)
	}
	m.mu.Unlock()
	for _, job := range jobs {
		var long, short []*ormo.InOutOrder
		for _, od := range projected {
			if od.Strategy == job.Strat.Name && od.Symbol == job.Symbol.Symbol && od.Timeframe == job.TimeFrame && od.Status < ormo.InOutStatusFullExit {
				if od.Short {
					short = append(short, od)
				} else {
					long = append(long, od)
				}
			}
		}
		job.SetProjectedOrders(long, short)
	}
	held := map[int64]int64{}
	entered := map[int64]int64{}
	fees := map[int64]decimal.Decimal{}
	exitFees := map[int64]decimal.Decimal{}
	for _, od := range projected {
		delta := unread[[2]string{string(m.config.Strategies[od.Strategy].ID), od.Info["shared_lot"].(string)}]
		held[od.ID] = od.Info["shared_signed_steps"].(int64) - delta.held
		entered[od.ID] = od.Info["shared_entry_steps"].(int64)
		if od.Short {
			entered[od.ID] -= delta.sell
		} else {
			entered[od.ID] -= delta.buy
		}
		fees[od.ID] = projectedFees[od.ID].Sub(delta.fees)
		if _, hasSourceFees := projectedExitFees[od.ID]; hasSourceFees {
			pendingFees := delta.sellFees
			if od.Short {
				pendingFees = delta.buyFees
			}
			exitFees[od.ID] = projectedExitFees[od.ID].Sub(pendingFees)
		}
	}
	// Replay bounded pages outside account admission so callbacks can commit.
	cursor := startCursor
	for !restore && cursor < snapshot.Checkpoint {
		var page []execution.CommittedEvent
		var corrections map[int64]sharedProjectionFeeAllocation
		err := m.account.WithState(func(s *SharedAccount) error {
			var err error
			page, err = s.Store().EventsAfter(context.Background(), cursor, 512)
			if err != nil {
				return err
			}
			corrections, err = sharedProjectionCorrections(s.Store(), page, snapshot.Checkpoint)
			return err
		})
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return errors.New("biz: committed projection page missing")
		}
		for _, event := range page {
			if event.Checkpoint > snapshot.Checkpoint {
				break
			}
			if event.Kind == "StrategyAccepted" {
				var accepted execution.StrategyAcceptedEvent
				if err := json.Unmarshal(event.Payload, &accepted); err != nil {
					return err
				}
				for _, od := range projected {
					if accepted.Strategy != m.config.Strategies[od.Strategy].ID || string(accepted.Lot) != od.Info["shared_lot"] {
						continue
					}
					visible := od.Clone()
					visible.Info["shared_event_id"] = event.ID
					instrument := m.config.Instruments[od.Symbol]
					visible.Enter.Filled = decimal.NewFromInt(entered[od.ID]).Mul(instrument.QuantityStep).InexactFloat64()
					if entered[od.ID] == 0 {
						visible.Enter.UpdateAt, visible.Enter.Average = 0, 0
						visible.Enter.Fee, visible.Enter.FeeQuote = 0, 0
						visible.Status = ormo.InOutStatusInit
					}
					kind := strat.OdChgEnter
					if accepted.Kind == execution.ExitIntent {
						kind = strat.OdChgExit
					}
					m.dispatchOrderEvent(visible, kind, event.ID)
				}
			}
			if event.Kind == "OrderState" {
				var changed execution.OrderStateEvent
				if err := json.Unmarshal(event.Payload, &changed); err != nil {
					return err
				}
				if changed.State == execution.OrderAcknowledged || changed.State == execution.OrderPartial || changed.State == execution.OrderCanceled || changed.State == execution.OrderRejected {
					seen := map[int64]bool{}
					for _, allocation := range changed.Allocations {
						for _, od := range projected {
							if seen[od.ID] || allocation.Strategy != m.config.Strategies[od.Strategy].ID || string(allocation.Lot) != od.Info["shared_lot"] {
								continue
							}
							seen[od.ID] = true
							visible := od.Clone()
							visible.Info["shared_event_id"], visible.Info["shared_order_state"] = event.ID, string(changed.State)
							visible.Info["shared_real_order_id"] = changed.OrderID
							visible.Enter.Filled = decimal.NewFromInt(entered[od.ID]).Mul(m.config.Instruments[od.Symbol].QuantityStep).InexactFloat64()
							m.dispatchOrderEvent(visible, strat.OdChgOrderChanged, event.ID+"/"+string(allocation.Lot))
						}
					}
				}
			}
			if !restore && (event.Kind == "ExchangeFill" || event.Kind == "InternalFill") {
				for _, entry := range event.Ledger {
					for _, od := range projected {
						if entry.Strategy == m.config.Strategies[od.Strategy].ID && string(entry.Lot) == od.Info["shared_lot"] && (entry.QuantityDelta != 0 || entry.Kind == "FeeCorrection") {
							fees[od.ID] = fees[od.ID].Add(entry.Fee)
							if entry.QuantityDelta == 0 {
								if corrections[entry.ID].kind == execution.ExitIntent {
									exitFees[od.ID] = exitFees[od.ID].Add(entry.Fee)
								}
								continue
							}
							held[od.ID] += entry.QuantityDelta
							isEntry := entry.QuantityDelta > 0 != od.Short
							if isEntry {
								units := entry.QuantityDelta
								if units < 0 {
									units = -units
								}
								entered[od.ID] += units
							} else {
								exitFees[od.ID] = exitFees[od.ID].Add(entry.Fee)
							}
							visible := od.Clone()
							visible.Info["shared_event_id"] = event.ID
							visible.Info["shared_event_fee"] = entry.Fee.String()
							visible.Info["shared_cumulative_fee"] = fees[od.ID].String()
							visible.Enter.Fee, visible.Enter.FeeQuote = fees[od.ID].InexactFloat64(), fees[od.ID].InexactFloat64()
							instrument := m.config.Instruments[od.Symbol]
							visible.Enter.Filled = decimal.NewFromInt(entered[od.ID]).Mul(instrument.QuantityStep).InexactFloat64()
							if isEntry {
								visible.Enter.UpdateAt = entry.AtMS
							} else if visible.Exit != nil {
								visible.Exit.UpdateAt = entry.AtMS
							}
							remaining := held[od.ID]
							if remaining < 0 {
								remaining = -remaining
							}
							exited := max(int64(0), entered[od.ID]-remaining)
							visible.Status = ormo.InOutStatusFullEnter
							if visible.Enter.Filled < visible.Enter.Amount {
								visible.Status = ormo.InOutStatusPartEnter
							}
							if exited > 0 {
								visible.Status = ormo.InOutStatusPartExit
								if visible.Exit == nil {
									visible.Exit = &ormo.ExOrder{Symbol: visible.Symbol, Amount: visible.Enter.Filled}
								}
								visible.Exit.Filled = decimal.NewFromInt(exited).Mul(instrument.QuantityStep).InexactFloat64()
								if finalExitFees, hasSourceFees := projectedExitFees[od.ID]; hasSourceFees {
									if visible.Exit.FeeType == "" || visible.Exit.FeeType == instrument.SettlementCurrency {
										visible.Exit.Fee = decimal.NewFromFloat(visible.Exit.Fee).Add(exitFees[od.ID].Sub(finalExitFees)).InexactFloat64()
									}
									visible.Exit.FeeQuote = exitFees[od.ID].InexactFloat64()
								}
								visible.Exit.Status = ormo.OdStatusPartOK
								if visible.Exit.Filled >= visible.Exit.Amount {
									visible.Exit.Status = ormo.OdStatusClosed
								}
								if remaining == 0 {
									visible.Status = ormo.InOutStatusFullExit
								}
							}
							if m.callback != nil {
								m.callback(visible, isEntry)
							}
							kind := strat.OdChgEnterFill
							if entry.QuantityDelta > 0 == od.Short {
								kind = strat.OdChgExitFill
							}
							m.dispatchOrderEvent(visible, kind, fmt.Sprintf("%s/%d", event.ID, entry.ID))
						}
					}
				}
			}
			if err := m.account.AdvanceProjection(context.Background(), name, event.Checkpoint); err != nil {
				return err
			}
			cursor = event.Checkpoint
		}
	}
	m.pruneClosedFacades()
	return nil
}

func (m *SharedOrderMgr) dispatchOrderEvent(od *ormo.InOutOrder, kind int, eventID string) {
	m.mu.Lock()
	job := m.jobs[sharedJobIdentity{od.Strategy, od.Symbol, od.Timeframe}]
	m.mu.Unlock()
	dispatch := func() {
		strat.FireOdChangeWithState(m.deps.Strategies, m.deps.DefaultAccount, od, kind)
		if job != nil && job.Strat.OnOrderChange != nil {
			job.Strat.OnOrderChange(job, od, kind)
		}
	}
	if job != nil {
		job.WithOrderEvent(eventID, dispatch)
	} else {
		dispatch()
	}
}

func (m *SharedOrderMgr) ProcessOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	if job == nil || !job.BeginOrderProcessing() {
		return nil, nil, nil
	}
	finished := false
	defer func() {
		if !finished {
			job.EndOrderProcessing()
		}
	}()
	if job.Strat != nil && job.Symbol != nil {
		m.mu.Lock()
		m.jobs[sharedJobIdentity{job.Strat.Name, job.Symbol.Symbol, job.TimeFrame}] = job
		m.mu.Unlock()
	}
	var entries, exits []*ormo.InOutOrder
	for {
		entReqs, exitReqs := job.DrainOrderRequests()
		if len(entReqs) == 0 && len(exitReqs) == 0 {
			if job.FinishOrderProcessing() {
				finished = true
				break
			}
			continue
		}
		for _, req := range entReqs {
			copy := *req
			if copy.StratName == "" && job.Strat != nil {
				copy.StratName = job.Strat.Name
			}
			od, err := m.EnterOrder(job.Symbol, job.TimeFrame, &copy)
			if err != nil {
				return entries, exits, err
			}
			if od != nil {
				entries = append(entries, od)
			}
		}
		for _, req := range exitReqs {
			copy := *req
			if copy.StratName == "" && job.Strat != nil {
				copy.StratName = job.Strat.Name
			}
			ods, err := m.exitOpenOrders(job.Symbol.Symbol, &copy, job.TimeFrame)
			if err != nil {
				return entries, exits, err
			}
			exits = append(exits, ods...)
		}
	}
	return entries, exits, nil
}

func (m *SharedOrderMgr) coordinate(s *SharedAccount, state *sharedTSCheckpoint, snapshot execution.AccountSnapshot, now int64) (execution.CombinedRebalance, error) {
	var result execution.CombinedRebalance
	requests := map[string]*execution.InstrumentRebalance{}
	marks := map[string]decimal.Decimal{}
	for _, instrument := range m.config.Instruments {
		q, err := m.quote(s, instrument.ID, now)
		if err != nil {
			return result, err
		}
		now = s.SendTime(now)
		marks[instrument.ID] = q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2))
		requests[instrument.ID] = &execution.InstrumentRebalance{Instrument: instrument, Quote: q}
	}
	for _, request := range requests {
		if request.Quote.ValidUntilMS <= now {
			return result, errors.New("biz: legacy quote expired during collection")
		}
	}
	quotes := make(map[string]execution.VisibleQuote, len(requests))
	for id, request := range requests {
		quotes[id] = request.Quote
	}
	definitions, err := m.pendingDefinitions(state)
	if err != nil {
		return result, err
	}
	records := make(map[sharedPendingKey]*sharedTSOrder, len(state.Orders))
	for _, od := range state.Orders {
		records[sharedPendingKey{od.Strategy, od.Lot}] = od
	}
	list, err := s.CoordinatePending(m.account.Context(), definitions, quotes, now)
	if err != nil {
		return result, err
	}
	for _, definition := range definitions {
		key := sharedPendingKey{definition.Order.Strategy, definition.Order.Lot}
		if record := records[key]; record != nil {
			record.PendingOrder = definition.Order
		} else {
			state.carriedPending[key] = definition
		}
	}

	body, _ := json.Marshal(state)
	hash := sha256.Sum256(body)
	risk := m.config.Risk
	risk.Marks = marks
	return execution.CombinedRebalance{PlanID: "legacy-" + hex.EncodeToString(hash[:16]), DecisionMS: now, ExpiresMS: math.MaxInt64, Requests: list, Risk: risk}, nil
}

func (m *SharedOrderMgr) pendingDefinitions(state *sharedTSCheckpoint) ([]execution.PendingDefinition, error) {
	// The account book can contain adopted lots before BindJobs creates their
	// facade metadata. Retain their coordinated state across this mutation.
	if state.carriedPending == nil {
		state.carriedPending = make(map[sharedPendingKey]execution.PendingDefinition)
		if state.store != nil {
			definitions, err := state.store.LoadPendingBook(state.ctx)
			if err != nil {
				return nil, err
			}
			snapshot, err := state.store.Snapshot(state.ctx)
			if err != nil {
				return nil, err
			}
			live := make(map[sharedPendingKey]bool)
			for _, lot := range snapshot.Lots {
				if lot.SignedSteps != 0 {
					live[sharedPendingKey{lot.Strategy, lot.ID}] = true
				}
			}
			for _, order := range snapshot.Orders {
				if order.State == execution.OrderFilled || order.State == execution.OrderCanceled || order.State == execution.OrderRejected {
					continue
				}
				for _, allocation := range order.Intent.Allocations {
					live[sharedPendingKey{allocation.Strategy, allocation.Lot}] = true
				}
			}
			for _, definition := range definitions {
				order := definition.Order
				key := sharedPendingKey{order.Strategy, order.Lot}
				entryActive := !order.Canceled && order.Entry.State != execution.Filled && order.Entry.State != execution.Canceled && order.Entry.State != execution.Expired
				if order.Desired != 0 || order.ResumeEntrySteps != 0 || entryActive || live[key] {
					state.carriedPending[key] = definition
				}
			}
		}
	}
	var definitions []execution.PendingDefinition
	for _, order := range state.Orders {
		delete(state.carriedPending, sharedPendingKey{order.Strategy, order.Lot})
		if sharedEmptyCanceledEntry(order) {
			continue
		}
		instrument, ok := m.config.Instruments[order.Symbol]
		if !ok {
			return nil, fmt.Errorf("biz: pending instrument %s is not configured", order.Symbol)
		}
		definitions = append(definitions, execution.PendingDefinition{Order: order.PendingOrder, Instrument: instrument, Specs: sharedProtectionSpecs(order.Request), Policy: m.config.OrderPolicy})
	}
	for _, definition := range state.carriedPending {
		definitions = append(definitions, definition)
	}
	return definitions, nil
}

// Older facade checkpoints could retain a zero-sized source after an entirely
// unfilled cancellation. Keep its history, but never admit it as an intent.
func sharedEmptyCanceledEntry(order *sharedTSOrder) bool {
	return order.Canceled && order.Entry.State == execution.Canceled && order.Entry.QuantitySteps == 0 &&
		order.Entry.FilledSteps == 0 && order.Entry.ReservedSteps == 0 && order.Desired == 0 &&
		order.ResumeEntrySteps == 0 && order.Exit == nil && len(order.NativeExitOrders) == 0
}
