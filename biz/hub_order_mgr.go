package biz

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

// HubOrderBridgeConfig contains a remote command port, never an account owner,
// private exchange adapter, or execution ledger. StatePath is a private robot
// outbox/projection journal; the Hub remains the only execution authority.
type HubOrderBridgeConfig struct {
	Client        *execution.HubClient
	Account       execution.AccountKey
	Bridge        SharedOrderBridgeConfig
	WriterID      string
	ExpectedEpoch map[execution.StrategyID]uint64
	StatePath     string
}

func (c *HubOrderBridgeConfig) Validate() error {
	if c == nil || c.Client == nil || c.WriterID == "" || !filepath.IsAbs(c.StatePath) {
		return errors.New("biz: incomplete Hub order bridge")
	}
	if err := c.Account.Validate(); err != nil {
		return err
	}
	b := c.Bridge
	b.QuoteContext = func(context.Context, string, int64) (execution.VisibleQuote, error) {
		return execution.VisibleQuote{}, nil
	}
	return b.Validate()
}

type hubWriter struct {
	Expected uint64
	Lease    execution.StrategyWriterLease
	Sequence uint64
}
type hubFrozenCommand struct {
	Hash    string
	Pending *execution.PendingCommand
	Exit    *execution.TimeSeriesExitCommand
	OrderID int64
	Results []execution.TimeSeriesExitResult
	Done    bool
}
type hubOrderJournal struct {
	OrderKinds        map[string]map[string]execution.IntentKind
	FillTotals        map[string]hubFillTotal
	PendingDeliveries []hubOrderDelivery
	Admission         sharedEntryAdmission

	Version           string
	Account           execution.AccountKey
	WriterID, Session string
	Serial            int64
	Writers           map[execution.StrategyID]*hubWriter
	Commands          map[string]*hubFrozenCommand
	Cursor            int64
	Lots              map[string]*hubProjectedLot
}

type HubOrderMgr struct {
	deps       RuntimeDeps
	config     HubOrderBridgeConfig
	callback   FnOdCb
	mu         sync.Mutex
	journal    hubOrderJournal
	jobs       map[sharedJobIdentity]*strat.StratJob
	pending    map[string]execution.PendingDefinition
	lastError  error
	polling    bool
	started    bool
	delivering bool
	pollDone   chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	lockFile   *os.File
}

func hubRandomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func NewHubOrderMgr(deps RuntimeDeps, cfg *HubOrderBridgeConfig, callback FnOdCb) (*HubOrderMgr, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if deps.SharedExecution != nil || deps.SharedOrderBridge != nil || deps.Core == nil || deps.Core.BackTestMode || deps.Orders == nil || deps.Trading == nil || deps.Clock == nil {
		return nil, errors.New("biz: Hub bridge requires isolated live/dry-run runtime dependencies")
	}
	c := *cfg
	c.Bridge.QuoteContext = func(ctx context.Context, id string, _ int64) (execution.VisibleQuote, error) {
		return c.Client.VisibleQuote(ctx, c.Account, id)
	}
	c.Bridge.DefaultLimitPrice = func(ctx context.Context, id string, side execution.OrderSide, secs int) (float64, error) {
		return c.Client.DefaultLimitPrice(ctx, c.Account, id, side, secs)
	}
	ctx, cancel := context.WithCancel(deps.Core.Context())
	m := &HubOrderMgr{deps: deps, config: c, callback: callback, jobs: map[sharedJobIdentity]*strat.StratJob{}, pending: map[string]execution.PendingDefinition{}, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	if err := os.MkdirAll(filepath.Dir(c.StatePath), 0700); err != nil {
		cancel()
		return nil, err
	}
	lock, err := os.OpenFile(c.StatePath+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		cancel()
		return nil, err
	}
	if err = lockHubJournal(lock); err != nil {
		lock.Close()
		cancel()
		return nil, fmt.Errorf("biz: Hub robot journal already in use: %w", err)
	}
	m.lockFile = lock
	success := false
	defer func() {
		if !success {
			unlockHubJournal(lock)
			lock.Close()
		}
	}()
	body, err := os.ReadFile(c.StatePath)
	fresh := errors.Is(err, os.ErrNotExist)
	if err == nil {
		err = json.Unmarshal(body, &m.journal)
		if err == nil && (m.journal.Version != c.Bridge.Version || m.journal.Account != c.Account || m.journal.WriterID != c.WriterID || m.journal.Session == "" || m.journal.Writers == nil || m.journal.Commands == nil || m.journal.Lots == nil) {
			err = errors.New("biz: Hub robot journal identity mismatch")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		session, e := hubRandomID()
		err = e
		m.journal = hubOrderJournal{Version: c.Bridge.Version, Account: c.Account, WriterID: c.WriterID, Session: session, Writers: map[execution.StrategyID]*hubWriter{}, Commands: map[string]*hubFrozenCommand{}, Lots: map[string]*hubProjectedLot{}}
		for _, b := range c.Bridge.Strategies {
			expected, explicit := c.ExpectedEpoch[b.ID]
			if !explicit {
				current, e := c.Client.WriterLease(ctx, c.Account, b.ID)
				if e != nil {
					cancel()
					return nil, e
				}
				expected = current.Epoch
			}
			m.journal.Writers[b.ID] = &hubWriter{Expected: expected}
		}
		if err == nil {
			err = m.saveLocked()
		}
	}
	if err != nil {
		cancel()
		return nil, err
	}
	for _, b := range c.Bridge.Strategies {
		writer := m.journal.Writers[b.ID]
		if writer == nil {
			cancel()
			return nil, errors.New("biz: Hub journal strategy scope changed")
		}
		lease, e := c.Client.ClaimStrategyWriter(ctx, c.Account, execution.StrategyWriterClaim{Strategy: b.ID, WriterID: c.WriterID + "/" + m.journal.Session, ExpectedEpoch: writer.Expected})
		if e != nil {
			cancel()
			return nil, e
		}
		if writer.Lease.Epoch != 0 && writer.Lease != lease {
			cancel()
			return nil, errors.New("biz: Hub writer claim changed across restart")
		}
		current, e := c.Client.WriterLease(ctx, c.Account, b.ID)
		if e != nil {
			cancel()
			return nil, e
		}
		if current.Epoch != lease.Epoch || current.Sequence > writer.Sequence {
			cancel()
			return nil, errors.New("biz: Hub journal writer was fenced or lost commands")
		}
		writer.Lease = lease
	}
	if m.journal.OrderKinds == nil {
		m.journal.OrderKinds = map[string]map[string]execution.IntentKind{}
	}
	if m.journal.FillTotals == nil {
		m.journal.FillTotals = map[string]hubFillTotal{}
	}
	if err = m.saveLocked(); err == nil {
		err = m.resumeCommands()
	}
	if err == nil {
		if fresh {
			err = m.poll(true)
		} else {
			m.mu.Lock()
			err = m.restoreViewsLocked()
			m.mu.Unlock()
		}
	}
	if err != nil {
		cancel()
		return nil, err
	}
	success = true
	go m.run()
	return m, nil
}

func (m *HubOrderMgr) saveLocked() error {
	body, err := json.Marshal(m.journal)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(m.config.StatePath), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(m.config.StatePath), ".hub-robot-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(body)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, m.config.StatePath)
	}
	if err == nil {
		var dir *os.File
		dir, err = os.Open(filepath.Dir(name))
		if err == nil {
			err = dir.Sync()
			dir.Close()
		}
	}
	return err
}
func (m *HubOrderMgr) run() {
	defer close(m.done)
	defer func() {
		if m.lockFile != nil {
			unlockHubJournal(m.lockFile)
			m.lockFile.Close()
		}
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			started := m.started
			m.mu.Unlock()
			if !started {
				if _, err := m.config.Client.Snapshot(m.ctx, m.config.Account); err != nil && m.ctx.Err() == nil {
					m.fail(err)
					return
				}
				continue
			}
			if err := m.poll(false); err != nil {
				if m.ctx.Err() != nil {
					return
				}
				m.fail(err)
			}
		}
	}
}
func (m *HubOrderMgr) fail(err error) {
	m.mu.Lock()
	m.lastError = err
	m.mu.Unlock()
	m.cancel()
	if m.deps.Core != nil {
		m.deps.Core.Stop()
	}
}
func (m *HubOrderMgr) LastError() error { m.mu.Lock(); defer m.mu.Unlock(); return m.lastError }

func (m *HubOrderMgr) CleanUp() *errs.Error { m.cancel(); <-m.done; return nil }

func (m *HubOrderMgr) resumeCommands() error {
	for _, command := range m.journal.Commands {
		if !command.Done {
			if err := m.sendLocked(command); err != nil {
				return err
			}
		}
	}
	return nil
}
func (m *HubOrderMgr) sendLocked(command *hubFrozenCommand) error {
	var err error
	if command.Pending != nil {
		_, err = m.config.Client.ApplyPendingCommand(m.ctx, m.config.Account, *command.Pending)
	} else {
		command.Results, err = m.config.Client.ApplyTimeSeriesExit(m.ctx, m.config.Account, *command.Exit)
	}
	if err != nil {
		return err
	}
	command.Done = true
	return m.saveLocked()
}
func (m *HubOrderMgr) nextLocked(strategy execution.StrategyID) (string, uint64, uint64, error) {
	w := m.journal.Writers[strategy]
	if w == nil || w.Sequence == ^uint64(0) {
		return "", 0, 0, errors.New("biz: Hub writer sequence unavailable")
	}
	// A lost acknowledgement blocks newer commands until its frozen body is retried.
	if err := m.resumeCommands(); err != nil {
		return "", 0, 0, err
	}
	w.Sequence++
	m.journal.Serial++
	return fmt.Sprintf("ts/%s/%d", m.journal.Session, m.journal.Serial), w.Lease.Epoch, w.Sequence, nil
}
func (m *HubOrderMgr) strategy(name string) (string, SharedStrategyBinding, error) {
	if name == "" && len(m.config.Bridge.Strategies) == 1 {
		for n := range m.config.Bridge.Strategies {
			name = n
		}
	}
	b, ok := m.config.Bridge.Strategies[name]
	if !ok {
		return "", b, errors.New("biz: ambiguous Hub strategy")
	}
	return name, b, nil
}
func (m *HubOrderMgr) EnterOrder(exs *orm.ExSymbol, tf string, req *strat.EnterReq) (*ormo.InOutOrder, *errs.Error) {
	return m.enter(exs, tf, req, nil)
}
func (m *HubOrderMgr) enter(exs *orm.ExSymbol, tf string, req *strat.EnterReq, source map[string]any) (*ormo.InOutOrder, *errs.Error) {
	if exs == nil || req == nil {
		return nil, sharedBridgeError(errors.New("biz: missing Hub entry"))
	}
	m.mu.Lock()
	m.started = true
	id, err := m.enterLocked(exs, tf, req, source)
	m.mu.Unlock()
	if errors.Is(err, errSharedEntryRejected) {
		return nil, nil
	}
	if err == nil {
		err = m.poll(false)
	}
	if err != nil {
		return nil, sharedBridgeError(err)
	}
	od := m.order(id)
	if od == nil {
		m.mu.Lock()
		for _, cmd := range m.journal.Commands {
			if cmd.OrderID == id && cmd.Pending != nil && cmd.Pending.Metadata != nil {
				k := hubLotKey(cmd.Pending.Strategy, cmd.Pending.Lot)
				lot := m.journal.Lots[k]
				if lot == nil {
					lot = &hubProjectedLot{Record: execution.TimeSeriesProjectionRecord{TimeSeriesLotKey: execution.TimeSeriesLotKey{Strategy: cmd.Pending.Strategy, Lot: cmd.Pending.Lot}, Metadata: *cmd.Pending.Metadata}}
					m.journal.Lots[k] = lot
				}

				od, err = m.viewLocked(lot, true)
				if err == nil {
					orders, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
					lock.Lock()
					orders[id] = od
					lock.Unlock()
				}
				break
			}
		}
		m.mu.Unlock()
	}
	if err != nil {
		return nil, sharedBridgeError(err)
	}
	return od, nil
}
func (m *HubOrderMgr) enterLocked(exs *orm.ExSymbol, tf string, req *strat.EnterReq, source map[string]any) (int64, error) {
	if m.lastError != nil {
		return 0, m.lastError
	}
	name, b, err := m.strategy(req.StratName)
	if err != nil {
		return 0, err
	}
	instrument, ok := m.config.Bridge.Instruments[exs.Symbol]
	if !ok {
		return 0, errors.New("biz: undeclared Hub symbol")
	}
	hash, err := sharedHash(struct {
		Symbol, TF string
		Request    strat.EnterReq
		Source     map[string]any
	}{exs.Symbol, tf, *req, source})
	if err != nil {
		return 0, err
	}
	key := string(b.ID) + "/entry/" + req.CommandID
	if req.CommandID != "" {
		if cmd := m.journal.Commands[key]; cmd != nil {
			if cmd.Hash != hash {
				return 0, errors.New("biz: Hub command ID reused")
			}
			if !cmd.Done {
				err = m.sendLocked(cmd)
			}
			return cmd.OrderID, err
		}
	}
	copy := *req
	copy.StratName = name
	defaults := makeRuntimeOrderConfig(m.deps, m.deps.DefaultAccount)
	if m.deps.Core.EnvReal && (copy.Leverage != 0 || defaults.accountLeverage != 0) {
		return 0, errors.New("biz: Hub leverage changes are unsupported")
	}
	if copy.Leverage == 0 {
		gate := OrderMgr{runtimeDeps: true, runtimeCfg: defaults}
		copy.Leverage = gate.accountLeverage()
	}
	defaultStyle := copy.OrderType == core.OrderTypeEmpty && defaults.orderType != ""
	if defaultStyle {
		copy.OrderType = slices.Index(core.OrderTypeEnums, defaults.orderType)
	}
	if copy.OrderType < core.OrderTypeEmpty || copy.OrderType > core.OrderTypeLimitMaker {
		return 0, errors.New("biz: unsupported Hub order type")
	}
	q, err := m.config.Client.VisibleQuote(m.ctx, m.config.Account, instrument.ID)
	if err != nil {
		return 0, err
	}
	if defaultStyle && core.IsLimitOrder(copy.OrderType) && copy.Limit == 0 {
		side := execution.Buy
		if copy.Short {
			side = execution.Sell
		}
		copy.Limit, err = m.config.Bridge.DefaultLimitPrice(m.ctx, instrument.ID, side, defaults.limitVolSecs)
		if err != nil {
			return 0, err
		}
	}
	if copy.Limit > 0 && copy.StopBars == 0 {
		copy.StopBars = defaults.stopEnterBars
	}
	snapshot, err := m.config.Client.Snapshot(m.ctx, m.config.Account)
	if err != nil {
		return 0, err
	}
	definitions, err := m.config.Client.PendingOrders(m.ctx, m.config.Account)
	if err != nil {
		return 0, err
	}
	state := sharedTSCheckpoint{Orders: map[string]*sharedTSOrder{}, Admission: m.journal.Admission}
	names := map[execution.StrategyID]string{}
	for n, b := range m.config.Bridge.Strategies {
		names[b.ID] = n
	}
	for _, p := range definitions {
		n := names[p.Order.Strategy]
		if n != "" {
			state.Orders[hubLotKey(p.Order.Strategy, p.Order.Lot)] = &sharedTSOrder{PendingOrder: p.Order, StrategyName: n}
		}
	}
	gate := SharedOrderMgr{deps: m.deps, jobs: m.jobs}
	allowed, err := gate.allowEntry(&state, snapshot, exs, tf, &copy)
	if err != nil {
		return 0, err
	}
	if !allowed {
		return 0, errSharedEntryRejected
	}
	nav := snapshot.SyntheticStrategyCash[b.ID]
	for _, lot := range snapshot.Lots {
		if lot.Strategy == b.ID && lot.SignedSteps != 0 {
			mark, err := m.config.Client.VisibleQuote(m.ctx, m.config.Account, lot.Instrument.ID)
			if err != nil {
				return 0, err
			}
			nav = nav.Add(lot.EquityValue(mark.Bid.Add(mark.Ask).Div(decimal.NewFromInt(2))))
		}
	}
	commandID, epoch, sequence, err := m.nextLocked(b.ID)
	if err != nil {
		return 0, err
	}
	if copy.CommandID == "" {
		copy.CommandID = commandID
		key = string(b.ID) + "/entry/" + commandID
	}
	// Positive random facade IDs avoid collisions between robots sharing an account.
	random, err := hubRandomID()
	if err != nil {
		return 0, err
	}
	var orderID int64
	for _, c := range []byte(random[:15]) {
		orderID = orderID*16 + int64(slices.Index([]byte("0123456789abcdef"), c))
	}
	orderID++
	lot := execution.VirtualLotID("ts/" + m.journal.Session + "/" + fmt.Sprint(m.journal.Serial))
	c := IntentBridgeContext{Account: m.config.Account, Strategy: b.ID, StrategyName: name, Lot: lot, Intent: execution.VirtualIntentID(commandID), Instrument: instrument.ID, QuantityStep: instrument.QuantityStep, ContractSize: instrument.ContractSize, ReferencePrice: q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2)), StrategyNAV: nav, StakeNAVFraction: b.StakeNAVFraction, MaxNotional: b.MaxNotional, NowMS: m.deps.Clock.TimeMS()}
	info := map[string]any{}
	for k, v := range req.Infos {
		info[k] = v
	}
	for k, v := range source {
		info[k] = v
	}
	command, err := BridgeTimeSeriesEntryCommand(c, exs, tf, orderID, &copy, m.config.Bridge.OrderPolicy, info)
	if err != nil {
		return 0, err
	}
	command.StrategyEpoch, command.StrategySequence = epoch, sequence
	frozen := &hubFrozenCommand{Hash: hash, Pending: &command, OrderID: orderID}
	m.journal.Commands[key] = frozen
	m.journal.Admission = state.Admission
	if err = m.saveLocked(); err != nil {
		return 0, err
	}
	return orderID, m.sendLocked(frozen)
}
func sharedHash(v any) (string, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
func (m *HubOrderMgr) order(id int64) *ormo.InOutOrder {
	ods, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
	lock.Lock()
	defer lock.Unlock()
	return ods[id]
}

func (m *HubOrderMgr) ExitOpenOrders(pairs string, req *strat.ExitReq) ([]*ormo.InOutOrder, *errs.Error) {
	return m.exit(pairs, "", req)
}
func (m *HubOrderMgr) exit(pairs, tf string, req *strat.ExitReq) ([]*ormo.InOutOrder, *errs.Error) {
	if req == nil {
		return nil, sharedBridgeError(errors.New("biz: missing Hub exit"))
	}
	m.mu.Lock()
	m.started = true
	results, err := m.exitLocked(pairs, tf, req)
	m.mu.Unlock()
	if err == nil {
		err = m.poll(false)
	}
	if err != nil {
		return nil, sharedBridgeError(err)
	}
	var ods []*ormo.InOutOrder
	for _, r := range results {
		if od := m.order(r.OrderID); od != nil {
			ods = append(ods, od)
		}
	}
	return ods, nil
}
func (m *HubOrderMgr) exitLocked(pairs, tf string, req *strat.ExitReq) ([]execution.TimeSeriesExitResult, error) {
	if m.lastError != nil {
		return nil, m.lastError
	}
	name, b, err := m.strategy(req.StratName)
	if err != nil {
		return nil, err
	}
	hash, err := sharedHash(struct {
		Pairs, TF string
		Request   strat.ExitReq
	}{pairs, tf, *req})
	if err != nil {
		return nil, err
	}
	key := string(b.ID) + "/exit/" + req.CommandID
	if req.CommandID != "" {
		if cmd := m.journal.Commands[key]; cmd != nil {
			if cmd.Hash != hash {
				return nil, errors.New("biz: Hub exit ID reused")
			}
			if !cmd.Done {
				err = m.sendLocked(cmd)
			}
			return cmd.Results, err
		}
	}
	id, epoch, seq, err := m.nextLocked(b.ID)
	if err != nil {
		return nil, err
	}
	copy := *req
	copy.StratName = name
	if copy.OrderID > 0 {
		copy.Force = true
	}
	if copy.CommandID == "" {
		copy.CommandID = id
		key = string(b.ID) + "/exit/" + id
	}
	command, err := BridgeTimeSeriesExitCommand(b.ID, name, pairs, tf, &copy)
	if err != nil {
		return nil, err
	}
	command.StrategyEpoch, command.StrategySequence = epoch, seq
	frozen := &hubFrozenCommand{Hash: hash, Exit: &command}
	m.journal.Commands[key] = frozen
	if err = m.saveLocked(); err == nil {
		err = m.sendLocked(frozen)
	}
	return frozen.Results, err
}
func (m *HubOrderMgr) ExitOrder(od *ormo.InOutOrder, req *strat.ExitReq) (*ormo.InOutOrder, *errs.Error) {
	if od == nil || req == nil {
		return nil, sharedBridgeError(errors.New("biz: missing Hub exit order"))
	}
	copy := *req
	copy.OrderID = od.ID
	copy.StratName = od.Strategy
	ods, err := m.exit(od.Symbol, od.Timeframe, &copy)
	if len(ods) > 0 {
		return ods[0], err
	}
	return nil, err
}
func (m *HubOrderMgr) ExitAndFill(ods []*ormo.InOutOrder, req *strat.ExitReq) *errs.Error {
	for _, od := range ods {
		if _, err := m.ExitOrder(od, req); err != nil {
			return err
		}
	}
	return nil
}
func (m *HubOrderMgr) RelayOrders(ods []*ormo.InOutOrder) *errs.Error {
	if len(ods) > 0 {
		return sharedBridgeError(errors.New("biz: Hub relay requires explicit execution takeover policy"))
	}
	return nil
}
func (m *HubOrderMgr) UpdateByDataSeries(_ []*ormo.InOutOrder, _ *orm.DataSeries) *errs.Error {
	m.mu.Lock()
	m.started = true
	m.mu.Unlock()
	if err := m.poll(false); err != nil {
		return sharedBridgeError(err)
	}
	return nil
}
func (m *HubOrderMgr) OnEnvEnd(row *orm.DataSeries) *errs.Error {
	if row == nil || row.IsWarmUp {
		return nil
	}
	exs := orm.ResolveSeriesExSymbolWithSymbolState(m.deps.Symbols, row)
	if exs == nil || exs.Symbol == "" {
		return sharedBridgeError(errors.New("biz: Hub environment end requires resolved symbol"))
	}
	for name := range m.config.Bridge.Strategies {
		if _, err := m.exit(exs.Symbol, "", &strat.ExitReq{StratName: name, Tag: core.ExitTagEnvEnd, Dirt: core.OdDirtBoth}); err != nil {
			return err
		}
	}
	return nil
}

func (m *HubOrderMgr) BindJobs(jobs []*strat.StratJob) error {
	pending := map[sharedJobIdentity]*strat.StratJob{}
	for _, j := range jobs {
		if j == nil || j.Strat == nil || j.Symbol == nil || j.TimeFrame == "" {
			return errors.New("biz: incomplete Hub job")
		}
		if _, ok := m.config.Bridge.Strategies[j.Strat.Name]; !ok {
			return errors.New("biz: unknown Hub job strategy")
		}
		key := sharedJobIdentity{j.Strat.Name, j.Symbol.Symbol, j.TimeFrame}
		if pending[key] != nil {
			return errors.New("biz: duplicate Hub job")
		}
		pending[key] = j
	}
	m.mu.Lock()
	for key, j := range pending {
		m.jobs[key] = j
	}
	m.started = true
	m.publishJobsLocked()
	m.mu.Unlock()
	return m.poll(false)
}

func (m *HubOrderMgr) ProcessOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
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
		m.started = true
		m.mu.Unlock()
		if err := m.poll(false); err != nil {
			return nil, nil, sharedBridgeError(err)
		}
	}
	var entries, exits []*ormo.InOutOrder
	for {
		ent, ext := job.DrainOrderRequests()
		if len(ent) == 0 && len(ext) == 0 {
			if job.FinishOrderProcessing() {
				finished = true
				break
			}
			continue
		}
		for _, r := range ent {
			copy := *r
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
		for _, r := range ext {
			copy := *r
			if copy.StratName == "" && job.Strat != nil {
				copy.StratName = job.Strat.Name
			}
			ods, err := m.exit(job.Symbol.Symbol, job.TimeFrame, &copy)
			if err != nil {
				return entries, exits, err
			}
			exits = append(exits, ods...)
		}
	}
	return entries, exits, nil
}
