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
	"sort"
	"sync"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// HubOrderBridgeConfig contains a remote command port, never an account owner,
// private exchange adapter, or execution ledger. StatePath is a private robot
// outbox/projection journal; the Hub remains the only execution authority.
type HubOrderBridgeConfig struct {
	Client        execution.HubPort
	Account       execution.AccountKey
	Bridge        SharedOrderBridgeConfig
	WriterID      string
	ExpectedEpoch map[execution.StrategyID]uint64
	StatePath     string
	// LocalAccount marks a bridge whose Client is an in-process execution.LocalHubPort
	// serving the SAME account this runtime already borrows as SharedExecution
	// (biz.HubOrderMgr on an embedded account, execution.EmbeddedHub). It is the only case in which a
	// Hub bridge may coexist with SharedExecution; no SharedOrderMgr is built.
	// Nobody else observes public feeds then, so this manager forwards closed
	// bars to the account itself (UpdateByDataSeries).
	LocalAccount bool
}

func (c *HubOrderBridgeConfig) Validate() error {
	if c == nil || hubPortNil(c.Client) || c.WriterID == "" || !filepath.IsAbs(c.StatePath) {
		return errors.New("biz: incomplete Hub order bridge")
	}
	if c.LocalAccount {
		if _, ok := c.Client.(*execution.LocalHubPort); !ok {
			return errors.New("biz: local-account Hub bridge requires an in-process Hub port")
		}
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
	// Rejected marks a command the owner refused before committing anything.
	// It is finished: resending it unchanged would be refused forever.
	Rejected bool `json:",omitempty"`
	// Imported marks a command carried over from the legacy SharedOrderMgr
	// checkpoint (hub_order_legacy_import.go). It is finished history: a retry
	// with the same ID replays OrderIDs without comparing hashes (the facades
	// hash different encodings) and without sending, and resumeCommands never
	// resends it. OrderIDs are the legacy record's orders.
	Imported bool    `json:",omitempty"`
	OrderIDs []int64 `json:",omitempty"`
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
	// Plans/Followups hold the robot-side take-profit ladder and OCO state
	// (hub_order_ladder.go). Additive: journals without them load unchanged.
	Plans     map[string]*hubLotPlan  `json:",omitempty"`
	Followups map[string]*hubFollowup `json:",omitempty"`
	// Migrated records the one-time import of the SharedOrderMgr checkpoint
	// (hub_order_legacy_import.go). Additive and informational.
	Migrated *hubLegacyMigration `json:",omitempty"`
}

type HubOrderMgr struct {
	deps     RuntimeDeps
	config   HubOrderBridgeConfig
	callback FnOdCb
	mu       sync.Mutex
	journal  hubOrderJournal
	jobs     map[sharedJobIdentity]*strat.StratJob
	archiver orderArchiver
	pending  map[string]execution.PendingDefinition
	backoff  map[string]*hubBackoff // per-lot follow-up retry state, memory only
	// takeoverWarned dedupes takeover warnings; memory only.
	takeoverWarned map[string]bool
	lastError      error
	polling        bool
	started        bool
	delivering     bool
	pollDone       chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}
	lockFile       *os.File
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
	// A local-account bridge reads the same account the runtime borrows; any
	// other SharedExecution/SharedOrderBridge would mean a second facade.
	localOwner := cfg.LocalAccount && deps.SharedExecution != nil && deps.SharedExecution.AccountKey() == cfg.Account
	if (deps.SharedExecution != nil && !localOwner) || deps.SharedOrderBridge != nil || deps.Core == nil || deps.Core.BackTestMode || deps.Orders == nil || deps.Trading == nil || deps.Clock == nil {
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
			// First start of an embedded account: import the legacy facade state
			// before the journal exists on disk (see hub_order_legacy_import.go).
			err = m.importLegacyCheckpoint(ctx)
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
	if policy, active, e := timeSeriesLossStop(deps); e != nil {
		cancel()
		return nil, e
	} else if active {
		if e = c.Client.SetLossStopPolicy(ctx, c.Account, policy); e != nil {
			cancel()
			return nil, e
		}
	}
	// Account-level admission limits are authoritative in the owner; each robot
	// registers its own under its stable writer id and the owner keeps the most
	// restrictive of all registrants (see execution/admission_policy.go).
	if e := c.Client.SetAdmissionPolicy(ctx, c.Account, c.WriterID, timeSeriesAdmissionPolicy(deps)); e != nil {
		cancel()
		return nil, e
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
	// The owner accepts a strategy's commands only with a strictly increasing
	// sequence, so unsent frozen commands are resent in sequence order (several
	// can be pending after exits were frozen without sending).
	pending := make([]*hubFrozenCommand, 0, len(m.journal.Commands))
	for _, command := range m.journal.Commands {
		if !command.Done && !command.Imported {
			pending = append(pending, command)
		}
	}
	sort.SliceStable(pending, func(i, j int) bool { return hubCommandSequence(pending[i]) < hubCommandSequence(pending[j]) })
	for _, command := range pending {
		m.logResend(command)
		if err := m.sendLocked(command); err != nil && !errors.Is(err, errSharedEntryRejected) {
			return err
		}
	}
	return nil
}

func hubCommandSequence(c *hubFrozenCommand) uint64 {
	if c.Pending != nil {
		return c.Pending.StrategySequence
	}
	if c.Exit != nil {
		return c.Exit.StrategySequence
	}
	return 0
}

// logResend records that a restart or retry resends a frozen command, so a late
// fill from an old attempt is traceable. Age is measured from the time-series
// signal time (metadata CreatedMS); it is omitted when the command carries none.
func (m *HubOrderMgr) logResend(command *hubFrozenCommand) {
	fields := []zap.Field{zap.Int64("order_id", command.OrderID)}
	kind := "exit"
	if command.Pending != nil {
		kind = string(command.Pending.Action)
		fields = append(fields, zap.String("lot", string(command.Pending.Lot)))
		if md := command.Pending.Metadata; md != nil && md.CreatedMS > 0 {
			nowMS := time.Now().UnixMilli()
			if m.deps.Clock != nil {
				nowMS = m.deps.Clock.TimeMS()
			}
			fields = append(fields, zap.Duration("age", time.Duration(nowMS-md.CreatedMS)*time.Millisecond))
		}
	} else if command.Exit != nil {
		fields = append(fields, zap.String("strategy", string(command.Exit.Strategy)))
	}
	fields = append(fields, zap.String("kind", kind))
	m.deps.Logger().Info("hub resends a frozen command", fields...)
}
func (m *HubOrderMgr) sendLocked(command *hubFrozenCommand) error {
	var err error
	if command.Pending != nil {
		_, err = m.config.Client.ApplyPendingCommand(m.ctx, m.config.Account, *command.Pending)
	} else {
		command.Results, err = m.config.Client.ApplyTimeSeriesExit(m.ctx, m.config.Account, *command.Exit)
	}
	if errors.Is(err, execution.ErrVenueMinimum) {
		command.Done, command.Rejected = true, true
		m.deps.Logger().Warn("hub rejected a command below the venue minimum; it is dropped, not retried", zap.Int64("order_id", command.OrderID), zap.Bool("exit", command.Exit != nil))
		if saveErr := m.saveLocked(); saveErr != nil {
			return saveErr
		}
		return errSharedEntryRejected
	}
	// Only a NEW entry is dropped for the other refusals (risk/exposure/margin,
	// price tick, spot short, self-cross): the owner refused it before committing
	// anything and would refuse the same frozen command every time. An exit, edit
	// or protection change is never dropped here; it stays fail-closed.
	if code, ok := execution.RejectionCode(err); ok && command.Pending != nil && command.Pending.Action == execution.PendingCreate {
		command.Done, command.Rejected = true, true
		m.deps.Logger().Warn("hub rejected an entry; it is dropped, not retried", zap.String("reason", code), zap.Int64("order_id", command.OrderID), zap.String("lot", string(command.Pending.Lot)))
		if saveErr := m.saveLocked(); saveErr != nil {
			return saveErr
		}
		return errSharedEntryRejected
	}
	if err != nil {
		return err
	}
	command.Done = true
	return m.saveLocked()
}
func (m *HubOrderMgr) nextLocked(strategy execution.StrategyID) (string, uint64, uint64, error) {
	return m.nextSeqLocked(strategy, true)
}

// nextSeqLocked allocates the next command id and sequence. With resume it
// first retries unacknowledged frozen commands (a lost acknowledgement blocks
// newer commands); without it the caller only journals and sends nothing.
func (m *HubOrderMgr) nextSeqLocked(strategy execution.StrategyID, resume bool) (string, uint64, uint64, error) {
	w := m.journal.Writers[strategy]
	if w == nil || w.Sequence == ^uint64(0) {
		return "", 0, 0, errors.New("biz: Hub writer sequence unavailable")
	}
	if resume {
		if err := m.resumeCommands(); err != nil {
			return "", 0, 0, err
		}
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
			if cmd.Imported {
				if len(cmd.OrderIDs) == 0 {
					return 0, errSharedEntryRejected
				}
				return cmd.OrderIDs[0], nil
			}
			if cmd.Hash != hash {
				return 0, errors.New("biz: Hub command ID reused")
			}
			if !cmd.Done {
				err = m.sendLocked(cmd)
			}
			if cmd.Rejected {
				return 0, errSharedEntryRejected
			}
			return cmd.OrderID, err
		}
	}
	copy := *req
	copy.StratName = name
	defaults := makeRuntimeOrderConfig(m.deps, m.deps.DefaultAccount)
	if m.deps.Core.EnvReal && copy.Leverage != 0 && copy.Leverage != defaults.accountLeverage {
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
	roundEntryLimits(&copy, instrument.PriceTick)
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
	if _, active, _ := timeSeriesLossStop(m.deps); active {
		status, err := m.config.Client.LossStopStatus(m.ctx, m.config.Account)
		if err != nil {
			return 0, err
		}
		if err = applyLossStopPause(m.deps, status); err != nil {
			return 0, err
		}
	}
	allowed, err := admitTimeSeriesEntry(m.deps, timeSeriesJobPolicies(m.jobs, exs.Symbol, tf), &state, snapshot, exs, tf, &copy)
	if err != nil {
		return 0, err
	}
	if !allowed {
		return 0, errSharedEntryRejected
	}
	// Ladder / OCO are orchestrated by the robot from the first rung and the
	// group id; a request that cannot be expressed is refused, never dropped.
	plan, err := newHubLotPlan(&copy, q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2)).InexactFloat64())
	if err != nil {
		return 0, hubRejectLadderPlan(m, name, exs.Symbol, req.Tag, err)
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
	// 52 bits keep them exact in JSON consumers that read numbers as float64
	// (browsers): a larger ID is rounded there and no longer names the order.
	random, err := hubRandomID()
	if err != nil {
		return 0, err
	}
	var orderID int64
	for _, c := range []byte(random[:13]) {
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
	if err == nil && command.Create != nil {
		// The owner refuses an order below the venue minimum, and a refused
		// frozen command stops this robot. Size is a property of the request,
		// not of the account, so it is checked here before anything is frozen.
		steps := command.Create.QuantitySteps
		if steps < instrument.MinSteps || instrument.Notional(steps, c.ReferencePrice).LessThan(instrument.MinNotional) {
			err = errEntrySizeRejected
		}
	}
	if errors.Is(err, errEntrySizeRejected) {
		// Nothing was frozen or sent: give the unused command identity back.
		m.journal.Writers[b.ID].Sequence--
		m.journal.Serial--
		m.deps.Logger().Warn("entry rejected: size below the tradable minimum or above the strategy allowance",
			zap.String("strategy", name), zap.String("symbol", exs.Symbol), zap.String("tag", req.Tag),
			zap.String("reference_price", c.ReferencePrice.String()), zap.String("min_notional", instrument.MinNotional.String()), zap.Int64("min_steps", instrument.MinSteps))
		return 0, errSharedEntryRejected
	}
	if err != nil {
		return 0, err
	}
	command.StrategyEpoch, command.StrategySequence = epoch, sequence
	frozen := &hubFrozenCommand{Hash: hash, Pending: &command, OrderID: orderID}
	m.journal.Commands[key] = frozen
	if plan != nil {
		if m.journal.Plans == nil {
			m.journal.Plans = map[string]*hubLotPlan{}
		}
		m.journal.Plans[hubLotKey(b.ID, lot)] = plan
	}
	previousAdmission := m.journal.Admission
	m.journal.Admission = state.Admission
	if err = m.saveLocked(); err != nil {
		return 0, err
	}
	err = m.sendLocked(frozen)
	if errors.Is(err, errSharedEntryRejected) {
		// The owner refused the entry before committing anything: it must not
		// use up this bar's open-order capacity.
		m.journal.Admission = previousAdmission
		if saveErr := m.saveLocked(); saveErr != nil {
			return 0, saveErr
		}
	}
	return orderID, err
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
	if errors.Is(err, errSharedEntryRejected) {
		// The owner refused the reduction (below the venue minimum): the
		// position is unchanged and no order view is returned.
		return nil, nil
	}
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
	return m.exitLockedMode(pairs, tf, req, true)
}

// exitLockedMode is exitLocked; with send=false the exit is only frozen into the
// journal (same key, hash, CommandID and sequence as the normal path) so a
// restart resends it. A duplicate with the same hash is not frozen twice.
func (m *HubOrderMgr) exitLockedMode(pairs, tf string, req *strat.ExitReq, send bool) ([]execution.TimeSeriesExitResult, error) {
	// A stopped robot or an unsendable older frozen command must not lose this
	// exit: it is still journaled (not sent) and the cause is returned.
	var deferred error
	if send && m.lastError != nil {
		deferred, send = m.lastError, false
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
			if cmd.Imported {
				results := make([]execution.TimeSeriesExitResult, 0, len(cmd.OrderIDs))
				for _, id := range cmd.OrderIDs {
					results = append(results, execution.TimeSeriesExitResult{OrderID: id})
				}
				return results, nil
			}
			if cmd.Hash != hash {
				return nil, errors.New("biz: Hub exit ID reused")
			}
			if !cmd.Done && send {
				err = m.sendLocked(cmd)
			}
			if err == nil {
				err = deferred
			}
			return cmd.Results, err
		}
	}
	if send {
		if e := m.resumeCommands(); e != nil {
			deferred, send = e, false
		}
	}
	id, epoch, seq, err := m.nextSeqLocked(b.ID, false)
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
	if copy.Limit > 0 {
		orderSymbol := ""
		if copy.OrderID > 0 {
			if od := m.order(copy.OrderID); od != nil {
				orderSymbol = od.Symbol
			}
		}
		if tick, ok := exitTick(m.config.Bridge.Instruments, pairs, orderSymbol); ok {
			copy.Limit = roundToTick(copy.Limit, tick)
		}
	}
	command, err := BridgeTimeSeriesExitCommand(b.ID, name, pairs, tf, &copy)
	if err != nil {
		return nil, err
	}
	command.StrategyEpoch, command.StrategySequence = epoch, seq
	frozen := &hubFrozenCommand{Hash: hash, Exit: &command}
	m.journal.Commands[key] = frozen
	if err = m.saveLocked(); err == nil && send {
		err = m.sendLocked(frozen)
	}
	if err == nil {
		err = deferred
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
func (m *HubOrderMgr) UpdateByDataSeries(_ []*ormo.InOutOrder, evt *orm.DataSeries) *errs.Error {
	m.mu.Lock()
	m.started = true
	m.mu.Unlock()
	if m.config.LocalAccount && evt != nil && evt.Closed && !evt.IsWarmUp {
		// No Hub host observes this robot's feeds: persist the closed bar so
		// StopBars expiry advances, as SharedOrderMgr does for its account.
		if observer, ok := m.config.Client.(interface {
			ObservePendingFeed(context.Context, execution.AccountKey, execution.PendingFeedObservation) (bool, error)
		}); ok {
			if _, err := observer.ObservePendingFeed(m.ctx, m.config.Account, execution.PendingFeedObservation{
				Feed: execution.PendingFeedID(sharedFeedKey(evt.Sid, evt.TimeFrame)), EndMS: evt.EndMS, Closed: true,
			}); err != nil {
				return sharedBridgeError(err)
			}
		}
	}
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
			m.salvageQueued(job, nil, nil)
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
		for i, r := range ent {
			copy := *r
			if copy.StratName == "" && job.Strat != nil {
				copy.StratName = job.Strat.Name
			}
			od, err := m.EnterOrder(job.Symbol, job.TimeFrame, &copy)
			if err != nil {
				m.salvageQueued(job, ent[i+1:], ext)
				return entries, exits, err
			}
			if od != nil {
				entries = append(entries, od)
			}
		}
		for i, r := range ext {
			copy := *r
			if copy.StratName == "" && job.Strat != nil {
				copy.StratName = job.Strat.Name
			}
			ods, err := m.exit(job.Symbol.Symbol, job.TimeFrame, &copy)
			if err != nil {
				m.salvageQueued(job, nil, ext[i+1:])
				return entries, exits, err
			}
			exits = append(exits, ods...)
		}
	}
	return entries, exits, nil
}

// salvageQueued runs when ProcessOrders fails. The request that failed is
// already journaled (or never was and is the caller's error); every other
// request the strategy queued (the unprocessed rest of the drained batch plus
// anything still queued) was already acknowledged to the strategy, so exits are
// frozen into the journal without sending and a restart resends them. Entries
// are dropped with a warning: a fresh entry after a failure would be stale.
func (m *HubOrderMgr) salvageQueued(job *strat.StratJob, ent []*strat.EnterReq, ext []*strat.ExitReq) {
	qe, qx := job.DrainOrderRequests()
	ent, ext = append(ent, qe...), append(ext, qx...)
	for _, r := range ent {
		if r == nil {
			continue
		}
		m.deps.Logger().Warn("hub dropped a queued entry after an order processing failure",
			zap.String("strategy", r.StratName), zap.String("tag", r.Tag), zap.String("command_id", r.CommandID))
	}
	if len(ext) == 0 || job.Symbol == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range ext {
		if r == nil {
			continue
		}
		copy := *r
		if copy.StratName == "" && job.Strat != nil {
			copy.StratName = job.Strat.Name
		}
		if _, err := m.exitLockedMode(job.Symbol.Symbol, job.TimeFrame, &copy, false); err != nil {
			m.deps.Logger().Error("hub could not journal a queued exit after an order processing failure",
				zap.String("strategy", copy.StratName), zap.String("tag", copy.Tag), zap.String("command_id", copy.CommandID), zap.Error(err))
			continue
		}
		m.deps.Logger().Warn("hub froze a queued exit without sending after an order processing failure; it is resent on restart",
			zap.String("strategy", copy.StratName), zap.String("tag", copy.Tag), zap.String("command_id", copy.CommandID))
	}
}

// hubPortNil reports a nil port, including a typed-nil pointer stored in the
// interface (a nil *HubClient or *LocalHubPort would otherwise pass == nil).
func hubPortNil(p execution.HubPort) bool {
	switch v := p.(type) {
	case nil:
		return true
	case *execution.HubClient:
		return v == nil
	case *execution.LocalHubPort:
		return v == nil
	}
	return false
}
