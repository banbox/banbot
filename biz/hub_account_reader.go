package biz

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

const (
	// hubAccountReaderTTL bounds how often one runtime asks the Hub for its
	// account: strategy callbacks and per-bar equity observation share a read.
	hubAccountReaderTTL = time.Second
	// hubAccountReaderTimeout bounds one refresh (Snapshot plus quotes).
	hubAccountReaderTimeout = 3 * time.Second
)

// HubAccountReader is a READ-ONLY view of a Hub account for a runtime that has
// no time-series order manager (a factor-only Hub robot). It only issues
// Snapshot and VisibleQuote queries; it never sends commands, claims writers or
// keeps a journal. Reads are cached for a short TTL so metrics reads cannot
// hammer the Hub.
type HubAccountReader struct {
	client  execution.HubPort
	account execution.AccountKey
	bridge  *SharedOrderBridgeConfig // only Risk is used (margin rate, marks)

	mu     sync.Mutex
	at     time.Time
	cached strat.AccountMetrics
	calls  int64 // Hub RPC rounds performed (Snapshot rounds), for tests
	// stamp, when set, reports a value that changes whenever this robot's own
	// orders or the polled ledger cursor move; ok=false means "busy, do not
	// trust the cache". A changed stamp expires the cache before the TTL.
	stamp   func() (v [2]int64, ok bool)
	stampAt [2]int64
	nowFunc func() time.Time
}

var hubAccountReaders sync.Map // *TradingState -> *HubAccountReader

// BindHubAccountReader registers a read-only Hub account binding for a runtime
// (keyed by its trading state, the same owner used for strategy account
// metrics). The returned release function removes it. It changes nothing about
// how orders flow.
func BindHubAccountReader(deps RuntimeDeps, client execution.HubPort, account execution.AccountKey, risk execution.PortfolioRisk) (func(), error) {
	release, _, err := bindHubAccountReader(deps, client, account, risk)
	return release, err
}

// BoundHubAccountReader returns the runtime's cached Hub account reader: the
// read-only binding of a factor-only robot, or the lazily built reader of a
// time-series Hub robot. Nil when the runtime has neither.
func BoundHubAccountReader(deps RuntimeDeps) *HubAccountReader {
	if r := lookupHubAccountReader(deps); r != nil {
		return r
	}
	return hubTSAccountReader(deps)
}

func bindHubAccountReader(deps RuntimeDeps, client execution.HubPort, account execution.AccountKey, risk execution.PortfolioRisk) (func(), *HubAccountReader, error) {
	if deps.Trading == nil || hubPortNil(client) {
		return nil, nil, errors.New("biz: hub account reader requires a trading state and Hub client")
	}
	if err := account.Validate(); err != nil {
		return nil, nil, err
	}
	if deps.SharedExecution != nil || deps.HubOrderBridge != nil {
		return nil, nil, errors.New("biz: runtime already has an execution account binding")
	}
	reader := &HubAccountReader{client: client, account: account, bridge: &SharedOrderBridgeConfig{Risk: risk}, nowFunc: time.Now}
	if _, loaded := hubAccountReaders.LoadOrStore(deps.Trading, reader); loaded {
		return nil, nil, errors.New("biz: hub account reader already bound")
	}
	return func() { hubAccountReaders.CompareAndDelete(deps.Trading, reader) }, reader, nil
}

func lookupHubAccountReader(deps RuntimeDeps) *HubAccountReader {
	if deps.Trading == nil {
		return nil
	}
	if v, ok := hubAccountReaders.Load(deps.Trading); ok {
		return v.(*HubAccountReader)
	}
	return nil
}

// RPCRounds reports how many Hub refreshes (Snapshot rounds) were performed.
func (r *HubAccountReader) RPCRounds() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// Currency returns the settlement currency of the bound account.
func (r *HubAccountReader) Currency() string { return r.account.SettlementDomain }

func (r *HubAccountReader) read(deps RuntimeDeps) strat.AccountMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	var cur [2]int64
	trusted := true
	if r.stamp != nil {
		cur, trusted = r.stamp()
	}
	if trusted && !r.at.IsZero() && r.nowFunc().Sub(r.at) < hubAccountReaderTTL && (r.stamp == nil || cur == r.stampAt) {
		return r.cached
	}
	r.cached = r.fetch(deps)
	r.at, r.stampAt = r.nowFunc(), cur
	if !trusted {
		r.at = time.Time{} // never serve a read taken while a command was in flight
	}
	return r.cached
}

func (r *HubAccountReader) fetch(deps RuntimeDeps) strat.AccountMetrics {
	r.calls++
	parent := context.Background()
	if deps.Core != nil && deps.Core.Context() != nil {
		parent = deps.Core.Context()
	}
	ctx, cancel := context.WithTimeout(parent, hubAccountReaderTimeout)
	defer cancel()
	snapshot, err := r.client.Snapshot(ctx, r.account)
	if err != nil {
		return strat.AccountMetrics{}
	}
	marks := make(map[string]decimal.Decimal)
	for _, pos := range snapshot.ActualPositions {
		if pos.SignedSteps == 0 {
			continue
		}
		if _, exists := marks[pos.Instrument.ID]; exists {
			continue
		}
		if q, qErr := r.client.VisibleQuote(ctx, r.account, pos.Instrument.ID); qErr == nil &&
			q.Bid.IsPositive() && q.Ask.GreaterThanOrEqual(q.Bid) {
			marks[pos.Instrument.ID] = q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2))
		}
	}
	return snapshotToAccountMetrics(deps, snapshot, marks, r.bridge)
}

// hubTSReaders caches one reader per time-series Hub runtime (keyed by its
// trading state, like hubAccountReaders). The reader is rebuilt if the
// runtime's bridge config pointer changes.
var hubTSReaders sync.Map // *TradingState -> *hubTSReaderEntry

type hubTSReaderEntry struct {
	cfg    *HubOrderBridgeConfig
	reader *HubAccountReader
}

// hubTSAccountReader returns the cached, timeout-bounded reader of a
// time-series Hub robot. Strategy equity reads and the per-bar equity
// observation share one Snapshot round per hubAccountReaderTTL; one round is
// bounded by hubAccountReaderTimeout. Sub-second callbacks (OnTick) may thus see
// account metrics up to 1s old, bars of 1m or longer are unaffected.
func hubTSAccountReader(deps RuntimeDeps) *HubAccountReader {
	cfg := deps.HubOrderBridge
	if cfg == nil || hubPortNil(cfg.Client) {
		return nil
	}
	build := func() *HubAccountReader {
		return &HubAccountReader{client: cfg.Client, account: cfg.Account, bridge: &cfg.Bridge, nowFunc: time.Now,
			stamp: func() ([2]int64, bool) { return hubOrderStamp(deps.Trading) }}
	}
	if deps.Trading == nil {
		return build() // no stable owner to key on: single uncached fetch
	}
	if v, ok := hubTSReaders.Load(deps.Trading); ok {
		if e := v.(*hubTSReaderEntry); e.cfg == cfg {
			return e.reader
		}
	}
	e := &hubTSReaderEntry{cfg: cfg, reader: build()}
	actual, _ := hubTSReaders.LoadOrStore(deps.Trading, e)
	if a := actual.(*hubTSReaderEntry); a.cfg == cfg {
		return a.reader
	}
	hubTSReaders.Store(deps.Trading, e)
	return e.reader
}

// hubOrderStamp sums the command serial and ledger cursor of the runtime's Hub
// order managers, so the account cache expires as soon as this robot sends a
// command or applies a polled change (a fill by another writer). While a
// manager is busy with a command it reports ok=false.
func hubOrderStamp(state *TradingState) (v [2]int64, ok bool) {
	for _, mgr := range GetAllOdMgrWithState(state) {
		m, isHub := mgr.(*HubOrderMgr)
		if !isHub {
			continue
		}
		if !m.mu.TryLock() {
			return v, false
		}
		v[0] += m.journal.Serial
		v[1] += m.journal.Cursor
		m.mu.Unlock()
	}
	return v, true
}
