package biz

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// hubTakeoverScope names the strategies whose adopted external positions this
// robot renders: only the configured take_over_strat, and only when this robot
// declares it. Everything else (including no configuration) requests nothing,
// so the default projection request is unchanged.
func (m *HubOrderMgr) hubTakeoverScope() (string, execution.StrategyID) {
	cfg := m.deps.ConfigView()
	if cfg == nil || cfg.TakeOverStrat == "" {
		return "", ""
	}
	binding, ok := m.config.Bridge.Strategies[cfg.TakeOverStrat]
	if !ok {
		return "", ""
	}
	return cfg.TakeOverStrat, binding.ID
}

// hubTakeoverOrderID is stable across restarts and journal loss: the same
// adopted lot always presents as the same order. 52 bits stay exact in JSON
// number consumers, like the random IDs of ordinary entries.
func hubTakeoverOrderID(strategy execution.StrategyID, lot execution.VirtualLotID) int64 {
	sum := sha256.Sum256([]byte("hub-takeover\x00" + string(strategy) + "\x00" + string(lot)))
	var id int64
	for _, c := range []byte(hex.EncodeToString(sum[:])[:13]) {
		id = id*16 + int64(hubHexDigit(c))
	}
	return id + 1
}

func hubHexDigit(c byte) int {
	if c >= 'a' {
		return int(c-'a') + 10
	}
	return int(c - '0')
}

// adoptTakeoversLocked renders lots the account owner adopted from external
// trades (take_over_strat) as strategy orders, like SharedOrderMgr
// restoreTakeovers: tag EnterTagThird, SourceInfo external_takeover, bound to
// the unique job of the strategy/symbol. Quantity, cost and ownership were
// committed by the owner; the robot only commits presentation metadata (so the
// regular exit can select the lot) and seeds its fill totals from the adoption
// record, since the adoption event itself was replayed without metadata.
// Callers hold m.mu and have fully replayed the page. A lot with no matching
// job yet is retried on the next poll, when jobs are bound.
func (m *HubOrderMgr) adoptTakeoversLocked(page execution.TimeSeriesProjectionPage, records map[string]execution.TimeSeriesProjectionRecord) error {
	name, id := m.hubTakeoverScope()
	if id == "" || len(page.Takeovers) == 0 {
		return nil
	}
	held := map[string]int64{}
	for _, l := range page.Snapshot.Lots {
		held[hubLotKey(l.Strategy, l.ID)] = l.SignedSteps
	}
	for _, adopted := range page.Takeovers {
		request := adopted.Attribution
		k := hubLotKey(request.Strategy, request.Lot)
		if request.Strategy != id || m.journal.Lots[k] != nil {
			continue
		}
		record, bound := records[k]
		if held[k] == 0 {
			continue // Already flat: nothing to present or close.
		}
		var symbol string
		for candidate, instrument := range m.config.Bridge.Instruments {
			if instrument.ID == request.Instrument.ID {
				symbol = candidate
				break
			}
		}
		if symbol == "" {
			m.warnTakeoverOnce(k, "adopted instrument is not declared by the Hub bridge", nil)
			continue
		}
		timeframe := ""
		if m.deps.Core != nil {
			timeframe, _ = m.deps.Core.StrategyTimeFrame(name, symbol)
		}
		var job *strat.StratJob
		candidates := 0
		for identity, candidate := range m.jobs {
			if identity.Strategy == name && identity.Symbol == symbol && (timeframe == "" || identity.TimeFrame == timeframe) {
				job = candidate
				candidates++
			}
		}
		if candidates == 0 {
			continue
		}
		if candidates != 1 || job == nil || job.Symbol == nil {
			m.warnTakeoverOnce(k, "takeover timeframe is ambiguous; configure the runtime strategy/pair timeframe", nil)
			continue
		}
		if !bound {
			instrument := m.config.Bridge.Instruments[symbol]
			quantity := absSharedSteps(request.ExpectedSignedSteps)
			price := adopted.Position.CostBasis.Div(decimal.NewFromInt(quantity).Mul(request.Instrument.QuantityStep).Mul(request.Instrument.ContractSize))
			amount := decimal.NewFromInt(quantity).Mul(instrument.QuantityStep).InexactFloat64()
			short := request.ExpectedSignedSteps < 0
			payload, err := json.Marshal(TimeSeriesEntryPayload{
				QuantitySteps: quantity, SID: int32(job.Symbol.ID),
				Request:    strat.EnterReq{StratName: name, Tag: core.EnterTagThird, Short: short, Amount: amount, OrderType: core.OrderTypeMarket},
				SourceInfo: map[string]any{"external_takeover": request.ID, "external_source_event": adopted.SourceEventID, "entry_time_is_adoption": true},
			})
			if err != nil {
				return err
			}
			metadata := execution.TimeSeriesMetadata{OrderID: hubTakeoverOrderID(request.Strategy, request.Lot), StrategyName: name, Symbol: symbol, TimeFrame: job.TimeFrame, EnterTag: core.EnterTagThird, Short: short, CreatedMS: request.AtMS, InitPrice: price, Payload: payload}
			if err = m.config.Client.BindTimeSeriesTakeover(m.ctx, m.config.Account, execution.TimeSeriesTakeoverBinding{Strategy: request.Strategy, Lot: request.Lot, Metadata: metadata}); err != nil {
				if m.ctx.Err() != nil {
					return m.ctx.Err()
				}
				m.warnTakeoverOnce(k, "owner refused the takeover binding", err)
				continue
			}
			record = execution.TimeSeriesProjectionRecord{TimeSeriesLotKey: execution.TimeSeriesLotKey{Strategy: request.Strategy, Lot: request.Lot}, Metadata: metadata}
		}
		entered := absSharedSteps(request.ExpectedSignedSteps)
		m.journal.Lots[k] = &hubProjectedLot{Record: record, Entered: entered, EntryCost: adopted.Position.CostBasis, Fees: adopted.Position.Fees, EntryMS: request.AtMS}
	}
	return m.saveLocked()
}

func (m *HubOrderMgr) warnTakeoverOnce(key, msg string, err error) {
	if m.takeoverWarned == nil {
		m.takeoverWarned = map[string]bool{}
	}
	if m.takeoverWarned[key+msg] {
		return
	}
	m.takeoverWarned[key+msg] = true
	fields := []zap.Field{zap.String("lot", key)}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	m.deps.Logger().Warn("hub takeover not rendered: "+msg, fields...)
}

func absSharedSteps(steps int64) int64 {
	if steps < 0 {
		return -steps
	}
	return steps
}
