package biz

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// TimeSeriesEntryPayload retains the strategy presentation fields without
// making execution depend on strat or orm. Runtime trigger state is not input.
type TimeSeriesEntryPayload struct {
	QuantitySteps int64 `json:",omitempty"`
	Request       strat.EnterReq
	SourceInfo    map[string]any
	SID           int32
}

// BridgeTimeSeriesEntryCommand converts an already defaulted strategy request.
// The caller must retain the resulting command, including its stable lot and
// command ID, across transport retries; it must not re-size from a newer quote.
// Account risk validation and all execution state remain in the account owner.
func BridgeTimeSeriesEntryCommand(c IntentBridgeContext, exs *orm.ExSymbol, timeframe string, orderID int64, req *strat.EnterReq, policy execution.OrderPolicy, sourceInfo map[string]any) (execution.PendingCommand, error) {
	var command execution.PendingCommand
	if exs == nil || exs.ID <= 0 || exs.Symbol == "" || timeframe == "" || orderID <= 0 || req == nil || req.CommandID == "" {
		return command, errors.New("biz: time-series command requires explicit symbol, order and stable command identities")
	}
	if err := policy.Validate(); err != nil {
		return command, err
	}
	bridged, err := BridgeEntryReq(c, req)
	if err != nil {
		return command, err
	}
	payload, err := json.Marshal(TimeSeriesEntryPayload{QuantitySteps: bridged.Intent.QuantitySteps, Request: bridged.Original, SourceInfo: sourceInfo, SID: exs.ID})
	if err != nil {
		return command, err
	}
	conditions := bridged.Intent.Conditions
	// A robot quote's Bar is not the hub's expiry clock. Feed identity/countdown
	// is bound atomically by the owner against its persisted feed progress.
	conditions.StopBars, conditions.CreatedBar = 0, 0
	create := &execution.PendingCreateSpec{Instrument: c.Instrument, Side: bridged.Intent.Side, QuantitySteps: bridged.Intent.QuantitySteps, Conditions: conditions, Policy: policy, Protections: sharedProtectionSpecs(bridged.Original)}
	if req.StopBars > 0 {
		create.Feed = execution.PendingFeedID(sharedFeedKey(exs.ID, timeframe))
		create.StopBars = int64(req.StopBars)
	}
	command = execution.PendingCommand{
		ID: req.CommandID, Strategy: c.Strategy, Lot: c.Lot, Action: execution.PendingCreate, Create: create,
		Metadata: &execution.TimeSeriesMetadata{OrderID: orderID, StrategyName: c.StrategyName, Symbol: exs.Symbol, TimeFrame: timeframe, EnterTag: req.Tag, Short: req.Short, CreatedMS: c.NowMS, InitPrice: c.ReferencePrice, Payload: payload},
	}
	return command, nil
}

// BridgeTimeSeriesExitCommand forwards the selector and one global quantity
// budget. It deliberately does not read robot order views or enumerate lots.
func BridgeTimeSeriesExitCommand(strategy execution.StrategyID, strategyName, pairs, timeframe string, req *strat.ExitReq) (execution.TimeSeriesExitCommand, error) {
	var command execution.TimeSeriesExitCommand
	if strategy == "" || strategyName == "" || req == nil || req.CommandID == "" || req.StratName != "" && req.StratName != strategyName {
		return command, errors.New("biz: time-series exit requires an unambiguous strategy and stable command ID")
	}
	if err := validateLegacyNumbers(req); err != nil {
		return command, err
	}
	if req.OrderID < 0 || req.ExitRate > 1 || req.UnFillOnly && req.FilledOnly || req.OrderType != core.OrderTypeEmpty && req.OrderType != core.OrderTypeMarket && req.OrderType != core.OrderTypeLimit && req.OrderType != core.OrderTypeLimitMaker || core.IsLimitOrder(req.OrderType) && req.Limit <= 0 {
		return command, errors.New("biz: invalid time-series exit selector or order style")
	}
	command = execution.TimeSeriesExitCommand{ID: req.CommandID, Strategy: strategy, TimeFrame: timeframe, OrderID: req.OrderID, EnterTag: req.EnterTag, FilledOnly: req.FilledOnly, UnFillOnly: req.UnFillOnly, Force: req.Force, Amount: decimal.NewFromFloat(req.Amount), Rate: decimal.NewFromFloat(req.ExitRate), ExitTag: req.Tag, Conditions: execution.IntentConditions{Limit: decimal.NewFromFloat(req.Limit), PostOnly: req.OrderType == core.OrderTypeLimitMaker}}
	switch req.Dirt {
	case core.OdDirtBoth:
	case core.OdDirtLong:
		command.Side = execution.Buy
	case core.OdDirtShort:
		command.Side = execution.Sell
	default:
		return execution.TimeSeriesExitCommand{}, errors.New("biz: invalid time-series exit direction")
	}
	for _, symbol := range strings.Split(pairs, ",") {
		if symbol != "" {
			command.Symbols = append(command.Symbols, symbol)
		}
	}
	return command, nil
}
