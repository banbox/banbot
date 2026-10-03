package biz

import (
	"errors"
	"math"
	"reflect"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// IntentBridgeContext is supplied by the owning strategy/account. Quantities
// are contracts/base units, QuantityStep has those same units, and notional is
// quantity*ContractSize*ReferencePrice in the strategy NAV currency. Leverage
// is retained for later risk validation; it does not multiply LegalCost.
type IntentBridgeContext struct {
	Account          execution.AccountKey
	Strategy         execution.StrategyID
	StrategyName     string
	Lot              execution.VirtualLotID
	Intent           execution.VirtualIntentID
	Instrument       string
	QuantityStep     decimal.Decimal
	ContractSize     decimal.Decimal
	ReferencePrice   decimal.Decimal
	StrategyNAV      decimal.Decimal
	StakeNAVFraction decimal.Decimal
	MaxNotional      decimal.Decimal // explicit account risk allowance, strictly positive
	NowMS            int64
	Bar              int64
}

type EntryIntentBridge struct {
	Intent              execution.EligibleIntent
	Original            strat.EnterReq
	DeferredProtections []string // retained, not yet executable or venue-native
}

func (c IntentBridgeContext) validate() error {
	if err := c.Account.Validate(); err != nil {
		return err
	}
	if c.StrategyName == "" || !c.QuantityStep.IsPositive() || !c.ContractSize.IsPositive() || !c.ReferencePrice.IsPositive() || !c.MaxNotional.IsPositive() || c.StrategyNAV.IsNegative() || c.StakeNAVFraction.IsNegative() || c.StakeNAVFraction.GreaterThan(decimal.NewFromInt(1)) || c.NowMS < 0 || c.Bar < 0 {
		return errors.New("biz: invalid intent bridge identity, precision or budget")
	}
	return nil
}

// validateLegacyNumbers rejects non-finite or negative numeric fields before
// decimal.NewFromFloat, whose input must be finite. Original requests remain
// untouched; conversion is an adapter boundary, not a strategy callback.
func validateLegacyNumbers(req any) error {
	v := reflect.ValueOf(req).Elem()
	for n := 0; n < v.NumField(); n++ {
		if v.Field(n).Kind() == reflect.Float64 {
			x := v.Field(n).Float()
			if math.IsNaN(x) || math.IsInf(x, 0) || x < 0 {
				return errors.New("biz: invalid legacy numeric field")
			}
		}
	}
	return nil
}

func (c IntentBridgeContext) intent(kind execution.IntentKind, side execution.OrderSide, steps int64, conditions execution.IntentConditions) execution.EligibleIntent {
	return execution.EligibleIntent{ID: c.Intent, Account: c.Account, Strategy: c.Strategy, Lot: c.Lot, Instrument: c.Instrument, Kind: kind, Side: side, QuantitySteps: steps, Conditions: conditions}
}

// BridgeEntryReq accepts market/limit semantics. Unsupported order styles are
// errors, not silently downgraded. StopLoss/TakeProfit/Trailing on EnterReq are
// post-entry protections, preserved explicitly for the later owner adapter.
func BridgeEntryReq(c IntentBridgeContext, req *strat.EnterReq) (*EntryIntentBridge, error) {
	if req == nil {
		return nil, errors.New("biz: nil entry request")
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	if err := validateLegacyNumbers(req); err != nil {
		return nil, err
	}
	if req.StratName != "" && req.StratName != c.StrategyName {
		return nil, errors.New("biz: entry strategy identity mismatch")
	}
	if req.OrderType != core.OrderTypeEmpty && req.OrderType != core.OrderTypeMarket && req.OrderType != core.OrderTypeLimit && req.OrderType != core.OrderTypeLimitMaker {
		return nil, errors.New("biz: unsupported entry order style")
	}
	if core.IsLimitOrder(req.OrderType) && req.Limit <= 0 || req.StopBars < 0 || req.StopLossRate > 1 || req.TakeProfitRate > 1 || req.CallbackPct >= 100 {
		return nil, errors.New("biz: invalid entry condition")
	}
	amount := decimal.NewFromFloat(req.Amount)
	var steps int64
	var err error
	unitNotional := c.QuantityStep.Mul(c.ContractSize).Mul(c.ReferencePrice)
	if amount.IsPositive() {
		steps, err = execution.QuantitySteps(amount, c.QuantityStep)
	} else {
		cost := decimal.NewFromFloat(req.LegalCost)
		if cost.IsZero() {
			rate := decimal.NewFromFloat(req.CostRate)
			if rate.IsZero() {
				rate = decimal.NewFromInt(1)
			}
			cost = c.StrategyNAV.Mul(c.StakeNAVFraction).Mul(rate)
		}
		units, _ := cost.QuoRem(unitNotional, 0)
		steps, err = execution.QuantitySteps(units, decimal.NewFromInt(1))
	}
	if err != nil {
		return nil, err
	}
	if steps <= 0 || unitNotional.Mul(decimal.NewFromInt(steps)).GreaterThan(c.MaxNotional) {
		return nil, errors.New("biz: entry quantity below step or above risk allowance")
	}
	side := execution.Buy
	if req.Short {
		side = execution.Sell
	}
	conditions := execution.IntentConditions{Limit: decimal.NewFromFloat(req.Limit), PostOnly: req.OrderType == core.OrderTypeLimitMaker, Stop: decimal.NewFromFloat(req.Stop), CreatedBar: c.Bar, StopBars: int64(req.StopBars)}
	result := &EntryIntentBridge{Intent: c.intent(execution.EntryIntent, side, steps, conditions), Original: *req}
	if req.Infos != nil {
		result.Original.Infos = make(map[string]string, len(req.Infos))
		for k, v := range req.Infos {
			result.Original.Infos[k] = v
		}
	}
	if req.StopLoss != 0 || req.StopLossVal != 0 || req.StopLossLimit != 0 || req.StopLossRate != 0 || req.StopLossTag != "" {
		result.DeferredProtections = append(result.DeferredProtections, "stop_loss")
	}
	if req.TakeProfit != 0 || req.TakeProfitVal != 0 || req.TakeProfitLimit != 0 || req.TakeProfitRate != 0 || req.TakeProfitTag != "" {
		result.DeferredProtections = append(result.DeferredProtections, "take_profit")
	}
	if req.CallbackPct != 0 || req.ActivationPrice != 0 {
		result.DeferredProtections = append(result.DeferredProtections, "trailing")
	}
	if _, err := result.Intent.Evaluate(c.ReferencePrice, c.NowMS, c.Bar); err != nil {
		return nil, err
	}
	return result, nil
}

type LegacyIntentLot struct {
	Selection execution.LotSelection
	OrderID   int64
	EnterTag  string
	Short     bool
}

type ExitIntentBridge struct {
	Original         strat.ExitReq
	Reduction        *execution.EligibleIntent
	CancelEntrySteps int64
	Matched          bool
}

// BridgeExitReq converts one explicitly mapped lot. OrderID/EnterTag/Dirt
// filter that lot; it never guesses a strategy from symbol. Amount overrides
// ExitRate per ExitReq's documented contract. Absolute amount reduces filled
// quantity first and then cancels pending entry; rates shrink each separately.
func BridgeExitReq(c IntentBridgeContext, req *strat.ExitReq, lot LegacyIntentLot) (*ExitIntentBridge, error) {
	if req == nil {
		return nil, errors.New("biz: nil exit request")
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	if err := validateLegacyNumbers(req); err != nil {
		return nil, err
	}
	if req.StratName != "" && req.StratName != c.StrategyName {
		return nil, errors.New("biz: exit strategy identity mismatch")
	}
	if req.OrderType != core.OrderTypeEmpty && req.OrderType != core.OrderTypeMarket && req.OrderType != core.OrderTypeLimit && req.OrderType != core.OrderTypeLimitMaker {
		return nil, errors.New("biz: unsupported order style exit")
	}
	if req.ExitRate > 1 || req.OrderID < 0 || (core.IsLimitOrder(req.OrderType) && req.Limit <= 0) || (req.Dirt != core.OdDirtBoth && req.Dirt != core.OdDirtLong && req.Dirt != core.OdDirtShort) {
		return nil, errors.New("biz: invalid exit request")
	}
	selection := execution.ExitSelection{Account: c.Account, Strategy: c.Strategy, Lot: c.Lot, Instrument: c.Instrument, FilledOnly: req.FilledOnly, UnFillOnly: req.UnFillOnly}
	reduce, cancel, err := selection.Select(lot.Selection)
	if err != nil {
		return nil, err
	}
	result := &ExitIntentBridge{Original: *req}
	if (req.OrderID != 0 && req.OrderID != lot.OrderID) || (req.EnterTag != "" && req.EnterTag != lot.EnterTag) || (req.Dirt == core.OdDirtLong && lot.Short) || (req.Dirt == core.OdDirtShort && !lot.Short) {
		return result, nil
	}
	result.Matched = true
	if req.Amount > 0 {
		units, err := execution.QuantitySteps(decimal.NewFromFloat(req.Amount), c.QuantityStep)
		if err != nil {
			return nil, err
		}
		reduce = min(reduce, units)
		units -= reduce
		cancel = min(cancel, units)
	} else {
		rate := decimal.NewFromFloat(req.ExitRate)
		if rate.IsZero() {
			rate = decimal.NewFromInt(1)
		}
		reduce, _ = execution.QuantitySteps(decimal.NewFromInt(reduce).Mul(rate), decimal.NewFromInt(1))
		cancel, _ = execution.QuantitySteps(decimal.NewFromInt(cancel).Mul(rate), decimal.NewFromInt(1))
	}
	result.CancelEntrySteps = cancel
	if reduce > 0 {
		side := execution.Sell
		if lot.Short {
			side = execution.Buy
		}
		intent := c.intent(execution.ExitIntent, side, reduce, execution.IntentConditions{Limit: decimal.NewFromFloat(req.Limit), PostOnly: req.OrderType == core.OrderTypeLimitMaker, CreatedBar: c.Bar})
		if _, err := intent.Evaluate(c.ReferencePrice, c.NowMS, c.Bar); err != nil {
			return nil, err
		}
		result.Reduction = &intent
	}
	return result, nil
}
