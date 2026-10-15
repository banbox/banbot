package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"

	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

// InstrumentFromBanexgMarket uses normalized SDK metadata only. Fixed-lattice
// linear perpetuals are supported; spot, inverse, dated and significant-digit
// markets require another valuation contract. Settlement precision is explicit
// currency metadata, never inferred from a symbol or a default money scale.
func InstrumentFromBanexgMarket(id string, market *banexg.Market, settlement *banexg.Currency) (Instrument, error) {
	var instrument Instrument
	if !canonicalID(id) || market == nil || !canonicalID(market.Symbol) || !market.Contract || !market.Linear || !market.Swap || market.Inverse || market.Spot || market.Future || market.Option || market.Expiry != 0 || market.Precision == nil || !canonicalID(market.Settle) || settlement == nil || settlement.Code != market.Settle {
		return instrument, errors.New("execution: normalized linear perpetual and matching settlement metadata required")
	}
	step, err := fixedPrecisionQuantum(market.Precision.Amount, market.Precision.ModeAmount)
	if err != nil {
		return instrument, err
	}
	tick, err := fixedPrecisionQuantum(market.Precision.Price, market.Precision.ModePrice)
	if err != nil {
		return instrument, err
	}
	moneyQuantum, err := fixedPrecisionQuantum(settlement.Precision, settlement.PrecMode)
	if err != nil {
		return instrument, err
	}
	moneyScale := int32(-moneyQuantum.Exponent())
	if moneyScale < 0 || moneyScale > 18 || !moneyQuantum.Equal(decimal.New(1, -moneyScale)) {
		return instrument, errors.New("execution: settlement precision is not a supported decimal money scale")
	}
	contractSize, err := decimalBoundary(market.ContractSize)
	if err != nil || !contractSize.IsPositive() {
		return instrument, errors.New("execution: normalized contract size is missing or invalid")
	}
	if market.Limits == nil || market.Limits.Amount == nil || market.Limits.Cost == nil {
		return instrument, errors.New("execution: normalized quantity and notional limits required")
	}
	minimumAmount, err := decimalBoundary(market.Limits.Amount.Min)
	if err != nil || minimumAmount.IsNegative() {
		return instrument, errors.New("execution: invalid normalized minimum quantity")
	}
	minimumNotional, err := decimalBoundary(market.Limits.Cost.Min)
	if err != nil || minimumNotional.IsNegative() {
		return instrument, errors.New("execution: invalid normalized minimum notional")
	}
	minimumSteps, remainder := minimumAmount.QuoRem(step, 0)
	if !remainder.IsZero() {
		minimumSteps = minimumSteps.Add(decimal.NewFromInt(1))
	}
	if minimumSteps.GreaterThan(decimal.NewFromInt(math.MaxInt64)) {
		return instrument, errors.New("execution: normalized minimum quantity exceeds step range")
	}
	instrument = Instrument{ID: id, Valuation: "linear_perpetual", SettlementCurrency: market.Settle, QuantityStep: step, ContractSize: contractSize, PriceTick: tick, MoneyScale: moneyScale, MinSteps: minimumSteps.IntPart(), MinNotional: minimumNotional}
	body, err := payload(struct {
		MarketID, Symbol string
		Units            Instrument
	}{market.ID, market.Symbol, instrument})
	if err != nil {
		return Instrument{}, err
	}
	hash := sha256.Sum256([]byte(body))
	instrument.Version = "banexg-" + hex.EncodeToString(hash[:])
	return instrument, instrument.Validate()
}

func fixedPrecisionQuantum(value float64, mode int) (decimal.Decimal, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return decimal.Zero, errors.New("execution: nonfinite normalized precision")
	}
	switch mode {
	case banexg.PrecModeTickSize:
		if value > 0 {
			return decimal.NewFromFloat(value), nil
		}
	case banexg.PrecModeDecimalPlace:
		if value == math.Trunc(value) && value >= -18 && value <= 18 {
			return decimal.New(1, -int32(value)), nil
		}
	}
	return decimal.Zero, errors.New("execution: normalized precision has no supported fixed quantum")
}
