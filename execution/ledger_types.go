package execution

import (
	"errors"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
)

// Instrument fixes the units of this first linear-settlement ledger. No venue
// name is consulted. Inverse contracts require a different valuation contract.
type Instrument struct {
	ID                 string
	Version            string
	Valuation          string // currently only "linear_perpetual"
	SettlementCurrency string
	QuantityStep       decimal.Decimal
	ContractSize       decimal.Decimal
	PriceTick          decimal.Decimal
	MoneyScale         int32
	MinSteps           int64
	MinNotional        decimal.Decimal
}

func (i Instrument) Validate() error {
	if i.Valuation != "linear_perpetual" || !canonicalID(i.ID) || !canonicalID(i.Version) || !canonicalID(i.SettlementCurrency) || !i.QuantityStep.IsPositive() || !i.ContractSize.IsPositive() || !i.PriceTick.IsPositive() || i.MoneyScale < 0 || i.MoneyScale > 18 || i.MinSteps < 0 || i.MinNotional.IsNegative() {
		return errors.New("execution: invalid linear instrument units/version/scale")
	}
	return nil
}

func canonicalID(s string) bool { return s != "" && strings.TrimSpace(s) == s }
func (i Instrument) Notional(steps int64, price decimal.Decimal) decimal.Decimal {
	return decimal.NewFromInt(steps).Mul(i.QuantityStep).Mul(i.ContractSize).Mul(price)
}

type Plan struct {
	ID                     string
	Sequence               int64
	DecisionMS             int64
	ExpiresMS              int64
	Intents                []EligibleIntent
	Targets                []ExecutableTarget // complete account-combined executable view
	RequestHash            string
	CarriedIntents         []EligibleIntent
	CarriedTargets         []ExecutableTarget // frozen untouched membership; never initiates execution
	ContributorConstraints []EligibleIntent   // original frozen definitions for retries and carried-strategy deltas
	Instruments            []Instrument
	RebalancedInstruments  []string // execution subset; other descriptors only reserve carried risk
	RiskValidUntilMS       int64
}

type FillAllocation struct {
	ID       string
	IntentID VirtualIntentID
	Strategy StrategyID
	Lot      VirtualLotID
	Side     OrderSide
	Kind     IntentKind
	Steps    int64
}

type OrderIntent struct {
	SubmitAtMS     int64 `json:"-"` // owner-assigned actual submission time, not frozen source quote time
	ID             string
	PlanID         string
	Instrument     Instrument
	Side           OrderSide
	Steps          int64
	Limit          decimal.Decimal
	PostOnly       bool
	Observation    ExecutionObservation
	ReduceOnly     bool
	RequiresFilled []string       // preceding decrease legs, never bypassed by Send
	Risk           *PortfolioRisk // coordinated plans freeze bounded quote/risk reservations
	Allocations    []FillAllocation
}

type ExecutionObservation struct {
	Price        decimal.Decimal
	Bid          decimal.Decimal // optional for market orders, required for passive simulation
	Ask          decimal.Decimal
	AtMS         int64
	ValidUntilMS int64
	Bar          int64
}

type RealOrderState string

const (
	OrderPrepared      RealOrderState = "Prepared"
	OrderSending       RealOrderState = "Sending"
	OrderAcknowledged  RealOrderState = "Acknowledged"
	OrderPartial       RealOrderState = "Partial"
	OrderFilled        RealOrderState = "Filled"
	OrderUnknown       RealOrderState = "Unknown"
	OrderCancelPending RealOrderState = "CancelPending"
	OrderCanceled      RealOrderState = "Canceled"
	OrderRejected      RealOrderState = "Rejected"
)

type StoredOrder struct {
	Intent           OrderIntent
	ClientID         string
	ExchangeID       string
	State            RealOrderState
	FilledSteps      int64
	ReportedFee      decimal.Decimal
	ReportedCost     decimal.Decimal
	Attempt          int64
	Generation       uint64
	AllocationFilled map[string]int64
}

type FillReport struct {
	EventID               string // stable trade ID or documented cumulative-report identity
	OrderID               string
	Steps                 int64           // incremental fill units, or total if Cumulative
	Fee                   decimal.Decimal // incremental fee, or total if Cumulative
	Price                 decimal.Decimal
	Cost                  decimal.Decimal // exact cumulative settlement notional for snapshots
	Cumulative            bool
	AuthoritativeSnapshot bool // controlled transition to cumulative normalization
	AtMS                  int64
}

type VirtualLot struct {
	Strategy    StrategyID
	ID          VirtualLotID
	Instrument  Instrument
	SignedSteps int64
	CostBasis   decimal.Decimal // absolute open notional, exact decimal
	RealizedPnL decimal.Decimal
	Fees        decimal.Decimal
	Funding     decimal.Decimal
}

func (l VirtualLot) Unrealized(mark decimal.Decimal) decimal.Decimal {
	value := l.Instrument.Notional(absSteps(l.SignedSteps), mark)
	if l.SignedSteps < 0 {
		return l.CostBasis.Sub(value)
	}
	return value.Sub(l.CostBasis)
}

type CashEventKind string

const (
	CapitalTransfer    CashEventKind = "CapitalTransfer"
	ExternalCashChange CashEventKind = "ExternalCashChange"
	Funding            CashEventKind = "Funding"
	Fee                CashEventKind = "Fee"
	Liquidation        CashEventKind = "Liquidation"
	Reconciliation     CashEventKind = "Reconciliation"
)

type CashPosting struct {
	Strategy StrategyID      // empty means the explicit unassigned account balance
	Amount   decimal.Decimal // positive credits account cash; negative debits it
}

// CashEvent postings must exactly total AccountDelta. CapitalTransfer moves
// between strategies/unassigned and requires AccountDelta=0. External events
// are unassigned and freeze risk until explicit reconciliation.
type CashEvent struct {
	ID           string
	Kind         CashEventKind
	AccountDelta decimal.Decimal
	Postings     []CashPosting
	AtMS         int64
}

type LedgerEntry struct {
	ID            int64
	EventID       string
	Kind          string
	Strategy      StrategyID
	Lot           VirtualLotID
	QuantityDelta int64
	CashDelta     decimal.Decimal
	Fee           decimal.Decimal
	RealizedPnL   decimal.Decimal
	AtMS          int64
}

type AccountSnapshot struct {
	AccountSettledCash    decimal.Decimal
	UnassignedCash        decimal.Decimal
	RiskFrozen            bool
	SyntheticStrategyCash map[StrategyID]decimal.Decimal // capital + virtual realized PnL - fee + funding; never a venue withdrawal balance
	PnLReclassification   decimal.Decimal                // synthetic + unassigned cash minus actual settled cash
	Lots                  []VirtualLot
	ActualPositions       []VirtualLot
	ExternalPositions     []VirtualLot
	Orders                []StoredOrder
	Checkpoint            int64
}

func (s AccountSnapshot) Equity(marks map[string]decimal.Decimal) (decimal.Decimal, error) {
	value := s.AccountSettledCash
	for _, lot := range s.ActualPositions {
		mark, ok := marks[lot.Instrument.ID]
		if !ok || !mark.IsPositive() {
			return decimal.Zero, fmt.Errorf("execution: missing mark for %s", lot.Instrument.ID)
		}
		value = value.Add(lot.Unrealized(mark))
	}
	return value, nil
}

func absSteps(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}
