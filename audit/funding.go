package audit

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

// FundingSnapshot is account-scoped venue evidence for [StartMS, StopMS).
// Instruments maps venue-independent symbols to the execution instrument IDs
// used by the supplied ledger. File presence does not prove venue completeness.
type FundingSnapshot struct {
	Account     execution.AccountKey `json:"account"`
	StartMS     int64                `json:"start_ms"`
	StopMS      int64                `json:"stop_ms"`
	Instruments map[string]string    `json:"instruments"`
	Rows        []banexg.FundingCash `json:"rows"`
}
type FundingLedgerSnapshot struct {
	Account execution.AccountKey       `json:"account"`
	Events  []execution.CommittedEvent `json:"events"`
}
type FundingConflict struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}
type FundingReport struct {
	Account    execution.AccountKey `json:"account"`
	StartMS    int64                `json:"start_ms"`
	StopMS     int64                `json:"stop_ms"`
	Matched    int                  `json:"matched"`
	Missing    []banexg.FundingCash `json:"missing_from_ledger"`
	Conflicts  []FundingConflict    `json:"conflicts"`
	LedgerOnly []string             `json:"ledger_only_ids"`
}

// AuditFunding compares immutable evidence only. It never replays a settlement,
// computes strategy attribution, advances a watermark or writes to any Store.
func AuditFunding(venue FundingSnapshot, ledger FundingLedgerSnapshot) (FundingReport, error) {
	report := FundingReport{Account: venue.Account, StartMS: venue.StartMS, StopMS: venue.StopMS}
	if err := venue.Account.Validate(); err != nil {
		return report, err
	}
	if venue.Account != ledger.Account || venue.StartMS < 0 || venue.StopMS <= venue.StartMS {
		return report, fmt.Errorf("audit: funding account/window mismatch")
	}
	booked := map[string]execution.FundingSettlement{}
	for _, event := range ledger.Events {
		if event.Kind != "Funding" {
			continue
		}
		var funding execution.FundingSettlement
		if err := json.Unmarshal(event.Payload, &funding); err != nil {
			return report, err
		}
		if event.ID == "" || event.ID != funding.ID || funding.AtMS < 0 || funding.Instrument.ID == "" || funding.Instrument.SettlementCurrency != venue.Account.SettlementDomain || !funding.Mark.IsPositive() {
			return report, fmt.Errorf("audit: invalid funding ledger identity")
		}
		if _, exists := booked[event.ID]; exists {
			return report, fmt.Errorf("audit: duplicate funding ledger ID %s", event.ID)
		}
		booked[event.ID] = funding
	}
	seen := map[string]bool{}
	for _, row := range venue.Rows {
		instrument, known := venue.Instruments[row.Symbol]
		amount, amountErr := decimal.NewFromString(row.Amount)
		mark, markErr := decimal.NewFromString(row.Mark)
		rate, rateErr := decimal.NewFromString(row.Rate)
		if row.ID == "" || seen[row.ID] || !known || instrument == "" || row.Currency != venue.Account.SettlementDomain || row.AtMS < venue.StartMS || row.AtMS >= venue.StopMS || amountErr != nil || markErr != nil || rateErr != nil || !mark.IsPositive() {
			return report, fmt.Errorf("audit: invalid, duplicate or out-of-window venue funding %q", row.ID)
		}
		seen[row.ID] = true
		old, exists := booked[row.ID]
		if !exists {
			report.Missing = append(report.Missing, row)
			continue
		}
		if old.Instrument.ID != instrument || old.Instrument.SettlementCurrency != row.Currency || old.AtMS != row.AtMS || !old.AccountAmount.Equal(amount) || !old.Mark.Equal(mark) || !old.Rate.Equal(rate) {
			report.Conflicts = append(report.Conflicts, FundingConflict{ID: row.ID, Reason: "instrument, currency, time, amount, mark or rate differs"})
		} else {
			report.Matched++
		}
	}
	for id, old := range booked {
		if old.AtMS >= venue.StartMS && old.AtMS < venue.StopMS && !seen[id] {
			report.LedgerOnly = append(report.LedgerOnly, id)
		}
	}
	sort.Slice(report.Missing, func(i, j int) bool { return report.Missing[i].ID < report.Missing[j].ID })
	sort.Slice(report.Conflicts, func(i, j int) bool { return report.Conflicts[i].ID < report.Conflicts[j].ID })
	sort.Strings(report.LedgerOnly)
	return report, nil
}
