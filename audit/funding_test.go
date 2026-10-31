package audit

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

func TestAuditFundingReadOnlyDistinctTypesAndConflicts(t *testing.T) {
	account := execution.AccountKey{VenueSessionIdentity: "paper", Account: "main", SettlementDomain: "USDT"}
	regular := banexg.FundingCash{ID: "funding/USDT/42", Symbol: "BTC/USDT:USDT", Currency: "USDT", Amount: "-0.1", Mark: "100", Rate: ".001", AtMS: 10}
	special := regular
	special.ID = "funding/SPECIAL_FUNDING_FEE/USDT/42"
	special.Amount = ".2"
	special.Rate = "-.002"
	event := execution.FundingSettlement{ID: regular.ID, Instrument: execution.Instrument{ID: "btc-perp", SettlementCurrency: "USDT"}, AccountAmount: decimal.RequireFromString(regular.Amount), Mark: decimal.RequireFromString(regular.Mark), Rate: decimal.RequireFromString(regular.Rate), AtMS: regular.AtMS}
	raw, _ := json.Marshal(event)
	events := []execution.CommittedEvent{{ID: regular.ID, Kind: "Funding", Checkpoint: 1, Payload: raw}, {ID: "funding-watermark/20", Kind: "Cash", Checkpoint: 2, Payload: json.RawMessage(`{}`)}}
	before, _ := json.Marshal(events)
	venue := FundingSnapshot{Account: account, StartMS: 0, StopMS: 20, Instruments: map[string]string{regular.Symbol: "btc-perp"}, Rows: []banexg.FundingCash{regular, special}}
	ledger := FundingLedgerSnapshot{Account: account, Events: events}
	report, err := AuditFunding(venue, ledger)
	if err != nil || report.Matched != 1 || len(report.Missing) != 1 || report.Missing[0].ID != special.ID {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	after, _ := json.Marshal(events)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("audit changed events")
	}
	venue.Rows[0].Amount = "-.3"
	report, err = AuditFunding(venue, ledger)
	if err != nil || len(report.Conflicts) != 1 {
		t.Fatalf("conflict hidden: %+v %v", report, err)
	}
	ledger.Account.Account = "another"
	if _, err = AuditFunding(venue, ledger); err == nil {
		t.Fatal("cross-account evidence accepted")
	}
}

func TestAuditFundingRejectsDuplicateAndOutOfWindowEvidence(t *testing.T) {
	account := execution.AccountKey{VenueSessionIdentity: "test", Account: "main", SettlementDomain: "USDT"}
	row := banexg.FundingCash{ID: "x", Symbol: "BTC/USDT:USDT", Currency: "USDT", Amount: "1", Mark: "100", Rate: ".1", AtMS: 10}
	venue := FundingSnapshot{Account: account, StartMS: 0, StopMS: 20, Instruments: map[string]string{row.Symbol: "btc-perp"}, Rows: []banexg.FundingCash{row, row}}
	ledger := FundingLedgerSnapshot{Account: account}
	if _, err := AuditFunding(venue, ledger); err == nil {
		t.Fatal("duplicate evidence accepted")
	}
	venue.Rows = venue.Rows[:1]
	venue.StopMS = 10
	if _, err := AuditFunding(venue, ledger); err == nil {
		t.Fatal("exclusive stop not enforced")
	}
}
