package entry

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/audit"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
)

func TestDataAuditRefreshStagesOnlyAndPreservesInputs(t *testing.T) {
	dir := t.TempDir()
	makeArchive := func(name string, close float64) string {
		t.Helper()
		store, _ := factor.NewVersionStore(10)
		row := factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1m", Sid: 1, TimeMS: 60000, EndMS: 120000, Values: map[string]any{"open": close, "high": close, "low": close, "close": close, "volume": 0.0, "custom": int64(1<<53 + 1), "null": nil}}, Revision: 1, SourceVersion: name}
		if err := store.Put(row); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name+".gob")
		if _, err := store.Export(path); err != nil {
			t.Fatal(err)
		}
		return path
	}
	baseline, candidate := makeArchive("baseline", 10), makeArchive("candidate", 11)
	before, _ := os.ReadFile(baseline)
	beforeCandidate, _ := os.ReadFile(candidate)
	stage := filepath.Join(dir, "review.gob")
	output := new(bytes.Buffer)
	command := newDataAuditCommand()
	command.SetOut(output)
	command.SetArgs([]string{"refresh-review", "--baseline", baseline, "--candidate", candidate, "--stage", stage, "--min-symbols", "2", "--min-fraction", ".8"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	var report struct {
		Applied bool
		SHA     string `json:"stage_sha256"`
	}
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Applied || report.SHA == "" {
		t.Fatalf("unsafe/incomplete report: %s", output)
	}
	after, _ := os.ReadFile(baseline)
	afterCandidate, _ := os.ReadFile(candidate)
	if !bytes.Equal(before, after) || !bytes.Equal(beforeCandidate, afterCandidate) {
		t.Fatal("input archive overwritten")
	}
	store, err := factor.OpenVersionStore(stage, 10)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Records()
	if err != nil || rows[0].Series.Values["custom"] != int64(1<<53+1) {
		t.Fatalf("stage=%v err=%v", rows, err)
	}
	// Existing nonidentical paths cannot be silently replaced by review output.
	command = newDataAuditCommand()
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"refresh-review", "--baseline", baseline, "--candidate", candidate, "--stage", baseline, "--min-symbols", "2", "--min-fraction", ".8"})
	if err := command.Execute(); err == nil {
		t.Fatal("review overwrote baseline")
	}
}

func TestDataAuditFundingRequiresExplicitSnapshotsAndRejectsTrailingJSON(t *testing.T) {
	command := newDataAuditCommand()
	command.SetArgs([]string{"funding"})
	command.SetOut(new(bytes.Buffer))
	command.SetErr(new(bytes.Buffer))
	if err := command.Execute(); err == nil {
		t.Fatal("unscoped audit accepted")
	}
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte(`{} {}`), 0600); err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := readAuditJSON(path, &value); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("trailing JSON error=%v", err)
	}
}

func TestDataAuditFundingReportsMissingWithoutChangingSnapshots(t *testing.T) {
	dir := t.TempDir()
	account := execution.AccountKey{VenueSessionIdentity: "test", Account: "main", SettlementDomain: "USDT"}
	venue := audit.FundingSnapshot{Account: account, StartMS: 1, StopMS: 3, Instruments: map[string]string{"BTC/USDT:USDT": "btc"}, Rows: []banexg.FundingCash{{ID: "special-1", Symbol: "BTC/USDT:USDT", Currency: "USDT", Amount: "1", Mark: "100", Rate: "-.01", AtMS: 2}}}
	ledger := audit.FundingLedgerSnapshot{Account: account}
	venueRaw, _ := json.Marshal(venue)
	ledgerRaw, _ := json.Marshal(ledger)
	venuePath, ledgerPath := filepath.Join(dir, "venue.json"), filepath.Join(dir, "ledger.json")
	if err := os.WriteFile(venuePath, venueRaw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ledgerPath, ledgerRaw, 0600); err != nil {
		t.Fatal(err)
	}
	output := new(bytes.Buffer)
	command := newDataAuditCommand()
	command.SetOut(output)
	command.SetArgs([]string{"funding", "--venue", venuePath, "--ledger", ledgerPath})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	var report audit.FundingReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil || len(report.Missing) != 1 || report.Missing[0].ID != "special-1" {
		t.Fatalf("report=%s err=%v", output, err)
	}
	afterVenue, _ := os.ReadFile(venuePath)
	afterLedger, _ := os.ReadFile(ledgerPath)
	if !bytes.Equal(venueRaw, afterVenue) || !bytes.Equal(ledgerRaw, afterLedger) {
		t.Fatal("funding audit changed input evidence")
	}
}
