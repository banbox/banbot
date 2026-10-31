package entry

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/banbox/banbot/audit"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
	"github.com/spf13/cobra"
)

func newDataAuditCommand() *cobra.Command {
	command := &cobra.Command{Use: "data-audit", Short: "build read-only data-quality and funding review artifacts"}
	command.AddCommand(newRefreshReviewCommand(), newFundingAuditCommand())
	return command
}

func readAuditArchive(path string, limit int) ([]*orm.DataSeries, error) {
	store, err := factor.OpenVersionStore(path, limit)
	if err != nil {
		return nil, err
	}
	records, err := store.Records()
	if err != nil {
		return nil, err
	}
	rows := make([]*orm.DataSeries, 0, len(records))
	for i := range records {
		rows = append(rows, &records[i].Series)
	}
	return rows, nil
}

func newRefreshReviewCommand() *cobra.Command {
	var baseline, candidate, stage string
	var maxRecords, minSymbols int
	var minFraction float64
	command := &cobra.Command{Use: "refresh-review", Short: "compare typed archives and stage a proposed refresh without database writes", RunE: func(command *cobra.Command, _ []string) error {
		if baseline == "" || candidate == "" || stage == "" {
			return fmt.Errorf("--baseline, --candidate and --stage are required")
		}
		old, err := readAuditArchive(baseline, maxRecords)
		if err != nil {
			return err
		}
		fresh, err := readAuditArchive(candidate, maxRecords)
		if err != nil {
			return err
		}
		policy := audit.FlatPolicy{MinSymbols: minSymbols, MinFraction: minFraction}
		oldAlerts, err := audit.DetectFlatMarket(old, policy)
		if err != nil {
			return err
		}
		freshAlerts, err := audit.DetectFlatMarket(fresh, policy)
		if err != nil {
			return err
		}
		proposed, report, err := audit.StageRefresh(old, fresh)
		if err != nil {
			return err
		}
		store, err := factor.NewVersionStore(maxRecords)
		if err != nil {
			return err
		}
		for _, record := range proposed {
			if err := store.Put(record); err != nil {
				return err
			}
		}
		digest, err := store.Export(stage)
		if err != nil {
			return err
		}
		return json.NewEncoder(command.OutOrStdout()).Encode(struct {
			Stage           string              `json:"stage"`
			SHA256          string              `json:"stage_sha256"`
			Review          audit.RefreshReport `json:"review"`
			BaselineAlerts  []audit.FlatAlert   `json:"baseline_alerts"`
			CandidateAlerts []audit.FlatAlert   `json:"candidate_alerts"`
			Applied         bool                `json:"applied"`
		}{stage, digest, report, oldAlerts, freshAlerts, false})
	}}
	command.Args = cobra.NoArgs
	command.Flags().StringVar(&baseline, "baseline", "", "existing typed factor archive (same SID registry as candidate)")
	command.Flags().StringVar(&candidate, "candidate", "", "freshly fetched typed factor archive")
	command.Flags().StringVar(&stage, "stage", "", "new immutable proposed-refresh archive; never imported automatically")
	command.Flags().IntVar(&maxRecords, "max-records", 100000, "maximum records per input and staged archive")
	command.Flags().IntVar(&minSymbols, "min-symbols", 0, "explicit minimum simultaneous flat symbols (at least 2)")
	command.Flags().Float64Var(&minFraction, "min-fraction", 0, "explicit minimum flat fraction of observed symbols (0,1]")
	return command
}

func readAuditJSON(path string, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("audit: snapshot must contain exactly one JSON object")
	}
	return nil
}

func newFundingAuditCommand() *cobra.Command {
	var venuePath, ledgerPath string
	command := &cobra.Command{Use: "funding", Short: "compare account-scoped venue and committed-event snapshots without repairs", RunE: func(command *cobra.Command, _ []string) error {
		if venuePath == "" || ledgerPath == "" {
			return fmt.Errorf("--venue and --ledger are required")
		}
		var venue audit.FundingSnapshot
		if err := readAuditJSON(venuePath, &venue); err != nil {
			return err
		}
		var ledger audit.FundingLedgerSnapshot
		if err := readAuditJSON(ledgerPath, &ledger); err != nil {
			return err
		}
		report, err := audit.AuditFunding(venue, ledger)
		if err != nil {
			return err
		}
		return json.NewEncoder(command.OutOrStdout()).Encode(report)
	}}
	command.Args = cobra.NoArgs
	command.Flags().StringVar(&venuePath, "venue", "", "account-scoped FundingSnapshot JSON with complete bounded venue evidence")
	command.Flags().StringVar(&ledgerPath, "ledger", "", "same-account FundingLedgerSnapshot JSON containing committed source events")
	return command
}
