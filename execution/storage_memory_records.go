package execution

type recordKind uint8

const (
	recordsCheckpoint         recordKind = 0
	recordsVirtualIntent      recordKind = 1
	recordsPlan               recordKind = 2
	recordsAccount            recordKind = 3
	recordsOrder              recordKind = 4
	recordsStrategy           recordKind = 6
	recordsAllocation         recordKind = 7
	recordsInternalAllocation recordKind = 8
	recordsLot                recordKind = 9
	recordsEvent              recordKind = 10
	recordsLedger             recordKind = 11
	recordsPosition           recordKind = 12
	recordsExternalPosition   recordKind = 13
	recordsMigration          recordKind = 14
	recordsLegacySource       recordKind = 15
	recordsPlanIntent         recordKind = 16
	recordsPaperOrder         recordKind = 17
)

// memoryRecord holds execution records, independent of SQLite and SQL syntax.
type memoryRecord struct {
	Account             string
	Strategy            string
	Name                string
	Payload             string
	RuntimePayload      string
	ID                  string
	Sequence            int64
	Frozen              bool
	State               string
	Attempt             int64
	Generation          string
	OrderId             string
	Kind                string
	AtMs                int64
	ExchangeId          string
	ReportMode          string
	ClientId            string
	Fees                string
	Funding             string
	Filled              int64
	IntentId            string
	Steps               int64
	EventId             string
	Lot                 string
	Quantity            int64
	Cash                string
	Fee                 string
	Realized            string
	Unassigned          string
	Checkpoint          int64
	PnlReclassification string
	Instrument          string
	Cost                string
	SourceHash          string
	SourceVersion       string
	SchemaVersion       int64
	MigrationId         string
	PlanId              string
	KeyJson             string
	PostingID           int64
}

func (r memoryRecord) runtimeBody() string {
	if r.RuntimePayload != "" {
		return r.RuntimePayload
	}
	return r.Payload
}
