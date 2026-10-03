package execution

// Quote is an engine-neutral, point-in-time market observation. AvailableAt
// identifies when the observation became usable; it must not precede AtMS.
type Quote struct {
	AtMS, AvailableAt int64
	Price             float64
	Bid, Ask          float64
}

// FundingObservation preserves the source event identity and its visibility time. It is
// an observation; FundingSettlement records the authoritative cash posting.
type FundingObservation struct {
	ID                string
	SID               int32
	AtMS, AvailableAt int64
	Rate              float64
}
