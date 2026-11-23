package execution

import "errors"

// Rejection codes are the closed set of owner refusals a Hub reports to a robot
// as such. A code is a fixed string, never error text. The refusal is raised
// while a plan is prepared inside one local transaction, so the rejected
// command left no receipt, revision, plan or pending definition behind; the
// robot must not resend it unchanged, because the owner would refuse again.
const (
	// RejectLimitOffTick: a contributor limit is not a multiple of the tick.
	RejectLimitOffTick = "limit_off_tick"
	// RejectRiskLimit: the plan would exceed a strategy/account gross, NAV or
	// margin budget. State dependent ("not now"), never a reason to stop.
	RejectRiskLimit = "risk_limit"
	// RejectSpotShort: a short target on an instrument that cannot be shorted.
	RejectSpotShort = "spot_short"
	// RejectSelfCross: opposing deltas of one strategy in one plan.
	RejectSelfCross = "self_cross"
	// RejectAdmissionLimit: the account admission policy (max_open_orders or
	// max_simul_open across all robots) refused a new time-series entry.
	RejectAdmissionLimit = "admission_limit"
	// RejectAccountNotReady: a new entry arrived while the account is not
	// reconciled (a venue read is failing or the account is frozen). State
	// dependent ("not now"): the entry is dropped and the robot keeps running.
	// Only a create is refused with this code; other commands stay hard errors.
	RejectAccountNotReady = "account_not_ready"
	// RejectStaleEntry: a time-series create whose signal time (metadata
	// CreatedMS) is older than max(2 x timeframe, 2 minutes) at the owner. A
	// frozen command resent after a restart or a long outage would fill at a
	// price the strategy never chose. A command already accepted earlier still
	// replays idempotently; only a first admission is refused.
	RejectStaleEntry = "stale_entry"
)

var hubRejectCodes = map[string]bool{RejectLimitOffTick: true, RejectRiskLimit: true, RejectSpotShort: true, RejectSelfCross: true, RejectAdmissionLimit: true, RejectAccountNotReady: true, RejectStaleEntry: true}

// RejectedError is a pre-commit refusal carrying a code from the closed set
// above. Its text is for the host-side log only and never crosses the RPC.
//
// Scope is the owner-side attribution of the refusal (which strategy, lot,
// instrument and intent kind raised it) when the raise site knows it. It lets
// the autonomous tick isolate one entry instead of freezing the account (see
// pending_isolation.go). It is never serialised: the RPC carries the code only.
type RejectedError struct {
	Code  string
	Err   error
	Scope rejectionScope
}

// rejectionScope is empty for refusals computed across all lots (account-level
// limits). Lot implies Strategy and Instrument; Kind is the intent kind that
// raised the refusal when known.
type rejectionScope struct {
	Strategy   StrategyID
	Lot        VirtualLotID
	Instrument string
	Kind       IntentKind
}

func (e *RejectedError) Error() string {
	if e.Err == nil {
		return "execution: command rejected: " + e.Code
	}
	return e.Err.Error()
}

func (e *RejectedError) Unwrap() error { return e.Err }

// Is makes two rejections with the same code equal, so a client can compare
// with errors.Is(err, &RejectedError{Code: RejectRiskLimit}).
func (e *RejectedError) Is(target error) bool {
	other, ok := target.(*RejectedError)
	return ok && other.Code == e.Code
}

func rejected(code string, message string) error {
	return &RejectedError{Code: code, Err: errors.New(message)}
}

func rejectedAt(code string, message string, scope rejectionScope) error {
	return &RejectedError{Code: code, Err: errors.New(message), Scope: scope}
}

// venueMinimumError is ErrVenueMinimum with the instrument whose real leg was
// too small. errors.Is(err, ErrVenueMinimum) holds; the text is unchanged.
type venueMinimumError struct{ Instrument string }

func (e *venueMinimumError) Error() string        { return ErrVenueMinimum.Error() }
func (e *venueMinimumError) Is(target error) bool { return target == ErrVenueMinimum }

// RejectionCode returns the code of a typed rejection anywhere in err's chain.
func RejectionCode(err error) (string, bool) {
	var rejection *RejectedError
	if errors.As(err, &rejection) && hubRejectCodes[rejection.Code] {
		return rejection.Code, true
	}
	return "", false
}

// hubRPCRejection chooses the fixed code reported for a failed hub operation.
// The venue minimum keeps its historical behaviour. The remaining codes are
// reported only for a new entry: refusing an exit, edit or protection change
// can come from another lot in the same combined plan, and dropping it would
// leave a position unmanaged, so those stay a generic failure.
func hubRPCRejection(path string, request hubRPCRequest, err error) string {
	if errors.Is(err, ErrVenueMinimum) {
		return hubRejectedVenueMinimum
	}
	if path == "/v1/pending-command" && request.Pending != nil && request.Pending.Action == PendingCreate {
		if code, ok := RejectionCode(err); ok {
			return code
		}
	}
	return ""
}
