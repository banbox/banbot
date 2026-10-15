package execution

import (
	"context"
	"errors"
	"time"

	"github.com/banbox/banexg"
)

// banexgTransport binds the SDK's capability proof and context-aware calls to
// the execution adapter.  It deliberately performs calls synchronously: a
// goroutine cannot safely interrupt an SDK call while preserving its owner
// lease.
type banexgTransport struct{}

func NewBanexgTransport() BanexgExecutionTransport { return banexgTransport{} }

func (banexgTransport) Verify(ctx context.Context, exchange banexg.BanExchange, account AccountKey) (BanexgExecutionProof, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return BanexgExecutionProof{}, err
	}
	for {
		wrapped, ok := exchange.(interface{ UnderlyingExchange() banexg.BanExchange })
		if !ok {
			break
		}
		exchange = wrapped.UnderlyingExchange()
	}
	capability, ok := exchange.(banexg.ExecutionCapability)
	if !ok {
		return BanexgExecutionProof{}, errors.New("execution: exchange has no verified execution capability")
	}
	proof, err := capability.VerifyExecution(ctx, account.Account, account.SettlementDomain)
	if err != nil {
		return BanexgExecutionProof{}, err
	}
	if proof.Account != account.Account || proof.Currency != account.SettlementDomain || ctx.Err() != nil {
		return BanexgExecutionProof{}, errors.Join(errors.New("execution: SDK proof account/currency mismatch or deadline expired"), ctx.Err())
	}
	return BanexgExecutionProof{Account: account, EvidenceID: proof.EvidenceID,
		ContextBound: proof.ContextBound, StableClientID: proof.StableClientID,
		QueryClientID: proof.QueryClientID, AuthoritativeNotFound: proof.AuthoritativeNotFound,
		CompleteCumulativeReports: proof.CompleteCumulativeReports,
		CompleteAccountSnapshot:   proof.CompleteAccountSnapshot, SettledCash: proof.SettledCash,
		NetLinearPositions: proof.NetLinearPositions, PostOnly: proof.PostOnly}, nil
}

func (banexgTransport) Invoke(ctx context.Context, call func() error) error {
	if call == nil {
		return errors.New("execution: nil exchange call")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := call()
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	return err
}
