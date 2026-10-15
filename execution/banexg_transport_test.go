package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type capabilityExchange struct {
	banexg.BanExchange
	proof banexg.ExecutionProof
}

func (e *capabilityExchange) VerifyExecution(context.Context, string, string) (banexg.ExecutionProof, *errs.Error) {
	return e.proof, nil
}

type capabilityWrapper struct{ banexg.BanExchange }

func (e *capabilityWrapper) UnderlyingExchange() banexg.BanExchange { return e.BanExchange }

func TestBanexgTransportProofIdentityAndSynchronousCancellation(t *testing.T) {
	key := AccountKey{"session", "account", "USD"}
	transport := NewBanexgTransport()
	exchange := &capabilityExchange{proof: banexg.ExecutionProof{Account: key.Account, Currency: key.SettlementDomain, EvidenceID: "read-only-proof", ContextBound: true}}
	proof, err := transport.Verify(context.Background(), &capabilityWrapper{exchange}, key)
	if err != nil || proof.Account != key || !proof.ContextBound {
		t.Fatal(proof, err)
	}
	exchange.proof.Account = "other-account"
	if _, err := transport.Verify(context.Background(), exchange, key); err == nil {
		t.Fatal("foreign account proof accepted")
	}
	exchange.proof.Account, exchange.proof.Currency = key.Account, "other-currency"
	if _, err := transport.Verify(context.Background(), exchange, key); err == nil {
		t.Fatal("foreign currency proof accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if err := transport.Invoke(ctx, func() error { called = true; return nil }); !errors.Is(err, context.Canceled) || called {
		t.Fatal("canceled invocation admitted", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	joined := false
	if err := transport.Invoke(ctx, func() error { cancel(); joined = true; return nil }); !errors.Is(err, context.Canceled) || !joined {
		t.Fatal("call did not join before cancellation returned", err)
	}
}
