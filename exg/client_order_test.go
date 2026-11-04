package exg

import (
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type clientOrderPurposeStub struct {
	clientOrderExchangeStub
	value string
}

func (s *clientOrderPurposeStub) BuildPurposeClientOrderID(string, int64, string, banexg.ClientOrderPurpose) string {
	return s.value
}

func TestBuildPurposeClientOrderIDRequiresAdapterCapability(t *testing.T) {
	exchange := &clientOrderExchangeStub{id: "adapter"}
	if got, err := BuildPurposeClientOrderIDChecked(exchange, "bot", 42, "entry",
		banexg.ClientOrderPurposeEnter); err == nil || got != "" {
		t.Fatalf("missing capability = %q/%v, want an error and no identifier", got, err)
	} else if err.Code != errs.CodeNotSupport {
		t.Fatalf("missing capability code = %d, want NotSupport", err.Code)
	}

	stub := &clientOrderPurposeStub{
		clientOrderExchangeStub: clientOrderExchangeStub{id: "adapter"},
	}
	if _, err := BuildPurposeClientOrderIDChecked(stub, "bot", 42, "entry",
		banexg.ClientOrderPurposeEnter); err == nil {
		t.Fatal("an empty adapter identifier must fail closed")
	}

	stub.value = "bot_42_z_entry"
	if got, err := BuildPurposeClientOrderIDChecked(stub, "bot", 42, "entry",
		banexg.ClientOrderPurposeExit); err != nil || got != stub.value {
		t.Fatalf("adapter identifier = %q/%v, want %q", got, err, stub.value)
	}
	if GetSubmitReconcileCapability(stub) != nil {
		t.Fatal("stub without a reconcile contract must not report one")
	}
}
