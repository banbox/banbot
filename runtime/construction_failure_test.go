package runtime

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type trackedConstructionParent struct {
	context.Context
	active atomic.Int32
}

func (p *trackedConstructionParent) Value(any) any { return nil }

func (p *trackedConstructionParent) AfterFunc(callback func()) func() bool {
	p.active.Add(1)
	stop := context.AfterFunc(p.Context, callback)
	return func() bool {
		p.active.Add(-1)
		return stop()
	}
}

type invalidConstructionExchange struct{ banexg.BanExchange }

func (*invalidConstructionExchange) PriceSymbolParts(string) ([4]string, *errs.Error) {
	return [4]string{}, errs.NewMsg(errs.CodeParamInvalid, "invalid configured symbol")
}

func TestRuntimeInvalidSymbolsReleaseParentRegistration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parent := &trackedConstructionParent{Context: ctx}
	process := NewProcess()
	defer process.Close()
	for attempt := 0; attempt < 3; attempt++ {
		_, err := process.NewRuntime(Options{
			Context: parent, Exchange: &invalidConstructionExchange{},
			ExchangeName: "test", Market: "linear", Pairs: []string{"invalid"},
		})
		if err == nil {
			t.Fatal("invalid symbol accepted")
		}
		if parent.active.Load() != 0 {
			t.Fatal("failed construction retained a child cancellation context")
		}
	}
}
