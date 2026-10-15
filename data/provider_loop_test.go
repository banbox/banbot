package data

import (
	"errors"
	"github.com/banbox/banbot/core"
	"net"
	"testing"

	"github.com/banbox/banexg/errs"
)

func TestLiveProviderIntentionalStopPreservesRealTransportFailures(t *testing.T) {
	p := &LiveProvider{}
	closed := errs.New(core.ErrNetReadFail, &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed})
	if p.liveLoopError(closed) != closed {
		t.Fatal("unexpected transport closure suppressed before Stop")
	}
	p.handlerStop = true
	if err := p.liveLoopError(closed); err != nil {
		t.Fatal("intentional closed socket reported as failure")
	}
	failure := errors.New("transport lost data")
	realFailure := errs.New(core.ErrNetReadFail, failure)
	if err := p.liveLoopError(realFailure); !errors.Is(err, failure) {
		t.Fatal("real source failure discarded during Stop")
	}
}
