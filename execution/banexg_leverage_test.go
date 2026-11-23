package execution

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type leverageSession struct {
	*fakeBanexgSession
	noAPI bool
	fail  *errs.Error
	calls []string
}

func (e *leverageSession) HasApi(api, market string) bool {
	if api == banexg.ApiSetLeverage {
		return !e.noAPI
	}
	return e.fakeBanexgSession.HasApi(api, market)
}

func (e *leverageSession) SetLeverage(leverage float64, symbol string, params map[string]any) (map[string]any, *errs.Error) {
	if params[banexg.ParamAccount] == "" {
		panic("leverage call lost its account")
	}
	if e.fail != nil {
		return nil, e.fail
	}
	e.calls = append(e.calls, fmt.Sprintf("%s=%g", symbol, leverage))
	return map[string]any{}, nil
}

func TestBanexgDeclaredLeverageIsAppliedBeforeOrders(t *testing.T) {
	ctx := context.Background()
	_, _, base, _, config := banexgFixture(t)
	config.Instruments[0].Leverage = 5

	t.Run("requires the SDK API", func(t *testing.T) {
		if _, err := NewBanexgAdapter(ctx, &leverageSession{fakeBanexgSession: base, noAPI: true}, config); err == nil {
			t.Fatal("declared leverage accepted without SetLeverage support")
		}
	})
	t.Run("rejects out of range", func(t *testing.T) {
		bad := config
		bad.Instruments = []BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: ledgerInstrument(), Leverage: 126}}
		if _, err := NewBanexgAdapter(ctx, &leverageSession{fakeBanexgSession: base}, bad); err == nil {
			t.Fatal("leverage above 125 accepted")
		}
	})
	t.Run("venue failure keeps orders blocked", func(t *testing.T) {
		session := &leverageSession{fakeBanexgSession: base, fail: errs.NewMsg(errs.CodeRunTime, "venue refused")}
		adapter, err := NewBanexgAdapter(ctx, session, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := adapter.ApplyLeverage(ctx); err == nil || !strings.Contains(err.Error(), "venue refused") {
			t.Fatalf("venue failure hidden: %v", err)
		}
		intent := OrderIntent{Instrument: ledgerInstrument()}
		if _, err := adapter.Submit(ctx, intent, "exec-test"); err == nil || !strings.Contains(err.Error(), "leverage") || base.creates != 0 {
			t.Fatalf("order reached the venue without applied leverage: %v creates=%d", err, base.creates)
		}
	})
	t.Run("applies once per declared instrument", func(t *testing.T) {
		session := &leverageSession{fakeBanexgSession: base}
		adapter, err := NewBanexgAdapter(ctx, session, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := adapter.ApplyLeverage(ctx); err != nil {
			t.Fatal(err)
		}
		if err := adapter.ApplyLeverage(ctx); err != nil {
			t.Fatal(err)
		}
		if len(session.calls) != 1 || session.calls[0] != "BTC/USDT:USDT=5" {
			t.Fatalf("unexpected venue calls %v", session.calls)
		}
	})
	t.Run("undeclared leverage never calls the venue", func(t *testing.T) {
		plain := config
		plain.Instruments = []BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: ledgerInstrument()}}
		session := &leverageSession{fakeBanexgSession: base, noAPI: true}
		adapter, err := NewBanexgAdapter(ctx, session, plain)
		if err != nil {
			t.Fatal(err)
		}
		if err := adapter.ApplyLeverage(ctx); err != nil || len(session.calls) != 0 {
			t.Fatalf("undeclared leverage touched the venue: %v %v", err, session.calls)
		}
	})
}
