package data

import (
	"errors"
	"strings"
	"testing"

	"github.com/banbox/banbot/orm"
)

type environmentGenerationSink struct {
	*generationSeriesSink
	end func(*orm.DataSeries) error
}

func (s *environmentGenerationSink) EnvironmentEnded(row *orm.DataSeries) error { return s.end(row) }

func TestManagedEnvironmentEndAdmission(t *testing.T) {
	for _, mode := range []string{"prepared", "foreign", "callback-error", "active"} {
		t.Run(mode, func(t *testing.T) {
			h, p, plan, _, symbol := generationDataFixture(t, "1m")
			calls := 0
			callbackErr := errors.New("close failed")
			sink := &environmentGenerationSink{generationSeriesSink: &generationSeriesSink{}, end: func(row *orm.DataSeries) error {
				calls++
				row.Values["custom"] = int64(9)
				if mode == "callback-error" {
					return callbackErr
				}
				return nil
			}}
			installation, err := p.PrepareSubscriptionPlan(t.Context(), plan, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { installation.Stop(); _ = installation.Join() }()
			if mode != "prepared" {
				if err := installation.Activate(); err != nil {
					t.Fatal(err)
				}
			}
			row := &orm.DataSeries{Source: "kline", Sid: symbol.ID, ExSymbol: symbol, TimeFrame: "1m", Values: map[string]any{"custom": int64(7)}}
			if mode == "foreign" {
				row.Sid++
			}
			err = h.EndEnvironment(0, row)
			switch mode {
			case "prepared":
				if err == nil || !strings.Contains(err.Error(), "before subscription activation") || calls != 0 {
					t.Fatalf("prepared environment admitted: calls=%d err=%v", calls, err)
				}
			case "foreign":
				if err == nil || !strings.Contains(err.Error(), "undeclared or foreign") || calls != 0 {
					t.Fatalf("foreign environment admitted: calls=%d err=%v", calls, err)
				}
			case "callback-error":
				if !errors.Is(err, callbackErr) || !errors.Is(installation.Join(), callbackErr) || !h.Stopped(0) {
					t.Fatalf("callback failure did not stop generation: %v", err)
				}
			case "active":
				if err != nil || calls != 1 || row.Values["custom"] != int64(7) {
					t.Fatalf("active callback ownership lost: calls=%d row=%v err=%v", calls, row, err)
				}
				installation.Stop()
				if err := h.EndEnvironment(0, row); err == nil || calls != 1 {
					t.Fatal("stopped generation delivered environment end")
				}
			}
		})
	}
}
