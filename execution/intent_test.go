package execution

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
)

func intentPrice(s string) decimal.Decimal { return decimal.RequireFromString(s) }
func testIntent(side OrderSide) EligibleIntent {
	return EligibleIntent{ID: "intent", Account: AccountKey{"paper", "account", "USDT"}, Strategy: "strategy", Lot: "lot", Instrument: "BTC", Kind: EntryIntent, Side: side, QuantitySteps: 10}
}

func TestIntentConditionsBothSides(t *testing.T) {
	for _, tc := range []struct {
		side                                     OrderSide
		limit, stop, before, trigger, executable string
	}{
		{Buy, "101", "102", "100", "102", "101"},
		{Sell, "101", "100", "102", "100", "101"},
	} {
		t.Run(string(tc.side), func(t *testing.T) {
			i := testIntent(tc.side)
			i.Conditions = IntentConditions{Limit: intentPrice(tc.limit), Stop: intentPrice(tc.stop)}
			for n, price := range []string{tc.before, tc.trigger, tc.executable} {
				units, err := i.Evaluate(intentPrice(price), int64(n), 0)
				if err != nil {
					t.Fatal(err)
				}
				want := int64(0)
				if n == 2 {
					want = 10
				}
				if units != want {
					t.Fatalf("price %s units %d want %d", price, units, want)
				}
			}
			if units, _ := i.Evaluate(intentPrice(tc.trigger), 3, 0); units != 0 {
				t.Fatal("latched stop bypassed limit")
			}
		})
	}
}

func TestIntentPartialCancellationAndExpiry(t *testing.T) {
	for _, expire := range []string{"cancel", "bar", "time"} {
		t.Run(expire, func(t *testing.T) {
			i := testIntent(Buy)
			i.Conditions = IntentConditions{CreatedBar: 5, StopBars: 2, ExpiresAtMS: 20}
			if err := i.Reserve(4, intentPrice("100"), 10, 5); err != nil {
				t.Fatal(err)
			}
			if err := i.ApplyFill(2); err != nil {
				t.Fatal(err)
			}
			if i.FilledSteps != 2 || i.ReservedSteps != 2 {
				t.Fatal("partial fill lost")
			}
			if i.State != Partial {
				t.Fatal("partial fill must expose Partial state")
			}
			if units, err := i.Evaluate(intentPrice("100"), 19, 6); err != nil || units != 6 {
				t.Fatalf("before expiry: %d %v", units, err)
			}
			switch expire {
			case "cancel":
				if err := i.Cancel(); err != nil {
					t.Fatal(err)
				}
			case "bar":
				if units, err := i.Evaluate(intentPrice("100"), 19, 7); err != nil || units != 0 {
					t.Fatal(units, err)
				}
			case "time":
				if units, err := i.Evaluate(intentPrice("100"), 20, 6); err != nil || units != 0 {
					t.Fatal(units, err)
				}
			}
			if i.FilledSteps != 2 || i.ReservedSteps != 0 {
				t.Fatal("terminal state swallowed fill")
			}
			if err := i.Reserve(1, intentPrice("100"), 21, 7); err == nil {
				t.Fatal("terminal intent executable")
			}
		})
	}
	i := testIntent(Buy)
	if err := i.ApplyFill(1); err == nil {
		t.Fatal("unreserved fill accepted")
	}
	if err := i.Reserve(10, intentPrice("100"), 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := i.ApplyFill(11); err == nil {
		t.Fatal("overfill accepted")
	}
	if err := i.ApplyFill(10); err != nil || i.State != Filled {
		t.Fatal("full fill failed", err)
	}
	if err := i.Cancel(); err != nil || i.State != Filled {
		t.Fatal("cancel changed filled intent")
	}
}

func TestLimitEligibilityChangesWithPrice(t *testing.T) {
	for _, side := range []OrderSide{Buy, Sell} {
		i := testIntent(side)
		i.Conditions.Limit = intentPrice("100")
		if n, err := i.Evaluate(intentPrice("100"), 1, 0); err != nil || n != 10 || i.State != Eligible {
			t.Fatal("inclusive limit failed", n, err)
		}
		blocked := "101"
		if side == Sell {
			blocked = "99"
		}
		if n, err := i.Evaluate(intentPrice(blocked), 2, 0); err != nil || n != 0 || i.State != PendingCondition {
			t.Fatal("limit remained executable at invalid price", n, err)
		}
	}
}

func TestTrailingCheckpointBothSides(t *testing.T) {
	for _, tc := range []struct {
		side                                OrderSide
		activation, before, anchor, trigger string
	}{
		{Sell, "100", "99", "110", "99"}, {Buy, "100", "101", "90", "99"},
	} {
		t.Run(string(tc.side), func(t *testing.T) {
			i := testIntent(tc.side)
			i.Kind = ExitIntent
			i.Conditions = IntentConditions{ActivationPrice: intentPrice(tc.activation), TrailingPercent: intentPrice("10")}
			if n, err := i.Evaluate(intentPrice(tc.before), 1, 0); err != nil || n != 0 || i.TrailingActive {
				t.Fatal("premature activation", n, err)
			}
			if n, err := i.Evaluate(intentPrice(tc.anchor), 2, 0); err != nil || n != 0 {
				t.Fatal(n, err)
			}
			data, err := json.Marshal(i)
			if err != nil {
				t.Fatal(err)
			}
			var restored EligibleIntent
			if err := json.Unmarshal(data, &restored); err != nil {
				t.Fatal(err)
			}
			if n, err := restored.Evaluate(intentPrice(tc.trigger), 3, 0); err != nil || n != 10 || !restored.Triggered {
				t.Fatal("restored trailing did not trigger", n, err)
			}
		})
	}
}

func TestExitSelectionOwnershipAndModes(t *testing.T) {
	i := testIntent(Buy)
	lot := LotSelection{i.Account, i.Strategy, i.Lot, i.Instrument, 4, 6}
	s := ExitSelection{Account: i.Account, Strategy: i.Strategy, Lot: i.Lot, Instrument: i.Instrument}
	for _, tc := range []struct {
		filled, unfilled bool
		reduce, cancel   int64
	}{{false, false, 4, 6}, {true, false, 4, 0}, {false, true, 0, 6}} {
		s.FilledOnly, s.UnFillOnly = tc.filled, tc.unfilled
		r, c, err := s.Select(lot)
		if err != nil || r != tc.reduce || c != tc.cancel {
			t.Fatal(r, c, err)
		}
	}
	s.FilledOnly, s.UnFillOnly = true, true
	if _, _, err := s.Select(lot); err == nil {
		t.Fatal("contradictory modes accepted")
	}
	s.FilledOnly, s.UnFillOnly = false, false
	for _, change := range []func(*LotSelection){func(l *LotSelection) { l.Account.Account = "other" }, func(l *LotSelection) { l.Strategy = "other" }, func(l *LotSelection) { l.Lot = "other" }, func(l *LotSelection) { l.Instrument = "other" }} {
		wrong := lot
		change(&wrong)
		if _, _, err := s.Select(wrong); err == nil {
			t.Fatal("foreign lot selected")
		}
	}
}

func TestIntentInvalidConditions(t *testing.T) {
	for _, conditions := range []IntentConditions{{Limit: intentPrice("-1")}, {StopBars: -1}, {TrailingPercent: intentPrice("100")}, {ActivationPrice: intentPrice("10")}, {Stop: intentPrice("10"), TrailingPercent: intentPrice("1")}} {
		i := testIntent(Buy)
		i.Conditions = conditions
		if _, err := i.Evaluate(intentPrice("100"), 1, 0); err == nil {
			t.Fatal("invalid condition accepted")
		}
	}
}
