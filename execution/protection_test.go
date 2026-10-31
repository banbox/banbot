package execution

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
)

func protectionTemplate(side OrderSide) EligibleIntent {
	return EligibleIntent{ID: "protection", Account: AccountKey{VenueSessionIdentity: "test", Account: "a", SettlementDomain: "USDT"}, Strategy: "s", Lot: "l", Instrument: "BTC", Kind: ExitIntent, Side: side}
}

func TestPositionProtectionLegacyScenarios(t *testing.T) {
	for _, tt := range []struct {
		name                           string
		kind                           ProtectionKind
		side                           OrderSide
		distance, limit, first, second string
	}{
		{"long-stop", StopLossProtection, Sell, "5", "0", "96", "95"},
		{"short-stop", StopLossProtection, Buy, "5", "0", "104", "105"},
		{"long-profit", TakeProfitProtection, Sell, "10", "0", "109", "110"},
		{"short-profit", TakeProfitProtection, Buy, "10", "0", "91", "90"},
		{"long-stop-limit", StopLossProtection, Sell, "5", "96", "95", "96"},
		{"short-stop-limit", StopLossProtection, Buy, "5", "104", "105", "104"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec := PositionProtection{Kind: tt.kind, Distance: decimal.RequireFromString(tt.distance), Limit: decimal.RequireFromString(tt.limit), Rate: decimal.RequireFromString("0.5")}
			intent, level, configured, err := PreparePositionProtection(protectionTemplate(tt.side), spec, 7, decimal.NewFromInt(100), 1)
			if err != nil || !configured || intent.QuantitySteps != 3 {
				t.Fatalf("intent=%+v configured=%v err=%v", intent, configured, err)
			}
			steps, err := EvaluatePositionProtection(tt.kind, &intent, level, decimal.RequireFromString(tt.first), 10, 1)
			if err != nil || steps != 0 {
				t.Fatalf("premature protection steps=%d err=%v", steps, err)
			}
			body, err := json.Marshal(intent)
			if err != nil {
				t.Fatal(err)
			}
			var restored EligibleIntent
			if err := json.Unmarshal(body, &restored); err != nil {
				t.Fatal(err)
			}
			steps, err = EvaluatePositionProtection(tt.kind, &restored, level, decimal.RequireFromString(tt.second), 11, 2)
			if err != nil || steps != 3 {
				t.Fatalf("restored protection steps=%d err=%v", steps, err)
			}
		})
	}
}

func TestPositionProtectionTrailingActivationAndRestart(t *testing.T) {
	for _, side := range []OrderSide{Buy, Sell} {
		t.Run(string(side), func(t *testing.T) {
			activation, peak, reversal := int64(110), int64(120), int64(108)
			if side == Buy {
				activation, peak, reversal = 90, 80, 88
			}
			spec := PositionProtection{Kind: TrailingProtection, TrailingPercent: decimal.NewFromInt(10), ActivationPrice: decimal.NewFromInt(activation)}
			intent, level, configured, err := PreparePositionProtection(protectionTemplate(side), spec, 4, decimal.NewFromInt(100), 1)
			if err != nil || !configured {
				t.Fatal(configured, err)
			}
			for _, price := range []int64{100, activation, peak} {
				steps, err := EvaluatePositionProtection(spec.Kind, &intent, level, decimal.NewFromInt(price), 10, 1)
				if err != nil || steps != 0 {
					t.Fatal("premature trailing execution", steps, err)
				}
			}
			body, _ := json.Marshal(intent)
			var restored EligibleIntent
			if err := json.Unmarshal(body, &restored); err != nil {
				t.Fatal(err)
			}
			steps, err := EvaluatePositionProtection(spec.Kind, &restored, level, decimal.NewFromInt(reversal), 11, 2)
			if err != nil || steps != 4 || !restored.TrailingAnchor.Equal(decimal.NewFromInt(peak)) {
				t.Fatal("lost trailing checkpoint", restored, steps, err)
			}
		})
	}
}

func TestPositionProtectionRejectsInvalidPolicyAndSkipsUnset(t *testing.T) {
	for _, spec := range []PositionProtection{
		{Kind: StopLossProtection, Level: decimal.NewFromInt(95), Rate: decimal.NewFromInt(2)},
		{Kind: StopLossProtection, Distance: decimal.NewFromInt(101)},
		{Kind: TrailingProtection, TrailingPercent: decimal.NewFromInt(100)},
		{Kind: "unknown", Level: decimal.NewFromInt(95)},
	} {
		if _, _, _, err := PreparePositionProtection(protectionTemplate(Sell), spec, 10, decimal.NewFromInt(100), 0); err == nil {
			t.Fatalf("invalid protection accepted: %+v", spec)
		}
	}
	for _, kind := range []ProtectionKind{StopLossProtection, TakeProfitProtection, TrailingProtection} {
		if _, _, configured, err := PreparePositionProtection(protectionTemplate(Sell), PositionProtection{Kind: kind}, 10, decimal.NewFromInt(100), 0); err != nil || configured {
			t.Fatal("unset protection must not create an order", kind, configured, err)
		}
	}
}

func TestPositionProtectionTakeProfitLimitLatchAndTerminalState(t *testing.T) {
	spec := PositionProtection{Kind: TakeProfitProtection, Level: decimal.NewFromInt(110), Limit: decimal.NewFromInt(111)}
	intent, level, configured, err := PreparePositionProtection(protectionTemplate(Sell), spec, 3, decimal.NewFromInt(100), 1)
	if err != nil || !configured {
		t.Fatal(configured, err)
	}
	for _, price := range []int64{110, 109} {
		steps, err := EvaluatePositionProtection(spec.Kind, &intent, level, decimal.NewFromInt(price), 10, 1)
		if err != nil || steps != 0 || !intent.Triggered {
			t.Fatal("take-profit must latch independently of its limit", intent, steps, err)
		}
	}
	steps, err := EvaluatePositionProtection(spec.Kind, &intent, level, decimal.NewFromInt(111), 11, 2)
	if err != nil || steps != 3 {
		t.Fatal(steps, err)
	}
	intent.State = Canceled
	if steps, err := EvaluatePositionProtection(spec.Kind, &intent, level, decimal.NewFromInt(111), 12, 3); err != nil || steps != 0 {
		t.Fatal("canceled protection reactivated", steps, err)
	}
}

func TestPositionProtectionUsesAbsoluteLevelAndFloorsPartialQuantity(t *testing.T) {
	spec := PositionProtection{Kind: StopLossProtection, Level: decimal.NewFromInt(90), Distance: decimal.NewFromInt(5), Rate: decimal.RequireFromString("0.1")}
	intent, level, configured, err := PreparePositionProtection(protectionTemplate(Sell), spec, 10, decimal.NewFromInt(100), 1)
	if err != nil || !configured || intent.QuantitySteps != 1 || !level.Equal(spec.Level) {
		t.Fatal("absolute level must take priority over relative distance", intent, level, err)
	}
	if _, _, configured, err := PreparePositionProtection(protectionTemplate(Sell), spec, 3, decimal.NewFromInt(100), 1); err != nil || configured {
		t.Fatal("sub-step partial protection must not round up", configured, err)
	}
}
