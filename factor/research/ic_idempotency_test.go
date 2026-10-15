package research

import (
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
)

func TestICHistoryDuplicateInvalidNumericIsIdempotent(t *testing.T) {
	for _, reason := range []factor.Validity{factor.Missing, factor.Null, factor.NotNumeric, factor.NonFinite, factor.Warmup} {
		t.Run(string(reason), func(t *testing.T) {
			h, err := NewICHistory(2, "forward", []string{"momentum"})
			if err != nil {
				t.Fatal(err)
			}
			sample := ICSample{Column: "momentum", Label: "forward", DecisionTime: 100, MatureAt: 110, AvailableAt: 115, IC: number(.2), RankIC: factor.Numeric{Value: math.NaN(), Validity: reason}, Samples: 24}
			if err := h.Add(120, sample); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if err := h.Add(120, sample); err != nil {
					t.Fatalf("identical sample rejected: %v", err)
				}
			}
			// Invalid payloads have no numeric meaning, including after decoding.
			sample.RankIC.Value = 0
			if err := h.Add(120, sample); err != nil {
				t.Fatalf("invalid payload changed sample identity: %v", err)
			}
			if h.Retained() != 1 || h.lastAsOf != 120 || h.samples[sample.Column][0].DecisionTime != 100 {
				t.Fatal("duplicate changed retained history or cursor")
			}
			weights, ready, err := h.QualityWeights(120, ComboSpec{Method: HistoryIC, Columns: []string{"momentum"}, MinSamples: 2})
			if err != nil || ready || len(weights) != 0 {
				t.Fatalf("duplicate counted towards warmup: %v %v %v", weights, ready, err)
			}
			if err := h.Add(130, sample); err != nil || h.lastAsOf != 130 || h.Retained() != 1 {
				t.Fatalf("successful redelivery did not advance observation time: %v", err)
			}
		})
	}
}

func TestICHistoryDuplicateRejectsRealConflicts(t *testing.T) {
	changes := map[string]func(*ICSample){
		"ic":      func(s *ICSample) { s.IC = number(.3) },
		"rank_ic": func(s *ICSample) { s.RankIC = number(.2) },
		"reason":  func(s *ICSample) { s.RankIC.Validity = factor.Null },
		"fully_invalid": func(s *ICSample) {
			s.IC = factor.Numeric{Value: math.NaN(), Validity: factor.Missing}
		},
		"mature_at":    func(s *ICSample) { s.MatureAt++ },
		"available_at": func(s *ICSample) { s.AvailableAt++ },
		"samples":      func(s *ICSample) { s.Samples++ },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			h, _ := NewICHistory(2, "forward", []string{"momentum"})
			sample := ICSample{Column: "momentum", Label: "forward", DecisionTime: 100, MatureAt: 110, AvailableAt: 115, IC: number(.2), RankIC: factor.Numeric{Value: math.NaN(), Validity: factor.Missing}, Samples: 24}
			if err := h.Add(120, sample); err != nil {
				t.Fatal(err)
			}
			conflict := sample
			change(&conflict)
			if err := h.Add(130, conflict); err == nil {
				t.Fatal("conflicting sample admitted")
			}
			stored := h.samples[sample.Column][0]
			if h.Retained() != 1 || h.lastAsOf != 120 || stored.IC != sample.IC || stored.MatureAt != sample.MatureAt || stored.AvailableAt != sample.AvailableAt || stored.Samples != sample.Samples || stored.RankIC.Validity != factor.Missing {
				t.Fatal("conflict changed history")
			}
			if _, ready, err := h.Weights(120, []string{"momentum"}); err != nil || !ready {
				t.Fatalf("conflict blocked existing decision: %v", err)
			}
		})
	}
}

func TestICHistorySkipsFullyInvalidSamples(t *testing.T) {
	h, _ := NewICHistory(2, "forward", []string{"momentum"})
	sample := ICSample{Column: "momentum", Label: "forward", DecisionTime: 100, MatureAt: 110, AvailableAt: 115, IC: factor.Numeric{Value: math.NaN(), Validity: factor.Missing}, RankIC: factor.Numeric{Value: math.NaN(), Validity: factor.Null}, Samples: 24}
	for i := 0; i < 2; i++ {
		if err := h.Add(120, sample); err != nil {
			t.Fatal(err)
		}
	}
	if h.Retained() != 0 {
		t.Fatal("fully invalid sample retained")
	}
}
