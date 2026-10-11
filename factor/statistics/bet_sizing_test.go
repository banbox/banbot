package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestProbabilityBetSizeSourceAndMetaSide(t *testing.T) {
	for _, row := range []struct {
		p, prediction, want float64
		classes, side       int
	}{{.5, 1, 0, 2, 1}, {1, -1, -1, 2, 1}, {0, 1, -1, 2, 1}, {.5, 0, 0, 3, -1}} {
		got, err := ProbabilityBetSize(row.p, row.prediction, row.classes, row.side)
		if err != nil || got != row.want {
			t.Fatalf("endpoint/prior: %g %v", got, err)
		}
	}
	ordinary, _ := ProbabilityBetSize(.8, 1, 2, 1)
	meta, _ := ProbabilityBetSize(.8, 1, 2, -1)
	closeTo(t, ordinary, math.Erf(.75/math.Sqrt2))
	closeTo(t, meta, -ordinary)
	multi, _ := ProbabilityBetSize(.8, 1, 3, 1)
	if multi <= ordinary {
		t.Fatal("one-versus-rest prior not used")
	}
	if _, err := ProbabilityBetSize(math.NaN(), 1, 2, 1); err == nil {
		t.Fatal("NaN probability accepted")
	}
}

func TestActiveBetAveragingUsesExclusiveEndAndKnownOpenBets(t *testing.T) {
	bets := []ActiveBet{{Start: 1, End: 3, Size: 1}, {Start: 2, End: 4, Size: -.5}, {Start: 3, Open: true, Size: .5}}
	got, err := AverageActiveBetSizes(bets, []int64{0, 1, 2, 3, 4, 5})
	if err != nil || !reflect.DeepEqual(got, []float64{0, 1, .25, 0, .5, .5}) {
		t.Fatalf("active sizes %v %v", got, err)
	}
	bets = append(bets, ActiveBet{Start: 10, Open: true, Size: -1})
	same, _ := AverageActiveBetSizes(bets, []int64{0, 1, 2, 3, 4, 5})
	if !reflect.DeepEqual(got, same) {
		t.Fatal("future-issued bet changed prefix")
	}
}

func TestBetDiscretizationUsesSourceNearestEvenAndClips(t *testing.T) {
	for _, row := range []struct{ size, want float64 }{{.125, 0}, {.375, .5}, {-.125, 0}, {-.375, -.5}, {1.2, 1}, {-1.2, -1}} {
		got, err := DiscretizeBetSize(row.size, .25)
		if err != nil || got != row.want {
			t.Fatalf("size=%g got=%g want=%g err=%v", row.size, got, row.want, err)
		}
	}
	if _, err := DiscretizeBetSize(1, 0); err == nil {
		t.Fatal("zero step accepted")
	}
}

func TestDynamicBetSizingSourceNumericExampleAndShortSymmetry(t *testing.T) {
	width, err := CalibrateBetWidth(10, .95)
	if err != nil {
		t.Fatal(err)
	}
	size, _ := SigmoidBetSize(width, 10)
	closeTo(t, size, .95)
	position, _ := DynamicBetPosition(width, 115, 100, 100)
	if position != 97 {
		t.Fatalf("source target=%d", position)
	}
	limit, err := DynamicBetLimitPrice(position, 0, 115, width, 100)
	if err != nil || math.Abs(limit-112.3657) > .0001 {
		t.Fatalf("source limit=%g %v", limit, err)
	}
	short, _ := DynamicBetLimitPrice(-position, 0, 115, width, 100)
	closeTo(t, limit+short, 230)
	for _, divergence := range []float64{-15, 0, 15} {
		m, _ := SigmoidBetSize(width, divergence)
		price, _ := InverseBetPrice(115, width, m)
		closeTo(t, price, 115-divergence)
	}
	flip, err := DynamicBetLimitPrice(-2, 2, 115, width, 100)
	if err != nil || flip <= 115 {
		t.Fatalf("signed zero crossing lost: %g %v", flip, err)
	}
	large, err := SigmoidBetSize(width, math.MaxFloat64)
	if err != nil || !finite(large) || large <= 0 {
		t.Fatalf("valid large divergence: %g %v", large, err)
	}
	if _, err := DynamicBetLimitPrice(100, 0, 115, width, 100); err == nil {
		t.Fatal("singular endpoint accepted")
	}
}
