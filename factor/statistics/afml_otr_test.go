package statistics

import (
	"reflect"
	"testing"
)

func TestAFMLOUExitGridDirectionalAndSeeded(t *testing.T) {
	c := OUExitConfig{Forecast: 5, HalfLife: 5, Sigma: 1, ProfitTaking: []float64{1, 3}, StopLoss: []float64{1, 3}, Trials: 3000, MaxHolding: 20, Seed: 7, MaxDraws: 300000}
	positive, err := SimulateOUExitGrid(c)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := SimulateOUExitGrid(c)
	if err != nil || !reflect.DeepEqual(positive, repeated) {
		t.Fatalf("OU simulation not reproducible: %v", err)
	}
	c.Forecast = -5
	negative, err := SimulateOUExitGrid(c)
	if err != nil {
		t.Fatal(err)
	}
	for i := range positive {
		if !positive[i].SharpeValid || !negative[i].SharpeValid || positive[i].Mean <= 0 || negative[i].Mean >= 0 {
			t.Fatalf("OU forecast direction lost: %+v %+v", positive[i], negative[i])
		}
	}
	c.MaxDraws = 10
	if _, err = SimulateOUExitGrid(c); err == nil {
		t.Fatal("unbounded simulation accepted")
	}
}

func TestAFMLOUExitGridSymmetry(t *testing.T) {
	c := OUExitConfig{HalfLife: 10, Sigma: 1, ProfitTaking: []float64{2}, StopLoss: []float64{2}, Trials: 20000, MaxHolding: 30, Seed: 11, MaxDraws: 620000}
	got, err := SimulateOUExitGrid(c)
	if err != nil {
		t.Fatal(err)
	}
	// Zero equilibrium and symmetric barriers should have no systematic payout.
	if got[0].Mean < -.08 || got[0].Mean > .08 || !got[0].SharpeValid {
		t.Fatalf("symmetric OU exit payout biased: %+v", got[0])
	}
}
