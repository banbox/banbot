package statistics

import (
	"reflect"
	"testing"
)

func TestChanMovingVolumeBoundariesAndTradeConservation(t *testing.T) {
	prices := []float64{100, 101, 102, 103}
	volumes := []float64{500, 500, 500, 200}
	got, err := MovingVolumeBarPrices(prices, volumes, 500, 4)
	if err != nil || !reflect.DeepEqual(got, []float64{100, 101, 102, 103}) {
		t.Fatalf("moving partial volume terminals %v %v", got, err)
	}
	// Another300 contracts shift every boundary; no new synthetic trade price.
	got, err = MovingVolumeBarPrices(append(prices, 104), append(volumes, 300), 500, 4)
	if err != nil || !reflect.DeepEqual(got, []float64{100, 101, 102, 104}) {
		t.Fatalf("exact boundaries %v %v", got, err)
	}
	got, err = MovingVolumeBarPrices([]float64{100, 105}, []float64{1, 2000}, 500, 5)
	if err != nil || !reflect.DeepEqual(got, []float64{100, 105, 105, 105, 105}) {
		t.Fatal("oversized tick split invented a price")
	}
}
