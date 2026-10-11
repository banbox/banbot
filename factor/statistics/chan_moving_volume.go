package statistics

import (
	"fmt"
	"math"
)

// MovingVolumeBarPrices rebuilds equal-volume bar terminal prices backwards
// from the latest observed trade volume. Unlike fixed historical buckets, every
// new trade shifts all prior volume boundaries. A boundary uses the actual
// trade price containing that one-based cumulative contract position.
func MovingVolumeBarPrices(prices, volumes []float64, bucketVolume float64, terminals int) ([]float64, error) {
	if len(prices) == 0 || len(prices) != len(volumes) || !finite(bucketVolume) || bucketVolume < 1 || math.Trunc(bucketVolume) != bucketVolume || terminals < 2 || terminals > 100002 {
		return nil, fmt.Errorf("statistics: invalid moving volume-bar dimensions")
	}
	cumulative := make([]float64, len(volumes))
	total := 0.0
	for i, volume := range volumes {
		if !finite(prices[i]) || prices[i] <= 0 || !finite(volume) || volume <= 0 || math.Trunc(volume) != volume {
			return nil, fmt.Errorf("statistics: positive prices/integer contract volumes required")
		}
		total += volume
		if total > 9007199254740991 || !finite(total) {
			return nil, fmt.Errorf("statistics: moving contract-volume precision exceeded")
		}
		cumulative[i] = total
	}
	first := total - float64(terminals-1)*bucketVolume
	if first <= 0 {
		return nil, fmt.Errorf("statistics: insufficient moving volume history")
	}
	out := make([]float64, terminals)
	index := 0
	for j := range out {
		boundary := first + float64(j)*bucketVolume
		for cumulative[index] < boundary {
			index++
		}
		out[j] = prices[index]
	}
	return out, nil
}
