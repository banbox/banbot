package research

import "math"

// centerScaled replaces finite values with centered values in power-of-two
// units, returning the exponent needed to recover the original units. Scaling
// before subtraction handles opposite-sign extremes and subnormal values;
// centering relative to the first value preserves small differences near a
// large offset. Callers own the nonempty input buffer.
func centerScaled(values []float64) int {
	scale := 0.0
	for _, v := range values {
		scale = max(scale, math.Abs(v))
	}
	_, exponent := math.Frexp(scale)
	for i, v := range values {
		values[i] = math.Ldexp(v, -exponent)
	}
	anchor := values[0]
	mean := 0.0
	for i, v := range values {
		values[i] = v - anchor
		mean += values[i] / float64(len(values))
	}
	for i := range values {
		values[i] -= mean
	}
	return exponent
}
