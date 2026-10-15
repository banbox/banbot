package statistics

import (
	"fmt"
	"math"
)

// LocalLinearTrend has x[t]=[[1,1],[0,1]]x[t-1]+w[t],
// y[t]=[1,0]x[t]+v[t], independent level/slope process noise and scalar R.
// Mean and covariance are the previous observation's posterior.
type LocalLinearTrend struct {
	Level, Slope                                      float64
	Covariance                                        [2][2]float64
	LevelVariance, SlopeVariance, ObservationVariance float64
}

type TrendObservation struct{ Prediction, Innovation, InnovationVariance, NextPrediction float64 }

func (k *LocalLinearTrend) Observe(y float64) (TrendObservation, error) {
	if k == nil {
		return TrendObservation{}, fmt.Errorf("statistics: nil trend filter")
	}
	p := k.Covariance
	vals := []float64{y, k.Level, k.Slope, p[0][0], p[0][1], p[1][0], p[1][1], k.LevelVariance, k.SlopeVariance, k.ObservationVariance}
	for _, v := range vals {
		if !finite(v) {
			return TrendObservation{}, fmt.Errorf("statistics: nonfinite trend input")
		}
	}
	if p[0][0] < 0 || p[1][1] < 0 || math.Abs(p[0][1]-p[1][0]) > 1e-12 || p[0][0]*p[1][1] < p[0][1]*p[1][0] || k.LevelVariance < 0 || k.SlopeVariance < 0 || k.ObservationVariance <= 0 {
		return TrendObservation{}, fmt.Errorf("statistics: invalid trend covariance/noise")
	}
	a := [2][2]float64{{p[0][0] + p[0][1] + p[1][0] + p[1][1] + k.LevelVariance, p[0][1] + p[1][1]}, {p[1][0] + p[1][1], p[1][1] + k.SlopeVariance}}
	pred := k.Level + k.Slope
	innovation := y - pred
	variance := a[0][0] + k.ObservationVariance
	gain := [2]float64{a[0][0] / variance, a[1][0] / variance}
	level, slope := pred+gain[0]*innovation, k.Slope+gain[1]*innovation
	// Joseph form retains PSD even when observation noise is small.
	m := [2][2]float64{{1 - gain[0], 0}, {-gain[1], 1}}
	var post [2][2]float64
	for i := 0; i < 2; i++ {
		for j := 0; j < 2; j++ {
			for l := 0; l < 2; l++ {
				for n := 0; n < 2; n++ {
					post[i][j] += m[i][l] * a[l][n] * m[j][n]
				}
			}
			post[i][j] += gain[i] * k.ObservationVariance * gain[j]
		}
	}
	post[0][1] = (post[0][1] + post[1][0]) / 2
	post[1][0] = post[0][1]
	for _, v := range []float64{level, slope, variance, post[0][0], post[0][1], post[1][1], level + slope} {
		if !finite(v) {
			return TrendObservation{}, fmt.Errorf("statistics: trend update overflow")
		}
	}
	k.Level, k.Slope, k.Covariance = level, slope, post
	return TrendObservation{pred, innovation, variance, level + slope}, nil
}
