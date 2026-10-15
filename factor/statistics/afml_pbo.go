package statistics

import (
	"errors"
	"math"
	"math/big"
)

// CSCVMetric scores one trial's time-ordered synchronous performance samples;
// larger scores are better. It must be deterministic and not retain the slice.
type CSCVMetric func([]float64) (float64, error)

type CSCVSplit struct {
	TrainGroups                        []int
	Selected                           int
	InSample, OutOfSample, Rank, Logit float64
}

type CSCVResult struct {
	Probability float64
	Splits      []CSCVSplit
}

// CombinatorialSymmetricCV follows chapter11.6 on an already generated
// synchronous performance matrix (rows=time, columns=trials). It does NOT
// train models or substitute for purged event CV. Rows must divide evenly into
// the even number of contiguous groups. Every half-group combination AND its
// complement is evaluated, preserving order within each half.
// Tied IS optima choose the first trial; tied OOS scores use average ranks.
// Rank/(N+1) stays strictly inside (0,1) and assigns median rank a zero logit.
// Probability is the source's empirical Pr(logit<=0), including median ties.
func CombinatorialSymmetricCV(performance [][]float64, groups, maxSplits int, metric CSCVMetric) (CSCVResult, error) {
	if len(performance) == 0 || groups < 2 || groups%2 != 0 || groups > len(performance) || len(performance)%groups != 0 || maxSplits < 1 || metric == nil {
		return CSCVResult{}, errors.New("statistics: invalid CSCV groups, matrix or metric")
	}
	trials := len(performance[0])
	if trials < 2 {
		return CSCVResult{}, errors.New("statistics: CSCV needs at least two trials")
	}
	for _, row := range performance {
		if len(row) != trials {
			return CSCVResult{}, errors.New("statistics: CSCV needs a synchronous rectangular matrix")
		}
		for _, value := range row {
			if !afmlFinite(value) {
				return CSCVResult{}, errors.New("statistics: CSCV performance must be finite")
			}
		}
	}
	half := groups / 2
	count := new(big.Int).Binomial(int64(groups), int64(half))
	if count.Cmp(big.NewInt(int64(maxSplits))) > 0 {
		return CSCVResult{}, errors.New("statistics: CSCV exceeds split budget")
	}
	result := CSCVResult{Splits: make([]CSCVSplit, 0, int(count.Int64()))}
	comb := make([]int, half)
	for i := range comb {
		comb[i] = i
	}
	blockSize, belowMedian := len(performance)/groups, 0
	for {
		inTrain := make([]bool, groups)
		for _, group := range comb {
			inTrain[group] = true
		}
		trainScores, testScores := make([]float64, trials), make([]float64, trials)
		for trial := 0; trial < trials; trial++ {
			train, test := make([]float64, 0, len(performance)/2), make([]float64, 0, len(performance)/2)
			for row, values := range performance {
				if inTrain[row/blockSize] {
					train = append(train, values[trial])
				} else {
					test = append(test, values[trial])
				}
			}
			var err error
			trainScores[trial], err = metric(train)
			if err != nil {
				return CSCVResult{}, err
			}
			testScores[trial], err = metric(test)
			if err != nil {
				return CSCVResult{}, err
			}
			if !afmlFinite(trainScores[trial]) || !afmlFinite(testScores[trial]) {
				return CSCVResult{}, errors.New("statistics: CSCV metric produced nonfinite score")
			}
		}
		selected := 0
		for trial := 1; trial < trials; trial++ {
			if trainScores[trial] > trainScores[selected] {
				selected = trial
			}
		}
		less, equal := 0, 0
		for _, score := range testScores {
			if score < testScores[selected] {
				less++
			} else if score == testScores[selected] {
				equal++
			}
		}
		rank := (float64(less) + (float64(equal)+1)/2) / float64(trials+1)
		logit := math.Log(rank / (1 - rank))
		result.Splits = append(result.Splits, CSCVSplit{append([]int(nil), comb...), selected, trainScores[selected], testScores[selected], rank, logit})
		if logit <= 0 {
			belowMedian++
		}
		at := half - 1
		for at >= 0 && comb[at] == groups-half+at {
			at--
		}
		if at < 0 {
			break
		}
		comb[at]++
		for i := at + 1; i < half; i++ {
			comb[i] = comb[i-1] + 1
		}
	}
	result.Probability = float64(belowMedian) / float64(len(result.Splits))
	return result, nil
}

// CSCVSharpe is an unannualized sample mean/sample-standard-deviation metric.
// Zero-variance columns are undefined and rejected, not assigned infinite SR.
func CSCVSharpe(values []float64) (float64, error) {
	if len(values) < 2 {
		return 0, errors.New("statistics: CSCV Sharpe needs two observations")
	}
	mean, m2 := 0., 0.
	for i, value := range values {
		if !afmlFinite(value) {
			return 0, errors.New("statistics: CSCV Sharpe needs finite observations")
		}
		delta := value - mean
		mean += delta / float64(i+1)
		m2 += delta * (value - mean)
	}
	if !afmlFinite(m2) || m2 <= 0 {
		return 0, errors.New("statistics: CSCV Sharpe variance is invalid or zero")
	}
	sharpe := mean / math.Sqrt(m2/float64(len(values)-1))
	if !afmlFinite(sharpe) {
		return 0, errors.New("statistics: CSCV Sharpe overflow")
	}
	return sharpe, nil
}
