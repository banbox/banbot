package utils

import "math"

/*
CalcExpectancy
Calculate expected returns and expectancy ratio
计算收益期望、收益期望比率
*/
func CalcExpectancy(profits []float64) (float64, float64) {
	if len(profits) == 0 {
		return 0, 0
	}

	var winNum, lossNum float64
	var profitSum, lossSum float64

	for _, val := range profits {
		if val >= 0 {
			winNum += 1
			profitSum += val
		} else {
			lossNum += 1
			lossSum -= val // Store the absolute value of losses
		}
	}
	var avgWin, avgLoss float64
	if winNum > 0 {
		avgWin = profitSum / winNum
	}
	if lossNum > 0 {
		avgLoss = lossSum / lossNum
	}

	totalNum := float64(len(profits))
	winRate := winNum / totalNum
	lossRate := lossNum / totalNum

	expectancy := (winRate * avgWin) - (lossRate * avgLoss)

	var expectancyRatio float64
	if avgLoss > 0 {
		riskRewardRatio := avgWin / avgLoss
		expectancyRatio = ((1 + riskRewardRatio) * winRate) - 1
	}

	return expectancy, expectancyRatio
}

// calcDrawDowns calculates the cumulative values, highs, drawdowns, and drawdown percentages.
func calcDrawDowns(profits []float64, initBalance float64) ([]float64, []float64, []float64, []float64) {
	cumulative := make([]float64, len(profits))  // Accumulated income 累计收益
	highs := make([]float64, len(profits))       // Highest cumulative income 累计收益最高值
	drawdown := make([]float64, len(profits))    // Cumulative maximum loss per cycle (negative) 每个周期累计最大亏损（负数）
	drawdownPct := make([]float64, len(profits)) // Withdrawal percentage per cycle 每个周期回撤百分比

	for i := 0; i < len(profits); i++ {
		if i == 0 {
			cumulative[i] = profits[i]
			highs[i] = cumulative[i]
		} else {
			cumulative[i] = cumulative[i-1] + profits[i]
			highs[i] = max(cumulative[i], highs[i-1])
		}
	}

	// Calculate drawdowns and drawdown percentages
	for i := 0; i < len(cumulative); i++ {
		drawdown[i] = cumulative[i] - highs[i]
		if initBalance != 0 {
			cumulativeBalance := initBalance + cumulative[i]
			maxBalance := initBalance + highs[i]
			drawdownPct[i] = (maxBalance - cumulativeBalance) / maxBalance
		} else {
			drawdownPct[i] = (highs[i] - cumulative[i]) / highs[i]
		}
	}

	return cumulative, highs, drawdown, drawdownPct
}

/*
CalcMaxDrawDown
Calculate maximum drawdown
Calculate the maximum drawdown percentage position when initBalance is provided; No location provided for calculating the maximum loss
Return: Maximum drawdown percentage, maximum drawdown amount, maximum value index, minimum value index, maximum value, minimum value
计算最大回撤
initBalance 提供时计算最大回撤百分比位置；未提供计算亏损最多时位置
返回：最大回撤百分比、最大回撤额、最大值索引、最小值索引、最大值、最小值
*/
func CalcMaxDrawDown(profits []float64, initBalance float64) (float64, float64, int, int, float64, float64) {
	cumulative, highs, drawdown, drawdownPct := calcDrawDowns(profits, initBalance)

	var idxMin int // 最低资产索引
	if initBalance > 0 {
		// Provide initial balance and index for calculating maximum drawdown percentage
		// 提供初始余额，计算最大回撤百分比时的索引
		idxMin = argMax(drawdownPct)
	} else {
		// Initial balance not provided, index when calculating maximum drawdown
		// 未提供初始余额，计算回撤最多时索引
		idxMin = argMin(drawdown)
	}

	if idxMin < 0 || idxMin >= len(cumulative) {
		return 0, 0, -1, -1, 0, 0
	}

	idxMax := argMax(highs[:idxMin+1])
	highVal := cumulative[idxMax]
	lowVal := cumulative[idxMin]

	ddPct, ddVal := drawdownPct[idxMin], math.Abs(drawdown[idxMin])
	return ddPct, ddVal, idxMax, idxMin, highVal, lowVal
}

// Helper function to find the index of the maximum value in a slice.
func argMax(arr []float64) int {
	maxIdx := 0
	for i, v := range arr {
		if v > arr[maxIdx] {
			maxIdx = i
		}
	}
	return maxIdx
}

// Helper function to find the index of the minimum value in a slice.
func argMin(arr []float64) int {
	minIdx := 0
	for i, v := range arr {
		if v < arr[minIdx] {
			minIdx = i
		}
	}
	return minIdx
}
