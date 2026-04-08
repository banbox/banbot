package utils

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	"gonum.org/v1/gonum/stat"
)

const thresFloat64Eq = 1e-9

/*
NumSign
Obtain the direction of numbers; 1, -1 or 0
获取数字的方向；1，-1或0
*/
func NumSign(obj interface{}) int {
	if val, ok := obj.(int); ok {
		if val > 0 {
			return 1
		} else if val < 0 {
			return -1
		} else {
			return 0
		}
	} else if val, ok := obj.(float32); ok {
		if val > 0 {
			return 1
		} else if val < 0 {
			return -1
		} else {
			return 0
		}
	} else if val, ok := obj.(float64); ok {
		if val > 0 {
			return 1
		} else if val < 0 {
			return -1
		} else {
			return 0
		}
	} else {
		panic(fmt.Errorf("invalid type for NumSign: %t", obj))
	}
}

/*
EqualNearly
Determine whether two floats are approximately equal and solve the problem of inequality caused by floating-point close reading
判断两个float是否近似相等，解决浮点精读导致不等
*/
func EqualNearly(a, b float64) bool {
	return EqualIn(a, b, thresFloat64Eq)
}

/*
EqualIn
Determine whether two floats are approximately equal within a certain range
判断两个float是否在一定范围内近似相等
*/
func EqualIn(a, b, thres float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return math.Abs(a-b) <= thres
}

func NanInfTo(v, to float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return to
	}
	return v
}

func ConvertFloat64(i interface{}) float64 {
	switch v := i.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return 0
	}
}

func ConvertInt64(i interface{}) int64 {
	switch v := i.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

// Calculate the function of the greatest common divisor (GCD) of two numbers using the Euclidean algorithm
// 计算两个数的最大公约数（GCD）的函数，使用欧几里得算法
func gcd(a, b int) int {
	for b != 0 {
		t := b
		b = a % b
		a = t
	}
	return a
}

/*
GcdMultiple
Calculate the greatest common divisor of all elements in a slice
计算一个切片中所有元素的最大公约数
*/
func GcdInts(numbers []int) int {
	if len(numbers) == 0 {
		return 0 // 没有数时返回0
	}

	// Starting from the first number, calculate its greatest common divisor with the following numbers one by one
	// 从第一个数字开始，逐个将其与后面的数字进行最大公约数计算
	result := numbers[0]
	for i := 1; i < len(numbers); i++ {
		result = gcd(result, numbers[i])
	}
	return result
}

type Cluster struct {
	Center float64
	Items  []float64
}

type ClusterRes struct {
	Clusters []*Cluster
	RowGIds  []int
}

func KMeansVals(vals []float64, num int) *ClusterRes {
	if len(vals) == 0 {
		return nil
	}
	if num <= 1 || len(vals) == 1 {
		sumVal := float64(0)
		for _, v := range vals {
			sumVal += v
		}
		avgVal := sumVal / float64(len(vals))
		return &ClusterRes{
			Clusters: []*Cluster{{Center: avgVal, Items: vals}},
			RowGIds:  make([]int, len(vals)),
		}
	}
	if num > len(vals) {
		num = len(vals)
	}

	type pair struct {
		idx int
		val float64
	}
	sortedVals := make([]pair, len(vals))
	for i, v := range vals {
		sortedVals[i] = pair{idx: i, val: v}
	}
	slices.SortFunc(sortedVals, func(a, b pair) int {
		return cmp.Compare(a.val, b.val)
	})

	initCenters := func() []float64 {
		centers := make([]float64, num)
		if num == 1 {
			centers[0] = sortedVals[len(sortedVals)/2].val
			return centers
		}
		last := len(sortedVals) - 1
		for i := range centers {
			pos := int(math.Round(float64(i*last) / float64(num-1)))
			centers[i] = sortedVals[pos].val
		}
		return centers
	}

	centers := initCenters()
	assignments := make([]int, len(vals))
	counts := make([]int, num)
	sums := make([]float64, num)
	for iter := 0; iter < 64; iter++ {
		clear(counts)
		clear(sums)
		for i, v := range vals {
			best := 0
			bestDist := math.Abs(v - centers[0])
			for j := 1; j < len(centers); j++ {
				dist := math.Abs(v - centers[j])
				if dist < bestDist-thresFloat64Eq || (EqualNearly(dist, bestDist) && centers[j] < centers[best]) {
					best = j
					bestDist = dist
				}
			}
			assignments[i] = best
			counts[best]++
			sums[best] += v
		}
		changed := false
		for j := range centers {
			if counts[j] == 0 {
				pos := int(math.Round((float64(j) + 0.5) * float64(len(sortedVals)-1) / float64(len(centers))))
				next := sortedVals[pos].val
				if !EqualNearly(next, centers[j]) {
					centers[j] = next
					changed = true
				}
				continue
			}
			next := sums[j] / float64(counts[j])
			if !EqualNearly(next, centers[j]) {
				centers[j] = next
				changed = true
			}
		}
		slices.Sort(centers)
		if !changed {
			break
		}
	}

	resList := make([]*Cluster, num)
	for i, center := range centers {
		resList[i] = &Cluster{Center: center, Items: make([]float64, 0, counts[i])}
	}
	rowGids := make([]int, len(vals))
	for i, v := range vals {
		best := 0
		bestDist := math.Abs(v - centers[0])
		for j := 1; j < len(centers); j++ {
			dist := math.Abs(v - centers[j])
			if dist < bestDist-thresFloat64Eq || (EqualNearly(dist, bestDist) && centers[j] < centers[best]) {
				best = j
				bestDist = dist
			}
		}
		rowGids[i] = best
		resList[best].Items = append(resList[best].Items, v)
	}

	return &ClusterRes{
		Clusters: resList,
		RowGIds:  rowGids,
	}
}

/*
StdDevVolatility
Calculate the standard deviation of K-line volatility
Data: It should be a set of numbers with a slight fluctuation above and below the mean of 1
Rate: default 1. If you need to enlarge the difference, pass in a value between 1-1000
计算K线的波动性标准差

	data: 应该是均值为1的上下轻微浮动的一组数
	rate: 默认1，如需要放大差异，则传入1~1000之间的值
*/
func StdDevVolatility(data []float64, rate float64) float64 {
	totalNum := len(data)
	var logRates = make([]float64, 0, totalNum)
	var weights = make([]float64, 0, totalNum)
	for _, v := range data {
		logRate := math.Log(v) * rate
		logRates = append(logRates, logRate)
		weights = append(weights, 1)
	}
	stdDev := stat.StdDev(logRates, weights)
	return stdDev * math.Sqrt(float64(totalNum))
}

/*
NearScore
Formula: y=e ^ - abs (x-a)
At the given maxAt, i.e. a, y takes its maximum value of 1, x moves towards both sides, y gradually approaches 0, and the slope decreases from large to small
The default rate is 1, and when x is offset by 40, it decays by 70%; When rate>1, attenuation accelerates
公式：y=e^-abs(x-a)
在给定的maxAt即a处，y取最大值1，x向两侧移动，y逐渐趋近于0，斜率由大变小
rate默认1，x偏移40时，衰减70%；rate>1时，衰减加速
*/
func NearScore(x, mid, rate float64) float64 {
	return math.Exp(-math.Abs(x-mid) * rate * 0.03)
}

type ValIdx[T cmp.Ordered] struct {
	Val T
	Idx int
}

// ArgSortDesc 返回float64切片降序排序后对应的原始索引
func ArgSortDesc[T cmp.Ordered](values []T) []int {
	// 创建索引值对的切片
	indexed := make([]*ValIdx[T], len(values))
	for i, v := range values {
		indexed[i] = &ValIdx[T]{Val: v, Idx: i}
	}

	// 对索引值对进行排序
	slices.SortFunc(indexed, func(a, b *ValIdx[T]) int {
		return -cmp.Compare(a.Val, b.Val)
	})

	// 提取排序后的索引
	indices := make([]int, len(values))
	for i, v := range indexed {
		indices[i] = v.Idx
	}

	return indices
}

// CalcDrawDown calculate drawDownRate & drawDownVal for input real assets
func CalcDrawDown(reals []float64, viewNum int) (float64, float64) {
	var drawDownRate, maxReal, drawDownVal float64
	if len(reals) > 0 {
		stop := len(reals)
		step := 1
		if viewNum > 0 && stop*2 > viewNum*3 {
			step = max(1, int(math.Round(float64(stop)/float64(viewNum))))
		}
		maxReal = reals[0]
		for i := step; i < stop; i += step {
			val := reals[i]
			if val > maxReal {
				maxReal = val
			} else {
				drawDownVal = max(drawDownVal, maxReal-val)
				curDown := math.Abs(val/maxReal - 1)
				if curDown > drawDownRate {
					drawDownRate = curDown
				}
			}
		}
	}
	return drawDownRate, drawDownVal
}
