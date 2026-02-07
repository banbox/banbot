package opt

import (
	"math/rand"
	"testing"

	ta "github.com/banbox/banta"
)

// BenchmarkSingleThreadIndicators 测试单线程下常规指标计算的性能
// 用于对比添加锁前后的性能变化
func BenchmarkSingleThreadIndicators(b *testing.B) {
	// 准备测试数据 - 增加到10000条K线
	dataSize := 10000
	bars := make([]*ta.Kline, dataSize)

	basePrice := 50000.0
	baseTime := int64(1600000000000) // 起始时间戳(毫秒)
	for i := 0; i < dataSize; i++ {
		change := (rand.Float64() - 0.5) * 1000
		open := basePrice + change
		high := open + rand.Float64()*500
		low := open - rand.Float64()*500
		close := low + rand.Float64()*(high-low)
		volume := rand.Float64() * 1000

		bars[i] = &ta.Kline{
			Time:   baseTime + int64(i)*60000,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  close,
			Volume: volume,
		}
		basePrice = close
	}

	b.ResetTimer()

	// 运行基准测试
	for n := 0; n < b.N; n++ {
		// 创建新的BarEnv
		env := &ta.BarEnv{
			TimeFrame: "1m",
			TFMSecs:   60000,
		}

		// 模拟真实使用场景：逐个添加K线并计算指标
		for _, bar := range bars {
			env.OnBar(bar.Time, bar.Open, bar.High, bar.Low, bar.Close, bar.Volume, 0, 0, 0)

			// 每个K线都计算常用指标
			_ = ta.SMA(env.Close, 5)
			_ = ta.SMA(env.Close, 20)
			_ = ta.EMA(env.Close, 20)
			_ = ta.RSI(env.Close, 14)
			_ = ta.ATR(env.High, env.Low, env.Close, 14)
			macd, signal := ta.MACD(env.Close, 12, 26, 9)
			_ = macd.Cross(signal)
			_, _, _ = ta.KDJ(env.High, env.Low, env.Close, 9, 3, 3)
		}
	}
}
