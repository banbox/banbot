package biz

import (
	"context"
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"math"
	"sync"
	"testing"
)

/*
*
ETH/USDT:USDT current price: 3426.9050
buy at 3769.5955 would cost at least 0 secs to fill 100.0%
buy at 3423.4781 would cost at least 112 secs to fill 100.0%
buy at 3420.0512 would cost at least 252 secs to fill 100.0%
buy at 3409.7705 would cost at least 525 secs to fill 71.0%
buy at 3392.6359 would cost at least 525 secs to fill 35.5%
buy at 3324.0978 would cost at least 525 secs to fill 11.8%
buy at 3255.5597 would cost at least 525 secs to fill 7.1%
buy at 3084.2145 would cost at least 525 secs to fill 3.5%
*/
func TestCalcSecsForPrice(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}
	_, err = orm.LoadMarkets(exg.Default, false)
	if err != nil {
		panic(err)
	}
	pair := "ETH/USDT:USDT"
	side := banexg.OdSideBuy
	priceRates := []float64{-0.1, 0.001, 0.002, 0.005, 0.01, 0.03, 0.05, 0.1}
	err = orm.EnsureCurSymbols([]string{pair})
	if err != nil {
		panic(err)
	}
	avgVol, lastVol, err := getPairMinsVol(pair, 50)
	if err != nil {
		panic(err)
	}
	secsVol := max(avgVol, lastVol) / 60
	if secsVol == 0 {
		panic(err)
	}
	book, err := exg.GetOdBook(pair)
	if err != nil {
		panic(err)
	}
	curPrice := book.Asks.Price[0]*0.5 + book.Bids.Price[0]*0.5
	fmt.Printf("%s current price: %.4f \n", pair, curPrice)
	dirt := float64(1)
	if side == banexg.OdSideBuy {
		dirt = float64(-1)
	}
	for _, rate := range priceRates {
		price := curPrice * (1 + rate*dirt)
		waitVol, rate := book.SumVolTo(side, price)
		minWaitSecs := int(math.Round(waitVol / secsVol))
		fmt.Printf("%s at %.4f would cost at least %d secs to fill %.1f%%\n",
			side, price, minWaitSecs, rate*100)
	}
}

func TestOrmQueries(t *testing.T) {
	err := initApp()
	if err != nil {
		panic(err)
	}

	// 固定测试1000次
	totalTests := 1000
	// 默认并发数为1，可通过修改此值测试不同并发
	concurrency := 1
	// 单个goroutine执行的次数
	testsPerGoroutine := totalTests / concurrency

	// 剩余待测试次数，使用锁保护
	var mu sync.Mutex
	remainingTests := totalTests

	// 收集每次测试的耗时
	type result struct {
		elapsed int64
		err     *errs.Error
	}
	results := make(chan result, totalTests)

	_, _ = runOrmoTest()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 每个goroutine执行指定次数
			for j := 0; j < testsPerGoroutine; j++ {
				// 检查是否还有剩余测试次数
				mu.Lock()
				if remainingTests <= 0 {
					mu.Unlock()
					return
				}
				remainingTests--
				mu.Unlock()

				elapsed, err := runOrmoTest()
				results <- result{elapsed: elapsed, err: err}
			}
		}(i)
	}
	wg.Wait()
	close(results)

	// 收集结果并计算统计
	var total int64
	var max int64
	var min int64 = 999999
	var successCount int
	var failCount int

	var lastErr *errs.Error
	for res := range results {
		if res.err != nil || res.elapsed < 0 {
			failCount++
			lastErr = res.err
			continue
		}
		successCount++
		total += res.elapsed
		if res.elapsed > max {
			max = res.elapsed
		}
		if res.elapsed < min {
			min = res.elapsed
		}
	}

	fmt.Printf("\n=== Conn Performance Test ===\n")
	fmt.Printf("Total Tests: %d\n", totalTests)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Tests per Goroutine: %d\n", testsPerGoroutine)
	fmt.Printf("Success: %d\n", successCount)
	fmt.Printf("Failed: %d\n", failCount)
	if lastErr != nil {
		fmt.Printf("Last error: %s\n", lastErr.Error())
	}
	if successCount > 0 {
		avg := total / int64(successCount)
		fmt.Printf("Average: %d ms\n", avg)
		fmt.Printf("Min: %d ms\n", min)
		fmt.Printf("Max: %d ms\n", max)
		fmt.Printf("Total Time: %d ms\n", total)
	}
}

func runOrmoTest() (int64, *errs.Error) {
	start := btime.TimeMS()

	// 获取数据库连接
	sess, conn, err := ormo.Conn(orm.DbTrades, true)
	if err != nil {
		return -1, err
	}

	elapsed := btime.TimeMS() - start

	// 执行简单查询
	_, _ = sess.GetTask(context.Background(), 1)
	conn.Close()
	return elapsed, nil
}
