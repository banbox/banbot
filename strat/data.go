package strat

import (
	ta "github.com/banbox/banta"
	"github.com/sasha-s/go-deadlock"
)

/*
下面变量中所有的stratName都是RunPolicy.ID()，不是原始策略名。后面添加了":l"或":s"后缀表示仅开多或仅开空
*/

var (
	Versions    = make(map[string]int)                             // stratName: int 策略版本号
	Envs        = make(map[string]*ta.BarEnv)                      // pair_tf: BarEnv
	TmpEnvs     = make(map[string]*ta.BarEnv)                      // pair_tf: BarEnv
	AccJobs     = make(map[string]map[string]map[string]*StratJob) // account: pair_tf: [stratID]StratJob
	AccInfoJobs = make(map[string]map[string]map[string]*StratJob) // account: pair_tf: [stratID_pair]StratJob 额外订阅
	PairStrats  = make(map[string]map[string]*TradeStrat)          // pair:[stratID]TradeStrat 所有的订阅策略，注意有些策略对象虽然Name相同但不是同一个实例
	ForbidJobs  = make(map[string]map[string]bool)                 // pair_tf: [stratID] occupy
	WsSubJobs   = make(map[string]map[string]map[*StratJob]bool)   // msgType: pair: job

	BatchTasks  = map[string]*BatchMap{} // tf_account_strat: pair: task 每个bar周期更新（只适用于单交易所单市场）
	LastBatchMS = int64(0)               // timeMS The timestamp of the last batch execution is only used for backtesting 上次批量执行的时间戳，仅用于回测

	lockJobs     deadlock.RWMutex
	lockInfoJobs deadlock.Mutex
	lockTmpEnv   deadlock.Mutex

	accOdSubs = map[string][]FnOdChange{} // acc: listeners List of subscription order status change events 订阅订单状态变化事件列表
	lockOdSub deadlock.Mutex

	accFailOpens    = make(map[string]map[string]int) // Statistics of reasons for failed entry for accounts 各个账号开单失败原因统计
	lockAccFailOpen deadlock.Mutex

	WsSubUnWatch func(map[string][]string)
)

var (
	FailOpenCostTooLess    = "CostTooLess"
	FailOpenBadDirtOrLimit = "BadDirtOrLimit"
	FailOpenNanNum         = "NanNum"
	FailOpenBadStopLoss    = "BadStopLoss"
	FailOpenBadTakeProfit  = "BadTakeProfit"
	FailOpenBarTooLate     = "BarTooLate"
	FailOpenNoEntry        = "NoEntry"
	FailOpenNumLimit       = "NumLimit"
	FailOpenNumLimitPol    = "NumLimitPol"
)
