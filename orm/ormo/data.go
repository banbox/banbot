package ormo

import "github.com/sasha-s/go-deadlock"

var (
	accOpenODs     = map[string]map[int64]*InOutOrder{}            // all open orders: accName:orderId:order 全部打开的订单
	accSyncStamps  = map[string]int64{}                            // accName:lastSyncStamp 记录每个账户上次同步订单的时间戳
	accTriggerODs  = map[string]map[string]map[int64]*InOutOrder{} // Waiting for the limit order to be triggered, only for real trading . accName:pair:orders 等待触发限价单的订单，仅实盘使用；
	lockOpenMap    = map[string]*deadlock.Mutex{}                  // Access to the lock of accOpenODs 访问accOpenODs的锁
	lockTriggerMap = map[string]*deadlock.Mutex{}                  // Access to the lock of accTriggerODs 访问accTriggerODs的锁
	lockOds        = map[string]*deadlock.Mutex{}                  // Modify the lock of the order to prevent concurrent modification 修改订单的锁，防止并发修改
	mLockOds       deadlock.Mutex
	mOpenLock      deadlock.Mutex
	mTriggerLock   deadlock.Mutex
	HistODs        []*InOutOrder             // Historical orders, used as storage for backtesting. 历史订单，回测时作为存储用
	doneODs        = make(map[int64]bool)    // 用于防止重复添加到HistODs
	FakeOdId       = int64(1)                // Virtual order ID, used for temporary maintenance during backtesting. 虚拟订单ID，用于回测时临时维护
	OdEditListener func(*InOutOrder, string) // 订单修改的监听
	odSyncIntvMS   = int64(120000)           // 实时模式订单同步间隔时间，单位毫秒
)

const (
	InOutStatusInit = iota
	InOutStatusPartEnter
	InOutStatusFullEnter
	InOutStatusPartExit
	InOutStatusFullExit
	InOutStatusDelete
)

var InOutStatusMap = map[int64]string{
	InOutStatusInit:      "init",
	InOutStatusPartEnter: "partEnter",
	InOutStatusFullEnter: "fullEnter",
	InOutStatusPartExit:  "partExit",
	InOutStatusFullExit:  "fullExit",
	InOutStatusDelete:    "delete",
}

const (
	OdStatusInit = iota
	OdStatusPartOK
	OdStatusClosed
)

const (
	KeyStatusMsg = "status_msg"
)

const (
	OdInfoLegalCost    = "LegalCost"
	OdInfoStopAfter    = "StopAfter"
	OdInfoStopBars     = "StopBars"
	OdInfoStopLoss     = "StopLoss"
	OdInfoCallbackPct  = "CallbackPct"  // 跟踪止损回撤幅度，单位:百分之
	OdInfoActivePrice  = "ActivePrice"  // 跟踪止损激活价格
	OdInfoTrailingBest = "TrailingBest" // 跟踪止损利润最大时价格
	OdInfoTrailingID   = "TrailingID"   // 跟踪止损ID
	OdInfoTakeProfit   = "TakeProfit"
	OdInfoClientID     = "ClientID"
	OdInfoRetry        = "Retry"
	OdInfoSLTP         = "SL_TP"
)

const (
	OdActionEnter      = "Enter"
	OdActionExit       = "Exit"
	OdActionLimitEnter = "LimitEnter"
	OdActionLimitExit  = "LimitExit"
	OdActionStopLoss   = "StopLoss"
	OdActionTakeProfit = "TakeProfit"
	OdActionTrailing   = "Trailing"
)
