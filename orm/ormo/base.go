package ormo

import (
	"maps"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

var (
	accTasks     = make(map[string]*BotTask)
	taskIdAccMap = make(map[int64]string)
)

func Conn(path string, write bool) (*Queries, *orm.TrackedDB, *errs.Error) {
	db, err := orm.DbLite(orm.DbTrades, path, write, 10000)
	if err != nil {
		return nil, nil, err
	}
	return New(db), db, nil
}

func (s *OrderState) BindTradesPath(path string) {
	s.tradesPath = path
}

func (s *OrderState) Conn(write bool) (*Queries, *orm.TrackedDB, *errs.Error) {
	if s == nil || s == legacyOrderState {
		return Conn(orm.DbTrades, write)
	}
	if s.tradesPath == "" {
		return nil, nil, errs.NewMsg(core.ErrDbConnFail, "runtime trades database path is required")
	}
	return Conn(s.tradesPath, write)
}

func GetTaskID(account string) int64 {
	task := GetTask(account)
	if task != nil {
		return task.ID
	}
	return -1
}

func GetTask(account string) *BotTask {
	if !core.EnvReal {
		account = config.DefAcc
	}
	if task, ok := accTasks[account]; ok {
		return task
	}
	return nil
}

func GetTaskAcc(id int64) string {
	if acc, ok := taskIdAccMap[id]; ok {
		return acc
	}
	return ""
}

func GetOpenODs(account string) (map[int64]*InOutOrder, *deadlock.Mutex) {
	openOrders, lock, _ := GetOpenODsStatus(account)
	return openOrders, lock
}

// GetOpenODsStatus is the order snapshot API for callers that must fail closed
// when a live database reload was not successful. The legacy GetOpenODs API
// intentionally keeps its two-return-value contract for the rest of Banbot.
func GetOpenODsStatus(account string) (map[int64]*InOutOrder, *deadlock.Mutex, bool) {
	if !core.EnvReal {
		account = config.DefAcc
	} else if account == "" {
		log.Warn("get open ods fail, unknown account")
		return make(map[int64]*InOutOrder), &deadlock.Mutex{}, false
	}
	isReload := false
	authoritative := true
	mOpenLock.Lock()
	if core.LiveMode {
		cfg, ok := config.Accounts[account]
		if ok && cfg.NoTrade {
			mOpenLock.Unlock()
			return make(map[int64]*InOutOrder), &deadlock.Mutex{}, true
		}
		curMS := btime.UTCStamp()
		stamp, _ := accSyncStamps[account]
		authoritative = stamp > 0
		if curMS-stamp > odSyncIntvMS {
			// 超过同步间隔，强制同步一次
			accSyncStamps[account] = curMS
			isReload = true
			authoritative = false
		}
	}
	val, ok := accOpenODs[account]
	if !ok {
		val = make(map[int64]*InOutOrder)
		accOpenODs[account] = val
	}
	lock, ok2 := lockOpenMap[account]
	if !ok2 {
		lock = &deadlock.Mutex{}
		lockOpenMap[account] = lock
	}
	mOpenLock.Unlock()

	if isReload {
		err := loadOpenODs(account, val)
		if err != nil {
			log.Error("loadOpenODs fail", zap.String("acc", account), zap.Error(err))
			// Keep retrying and keep the snapshot explicitly non-authoritative
			// until a later reload succeeds.
			mOpenLock.Lock()
			accSyncStamps[account] = -max(btime.UTCStamp(), int64(1))
			mOpenLock.Unlock()
			authoritative = false
		} else {
			authoritative = true
		}
	}
	return val, lock, authoritative
}

func loadOpenODs(account string, odMap map[int64]*InOutOrder) *errs.Error {
	sess, conn, err := Conn(orm.DbTrades, false)
	if err != nil {
		return err
	}
	defer conn.Close()
	taskId := GetTaskID(account)
	orders, err := sess.GetOrders(GetOrdersArgs{
		TaskID: taskId,
		Status: 1,
	})
	if err != nil {
		return err
	}
	var missKeys = map[int64]string{}
	var dump = maps.Clone(odMap)
	for _, od := range orders {
		if _, ok := odMap[od.ID]; !ok {
			odMap[od.ID] = od
			missKeys[od.ID] = od.Key()
		} else {
			delete(dump, od.ID)
		}
	}
	var dupKeys = make(map[int64]string)
	if len(dump) > 0 {
		for key, od := range dump {
			dupKeys[key] = od.Key()
		}
	}
	if btime.UTCStamp()-core.StartAt > 180000 {
		// 启动超过3分钟，才打印日志，避免启动时的噪声
		if len(dupKeys) > 0 {
			// dup: 内存有但DB没有，属于真正的数据不一致
			log.Error("loadOpenODs diff", zap.String("acc", account), zap.Any("dup", dupKeys),
				zap.Any("miss", missKeys))
		} else if len(missKeys) > 0 {
			// miss: DB有但内存没有，属于正常的同步补充行为，已添加到内存
			log.Info("loadOpenODs sync", zap.String("acc", account), zap.Any("miss", missKeys))
		}
	}
	return nil
}

func GetTriggerODs(account string) (map[string]map[int64]*InOutOrder, *deadlock.Mutex) {
	if !core.EnvReal {
		account = config.DefAcc
	}
	mTriggerLock.Lock()
	val, ok := accTriggerODs[account]
	if !ok {
		val = make(map[string]map[int64]*InOutOrder)
		accTriggerODs[account] = val
	}
	lock, ok2 := lockTriggerMap[account]
	if !ok2 {
		lock = &deadlock.Mutex{}
		lockTriggerMap[account] = lock
	}
	mTriggerLock.Unlock()
	return val, lock
}

func AddTriggerOd(account string, od *InOutOrder) {
	triggerOds, lock := GetTriggerODs(account)
	lock.Lock()
	ods, ok := triggerOds[od.Symbol]
	if !ok {
		ods = make(map[int64]*InOutOrder)
		triggerOds[od.Symbol] = ods
	}
	ods[od.ID] = od
	lock.Unlock()
}

/*
OpenNum
Returns the number of open orders that match the specified status
返回符合指定状态的尚未平仓订单的数量
*/
func OpenNum(account string, status int64) int {
	openNum := 0
	openOds, lock := GetOpenODs(account)
	lock.Lock()
	for _, od := range openOds {
		if od.Status >= status {
			openNum += 1
		}
	}
	lock.Unlock()
	return openNum
}

/*
SaveDirtyODs
Find unsaved orders from open orders and save them all to the database
从打开的订单中查找未保存的订单，全部保存到数据库
*/
func SaveDirtyODs(path string, account string) *errs.Error {
	var dirtyOds []*InOutOrder
	// 避免在持有 mOpenLock 时获取 lockOpenMap 锁导致死锁
	type accData struct {
		accKey string
		ods    map[int64]*InOutOrder
		lock   *deadlock.Mutex
	}
	var accList []accData
	mOpenLock.Lock()
	for accKey, ods := range accOpenODs {
		if account != "" && accKey != account {
			continue
		}
		lock, _ := lockOpenMap[accKey]
		accList = append(accList, accData{accKey: accKey, ods: ods, lock: lock})
	}
	mOpenLock.Unlock()

	// 在 mOpenLock 释放后，逐个处理每个账户
	for _, acc := range accList {
		acc.lock.Lock()
		for key, od := range acc.ods {
			if od.IsDirty() {
				dirtyOds = append(dirtyOds, od)
			}
			if od.Status >= InOutStatusFullExit {
				delete(acc.ods, key)
			}
		}
		acc.lock.Unlock()
	}

	if len(dirtyOds) == 0 {
		return nil
	}
	var odErr *errs.Error
	for _, od := range dirtyOds {
		err := od.Save()
		if err != nil {
			odErr = err
			log.Error("save unMatch od fail", zap.String("key", od.Key()), zap.Error(err))
		}
	}
	return odErr
}
