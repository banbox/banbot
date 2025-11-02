package ormo

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
	"maps"
)

var (
	accTasks     = make(map[string]*BotTask)
	taskIdAccMap = make(map[int64]string)
)

func Conn(path string, write bool) (*Queries, *orm.TrackedDB, *errs.Error) {
	db, err := orm.DbLite(orm.DbTrades, path, write, 5000)
	if err != nil {
		return nil, nil, err
	}
	return New(db), db, nil
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
	if !core.EnvReal {
		account = config.DefAcc
	}
	isReload := false
	mOpenLock.Lock()
	if core.LiveMode {
		cfg, ok := config.Accounts[account]
		if ok && cfg.NoTrade {
			return make(map[int64]*InOutOrder), &deadlock.Mutex{}
		}
		curMS := btime.UTCStamp()
		stamp, _ := accSyncStamps[account]
		if curMS-stamp > odSyncIntvMS {
			// 超过同步间隔，强制同步一次
			accSyncStamps[account] = curMS
			isReload = true
		}
	}
	val, ok := accOpenODs[account]
	if !ok {
		val = make(map[int64]*InOutOrder)
		accOpenODs[account] = val
	}
	mOpenLock.Unlock()
	if isReload {
		err := loadOpenODs(account, val)
		if err != nil {
			log.Error("loadOpenODs fail", zap.String("acc", account), zap.Error(err))
		}
	}
	mOpenLock.Lock()
	lock, ok2 := lockOpenMap[account]
	if !ok2 {
		lock = &deadlock.Mutex{}
		lockOpenMap[account] = lock
	}
	mOpenLock.Unlock()
	return val, lock
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
	if len(missKeys) > 0 || len(dupKeys) > 0 {
		if btime.UTCStamp()-core.StartAt > 180000 {
			// 启动超过3分钟，才打印日志，避免启动时的噪声
			log.Error("loadOpenODs diff", zap.String("acc", account), zap.Any("dup", dupKeys),
				zap.Any("miss", missKeys))
		}
	}
	return nil
}

func GetTriggerODs(account string) (map[string]map[int64]*InOutOrder, *deadlock.Mutex) {
	if !core.EnvReal {
		account = config.DefAcc
	}
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
	mOpenLock.Lock()
	for accKey, ods := range accOpenODs {
		if account != "" && accKey != account {
			continue
		}
		lock, _ := lockOpenMap[accKey]
		lock.Lock()
		for key, od := range ods {
			if od.IsDirty() {
				dirtyOds = append(dirtyOds, od)
			}
			if od.Status >= InOutStatusFullExit {
				delete(ods, key)
			}
		}
		lock.Unlock()
	}
	mOpenLock.Unlock()
	if len(dirtyOds) == 0 {
		return nil
	}
	sess, conn, err := Conn(path, true)
	if err != nil {
		return err
	}
	defer conn.Close()
	var odErr *errs.Error
	for _, od := range dirtyOds {
		err = od.Save(sess)
		if err != nil {
			odErr = err
			log.Error("save unMatch od fail", zap.String("key", od.Key()), zap.Error(err))
		}
	}
	return odErr
}

func ResetVars() {
	accOpenODs = make(map[string]map[int64]*InOutOrder)
	accTriggerODs = make(map[string]map[string]map[int64]*InOutOrder)
	lockOpenMap = make(map[string]*deadlock.Mutex)
	lockTriggerMap = make(map[string]*deadlock.Mutex)
	lockOds = make(map[string]*deadlock.Mutex)
	accTasks = make(map[string]*BotTask)
	taskIdAccMap = make(map[int64]string)
}

// VarsBackup 存储 ormo 包相关全局变量的备份
type VarsBackup struct {
	AccOpenODs    map[string]map[int64]*InOutOrder
	AccTriggerODs map[string]map[string]map[int64]*InOutOrder
	AccTasks      map[string]*BotTask
	TaskIdAccMap  map[int64]string
}

// BackupVars 备份所有全局变量
func BackupVars() *VarsBackup {
	return &VarsBackup{
		AccOpenODs:    accOpenODs,
		AccTriggerODs: accTriggerODs,
		AccTasks:      accTasks,
		TaskIdAccMap:  taskIdAccMap,
	}
}

// RestoreVars 从备份中恢复所有全局变量
func RestoreVars(backup *VarsBackup) {
	if backup == nil {
		return
	}
	accOpenODs = backup.AccOpenODs
	accTriggerODs = backup.AccTriggerODs
	accTasks = backup.AccTasks
	taskIdAccMap = backup.TaskIdAccMap
}
