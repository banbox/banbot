package biz

import (
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banexg/log"
)

// TelegramOrderManager 实现 rpc.OrderManagerInterface 接口
type TelegramOrderManager struct{}

// NewTelegramOrderManager 创建 Telegram 订单管理器
func NewTelegramOrderManager() *TelegramOrderManager {
	return &TelegramOrderManager{}
}

// GetActiveOrders 获取活跃订单列表
func (m *TelegramOrderManager) GetActiveOrders(account string) ([]*rpc.OrderInfo, error) {
	sess, conn, err := ormo.Conn(orm.DbTrades, false)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	taskId := ormo.GetTaskID(account)
	orders, getErr := sess.GetOrders(ormo.GetOrdersArgs{
		TaskID: taskId,
		Status: 1, // 1表示未平仓
	})

	if getErr != nil {
		return nil, getErr
	}

	var result []*rpc.OrderInfo
	for _, order := range orders {
		orderInfo := &rpc.OrderInfo{
			ID:       order.ID,
			Symbol:   order.Symbol,
			Short:    order.Short,
			EnterTag: order.EnterTag,
			Account:  account,
		}

		if order.Enter != nil {
			orderInfo.Price = order.Enter.Average
			orderInfo.Amount = order.Enter.Filled
		}

		result = append(result, orderInfo)
	}

	return result, nil
}

// CloseOrder 平仓指定订单
func (m *TelegramOrderManager) CloseOrder(account string, orderID int64) error {
	_, err := CloseBotOrdersRemote(RemoteCommand{
		Source:    RemoteSourceTelegram,
		Actor:     "telegram",
		Account:   account,
		OrderID:   orderID,
		ExitTag:   "telegram_close",
		Confirmed: true,
	})
	return err
}

// CloseAllOrders 平仓所有订单
func (m *TelegramOrderManager) CloseAllOrders(account string) (int, int, error) {
	res, err := CloseBotOrdersRemote(RemoteCommand{
		Source:    RemoteSourceTelegram,
		Actor:     "telegram",
		Account:   account,
		All:       true,
		ExitTag:   "telegram_close_all",
		Confirmed: true,
	})
	if err != nil {
		return 0, 0, err
	}
	return res.CloseNum, res.FailNum, nil
}

func (m *TelegramOrderManager) DisableTrading(account string, hours int) (int64, error) {
	res, err := RunRemoteCommand(RemoteCommand{
		Source:       RemoteSourceTelegram,
		Actor:        "telegram",
		Account:      account,
		Action:       RemoteActionTradingSwitch,
		DisableHours: hours,
	})
	if err != nil {
		return 0, err
	}
	return res.UntilMS, nil
}

func (m *TelegramOrderManager) EnableTrading(account string) error {
	_, err := RunRemoteCommand(RemoteCommand{
		Source:  RemoteSourceTelegram,
		Actor:   "telegram",
		Account: account,
		Action:  RemoteActionTradingSwitch,
		Enable:  true,
	})
	return err
}

// GetOrderStats 获取订单统计信息
func (m *TelegramOrderManager) GetOrderStats(account string) (longCount, shortCount int, err error) {
	sess, conn, connErr := ormo.Conn(orm.DbTrades, false)
	if connErr != nil {
		return 0, 0, connErr
	}
	defer conn.Close()

	taskId := ormo.GetTaskID(account)
	orders, getErr := sess.GetOrders(ormo.GetOrdersArgs{
		TaskID: taskId,
		Status: 1, // 1表示未平仓
	})

	if getErr != nil {
		return 0, 0, getErr
	}

	for _, order := range orders {
		if order.Short {
			shortCount++
		} else {
			longCount++
		}
	}

	return longCount, shortCount, nil
}

// InitTelegramOrderManager 初始化 Telegram 订单管理器
func InitTelegramOrderManager() {
	orderMgr := NewTelegramOrderManager()
	rpc.SetOrderManager(orderMgr)

	// 注册钱包信息提供者
	rpc.SetWalletInfoProvider(walletInfoProvider{})

	log.Info("Telegram order manager initialized")
}

// walletInfoProvider 实现钱包汇总接口
type walletInfoProvider struct{}

func (walletInfoProvider) GetSummary(account string) (totalLegal float64, availableLegal float64, unrealizedPOLLegal float64) {
	w := GetWallets(account)
	return w.TotalLegal(nil, true), w.AvaLegal(nil), w.UnrealizedPOLLegal(nil)
}
