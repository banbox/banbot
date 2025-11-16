package biz

import (
	"fmt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
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
	mgr := GetOdMgr(account)
	if mgr == nil {
		return fmt.Errorf("account invalid: %v", account)
	}

	sess, conn, err := ormo.Conn(orm.DbTrades, false)
	if err != nil {
		return err
	}
	defer conn.Close()

	// 查找订单
	taskId := ormo.GetTaskID(account)
	orders, getErr := sess.GetOrders(ormo.GetOrdersArgs{
		TaskID: taskId,
		Status: 1, // 1表示未平仓
	})

	if getErr != nil {
		return getErr
	}

	// 在订单列表中查找指定ID的订单
	var order *ormo.InOutOrder
	for _, o := range orders {
		if o.ID == orderID {
			order = o
			break
		}
	}

	if order == nil {
		return fmt.Errorf("order not found: %v", orderID)
	}

	// 创建平仓请求
	exitReq := &strat.ExitReq{
		Tag:     "telegram_close",
		OrderID: order.ID,
		Force:   true,
	}

	// 执行平仓
	_, exitErr := mgr.ExitOpenOrders(order.Symbol, exitReq)
	if exitErr != nil {
		return exitErr
	}
	return nil
}

// CloseAllOrders 平仓所有订单
func (m *TelegramOrderManager) CloseAllOrders(account string) (int, int, error) {
	mgr := GetOdMgr(account)
	if mgr == nil {
		return 0, 0, fmt.Errorf("account invalid: %v", account)
	}

	sess, conn, err := ormo.Conn(orm.DbTrades, false)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	// 获取所有活跃订单
	taskId := ormo.GetTaskID(account)
	orders, getErr := sess.GetOrders(ormo.GetOrdersArgs{
		TaskID: taskId,
		Status: 1, // 1表示未平仓
	})

	if getErr != nil {
		return 0, 0, getErr
	}

	successCount := 0
	failedCount := 0

	for _, order := range orders {
		exitReq := &strat.ExitReq{
			Tag:     "telegram_close_all",
			OrderID: order.ID,
			Force:   true,
		}

		_, exitErr := mgr.ExitOpenOrders(order.Symbol, exitReq)
		if exitErr != nil {
			log.Error("Failed to close order", zap.Int64("order_id", order.ID), zap.Error(exitErr))
			failedCount++
		} else {
			successCount++
		}
	}

	return successCount, failedCount, nil
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
