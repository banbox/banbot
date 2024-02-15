package orm

var (
	OpenODs    = map[int64]*InOutOrder{} // 全部打开的订单
	HistODs    []*InOutOrder             // 历史订单，回测时作为存储用
	UpdateOdMs int64                     // 上次刷新OpenODs的时间戳
	FakeOdId   = int64(1)                // 虚拟订单ID，用于回测时临时维护
)

const (
	InOutStatusInit = iota
	InOutStatusPartEnter
	InOutStatusFullEnter
	InOutStatusPartExit
	InOutStatusFullExit
)

const (
	OdStatusInit = iota
	OdStatusPartOK
	OdStatusClosed
)

const (
	KeyStopLossPrice   = "stop_loss_price"
	KeyTakeProfitPrice = "take_profit_price"
	KeyStatusMsg       = "status_msg"
)