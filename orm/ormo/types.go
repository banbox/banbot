package ormo

type ExitTrigger struct {
	Price float64 `json:"price,omitempty"` // Trigger Price 触发价格
	Limit float64 `json:"limit,omitempty"` // Submit limit order price after triggering, otherwise market order. 触发后提交限价单价格，否则市价单
	Rate  float64 `json:"rate,omitempty"`  // Stop-profit and stop-loss ratio, (0,1], 0 means all. 止盈止损比例，(0,1]，0表示全部
	Tag   string  `json:"tag,omitempty"`   // Reason, used for ExitTag. 原因，用于ExitTag
}

type TriggerState struct {
	*ExitTrigger
	Range    float64      `json:"range,omitempty"` // The stop-profit and stop-loss range is the range from the entry price to the exit price. 止盈止损区间，入场价格到离场价格的区间
	Hit      bool         `json:"hit,omitempty"`   // whether trigger price has been triggered? 是否已触发
	OrderId  string       `json:"order_id,omitempty"`
	ClientId string       `json:"client_id,omitempty"`
	Old      *ExitTrigger `json:"old,omitempty"`
}
