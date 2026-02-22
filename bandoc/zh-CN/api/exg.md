# exg 包

exg 包提供了交易所接口和交易相关的功能。

## 函数列表

### Setup
初始化交易所设置。

返回：
- `*errs.Error` - 如果初始化过程中发生错误则返回错误信息，否则返回 nil

### GetWith
根据指定的交易所名称、市场和合约类型获取交易所实例。

参数：
- `name string` - 交易所名称
- `market string` - 市场类型
- `contractType string` - 合约类型

返回：
- `banexg.BanExchange` - 交易所实例
- `*errs.Error` - 如果获取过程中发生错误则返回错误信息，否则返回 nil

### PrecCost
根据交易所的精度要求，处理交易成本金额。

参数：
- `exchange banexg.BanExchange` - 交易所实例
- `symbol string` - 交易对符号
- `cost float64` - 原始成本金额

返回：
- `float64` - 按照交易所精度处理后的成本金额
- `*errs.Error` - 如果处理过程中发生错误则返回错误信息，否则返回 nil

### PrecPrice
根据交易所的精度要求，处理交易价格。

参数：
- `exchange banexg.BanExchange` - 交易所实例
- `symbol string` - 交易对符号
- `price float64` - 原始价格

返回：
- `float64` - 按照交易所精度处理后的价格
- `*errs.Error` - 如果处理过程中发生错误则返回错误信息，否则返回 nil

### PrecAmount
根据交易所的精度要求，处理交易数量。

参数：
- `exchange banexg.BanExchange` - 交易所实例
- `symbol string` - 交易对符号
- `amount float64` - 原始数量

返回：
- `float64` - 按照交易所精度处理后的数量
- `*errs.Error` - 如果处理过程中发生错误则返回错误信息，否则返回 nil

### GetLeverage
获取指定交易对和名义价值下的杠杆率。

参数：
- `symbol string` - 交易对符号
- `notional float64` - 名义价值
- `account string` - 账户标识

返回：
- `float64, float64` - 返回两个浮点数，分别表示杠杆率的两个相关值

### GetOdBook
获取指定交易对的订单簿数据。

参数：
- `pair string` - 交易对符号

返回：
- `*banexg.OrderBook` - 订单簿数据
- `*errs.Error` - 如果获取过程中发生错误则返回错误信息，否则返回 nil

### GetTickers
获取所有交易对的行情数据。

返回：
- `map[string]*banexg.Ticker` - 以交易对为键的行情数据映射
- `*errs.Error` - 如果获取过程中发生错误则返回错误信息，否则返回 nil

### GetAlignOff
获取指定交易所和时间周期的对齐偏移量。

参数：
- `exgName string` - 交易所名称
- `tfSecs int` - 时间周期（以秒为单位）

返回：
- `int` - 对齐偏移量
