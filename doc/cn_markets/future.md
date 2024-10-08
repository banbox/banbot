期货交易是买卖约定未来某时某地得到某物的一份合约，不是当下的实物。交割期货的时间可以是一星期后，一个月后，几个月或一年后。  
期货合约的商品品种、交易单位、合约月份、保证金、数量、质量、等级、交货时间、交货地点等条款都是既定的，是标准化的，唯一的变量是价格。  
期货的交易单位是：手，必须以“一手”的整数倍进行交易。不同品种一手的商品数量不同。  
波动点位：期货计算盈亏一般按点位，不同品种一个点对应不同价格。  
最小变动价位：不同品种有不同的最小价格波动点位。  
结算价：当天交易结束后，对未平仓合约进行当日盈亏结算的基准价。商品期货：当日成交量加权平均价；金融期货：最后一小时成交量加权平均价；估值期货：最后2小时现货平均价。  
张跌停板：期货合约在一个交易日中的价格不得高于或低于前一日结算价的某一幅度；商品期货4%-7%；股指期货10%；国债期货0.5%-2%；连续涨跌停时，下个交易日将会扩板，即增大涨跌停板幅度。  
完整交易日：从前一个交易日晚9点到当天下午3点结束。（周五晚9点~下周一15点是一个交易日；法定节假日前一个晚上没有夜盘交易）  
最后交易日:某一期货合约在合约交割月份中进行交易的最后一个交易日，过后必须进行实物交割或现金交割。  
自然人最后交易日：大商所和郑商所自然人不能进入交割月；上期所大部分品种自然人需在最后交易日的前第三个交易日平仓；能源所自然人需要再最后交易日前第八个交易日平仓。
由于临近交割月后，合约保证金会大幅提高，所以尽量操作主力合约。  
双向交易：期货既可以做多，也可以做空。  
保证金交易：期货交易秩序缴纳少量保证金，一般为合约价值的5%~20%  
T+0：期货交易可以当天开仓当天平仓，持仓不到1s都可以。  
无负债结算制度：每天交易结束后，交易所按当日结算价结算所有合约盈亏、保证金、手续费税金等费用。（当日浮盈可用作次日开仓）  
移仓：卖掉临近交割合约同时买入远期合约。  


### 主力连续合约与指数合约
由于商品期货合约存续的特殊性，所以一般会对每个品种提供主力连续合约和指数合约两个人工合成的合约。  
主力合约：首次上市的品种以当日持仓量最大者作为后续交易日的主力合约。当其他合约持仓量在收盘后超过主力合约1.1倍时，在下个交易日进行主力合约切换。  
* 主力连续合约：由该品种不同时期主力合约拼接而成，代码以888结尾表示复权平滑处理，代码以88结尾未做平滑处理。  
* 指数合约（加权合约）：由该期货品种所有正在交易的合约，以持仓量加权平均计算。  
> 指数合约分999和000两种，000是按各个合约绝对价格以持仓量为加权计算的；999是先设定过一个初值，然后根据涨跌相对变动计算，同样也按持仓量加权。

### 复权
复权，是与除权相对的一个操作。“复”，表示对“除”这个动作的恢复。  
除权，就是在每股分给你多少钱以后，从每股的股价里面减去这么多钱。  
复权分为前复权和后复权，大部分炒股软件都是按前复权显示的，也就是对以前的数据进行处理，最新的数据保持不变。  
前复权按计算方式分为等差复权和等比复权。等差复权即计算相邻价格差值，对以前的数据进行加/减；等比复权即计算相邻价格比值因子，对以前的价格乘以因子。  

**连续前复权等比计算方案**  
复权因子=新价格/旧价格  
对于第n批K线，要得到复权后的价格，只需将此日期后所有因子相乘，然后乘到第n批所有价格上即可。  

### 期货合约规范
* 上期/能源所：小写+4个数字（年后2位+月份2位）
* 大商所：小写+4个数字
* 郑商所：大写+3个数字（年后1位+月份2位）
* 中金所：大写+4个数字

### 期权合约规范
* 上期所/能源所：小写+4个数字+C(或者P)+行权价
* 郑商所：大写+3个数字+C(或者P)+行权价
* 中金所：大写+4个数字+-C-(或者-P-)+行权价
* 大商所：小写+4个数字+-C-(或者-P-)+行权价

### 期货套利
#### 跨期套利
同一市场、同一品种，分别买卖不同月份合约，从价差中获利。
#### 跨品种套利
分别买卖品种不同，但有一定相关性的两种期货，从价差中获利。
#### 跨市套利
分别买卖同一品种，不同市场的两个期货，从价差中获利。
#### 套利交易几大原则
买卖方向对应、买卖数量相等、同时建仓平仓、合约相关性

### 手续费
构成：交易所手续费+期货公司加收部分+投资者保障基金部分。  
期货有些品种按比例收取，有些按每手固定金额收取。很多品种对日内平仓见面手续费。  


## 回测仿真
### 主力连续与复权
主力连续合约是不同时间段的主力合约拼接而成，在拼接的间隙有跳空；回测时一般需要对前面或后面数据进行复权抹平跳空，避免产生假信号。下面是回测的几种处理方案：  
**1. 分批前复权（采用）**  
始终保持回测时间对应的价格不变，对前面的旧K线复权抹平跳空；当主力切换时，重新复权切换前的必要数据并预热。  
优点：始终使用真实价格，无需映射；完全模拟实盘，绩效准确。  
缺点：主力切换需要重新计算前复权数据并预热。  
**2. 简单前/后复权**  
前复权：从后往前，复权因子累乘，作为旧日期的因子；旧数据乘以因子得到无跳空的复权K线，直接回测。  
后复权：从前往后，复权因子累乘，作为新日期的因子；新数据除以因子得到无跳空的复权K线，直接回测。  
缺点：历史价格非真实价格，绩效可能不能准确反映。  
升级版：回测下单时，计算复权价格对应的真实价格，以真实价格下单。  
**3. 分批不复权**  
主力切换时，使用新主力的前面数据预热，回测的所有时间点始终使用真实价格。  
缺点：新主力前面的K线成交量太少，K线变化不明朗，可能影响回测的准确性。  
















