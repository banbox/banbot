banbot有非常丰富的配置选项，默认情况下，这些都是通过`yaml`配置文件配置的。

## 数据目录
在每次启动回测或实盘时，您都需要传入`-datadir`参数，即数据目录。您也可以配置环境变量`BanDataDir`，然后忽略`-datadir`参数。

您可以在数据目录中存放若干个yaml配置文件。回测时还会自动在数据目录下创建`backtest`子目录用于存储回测结果。

## Yaml配置文件
banbot可以从命令行参数中接收若干个配置文件路径，后面的配置文件如果有和前面相同的配置，前面的会被覆盖。
您可以将配置文件分为多个部分，依次传入，方便维护。

#### 默认配置文件
banbot会尝试从数据目录下默认读取两个配置文件：`config.yml`和`config.local.yml`。
您可直接创建这两个配置文件，在启动banbot时无需传入，将会自动读取。

`config.yml`中建议存放比较全的不需要经常修改的默认配置。  
`config.local.yml`中建议存放本地相关的配置，如交易所api/secret，api_server等配置。

## 完整Yaml配置
> 您可使用环境变量替换配置中的敏感内容，比如`${bnb_user1_key}`

```yaml
name: local  # 机器人名称，用于在消息通知中区分不同机器人
env: prod  # 运行环境，prod表示生产网络，test表示测试网络，dry_run表示模拟实盘交易
leverage: 2  # 杠杆倍数，仅期货合约市场有效
limit_vol_secs: 5  # 按成交量取订单簿价格的过期时间，单位：秒，默认10
put_limit_secs: 120  # 在此预期时间内能成交的限价单，才提交到交易所，单位：秒，默认120
account_pull_secs: 60  # 定期更新账户余额和持仓的间隔秒数，默认60秒
market_type: spot  # 市场：spot现货，linear：U本位合约，inverse：币本位合约，option：期权合约
contract_type: swap # swao永续合约，future到期合约
odbook_ttl: 1000  # 订单簿过期时间，单位毫秒，默认500
concur_num: 2  # 标的K线并发下载数，默认2个标的
order_type: market  # 下单类型：market市价单  limit限价单
stop_enter_bars: 20  # 限价入场单超过多少个蜡烛未成交则取消入场，默认0不启用
prefire: 0  # 是否在bar即将完成时，提前10%时间触发
margin_add_rate: 0.66  # 交易合约时，如出现亏损，亏损达到初始保证金比率的此值时，进行追加保证金，避免强平，默认0.66
stake_amount: 15  # 单笔订单默认本金，优先级低于stake_pct
stake_pct: 50 # 单笔开单金额百分比，名义价值
max_stake_amt: 5000 # 单笔上限5k，仅stake_pct有值时有效
draw_balance_over: 0  # 余额超过此值时，超出部分自动提现，不用于后续交易，仅用于回测
charge_on_bomb: false # 回测爆仓时自动充值继续回测
take_over_strat: ma:demo # 实盘时接管用户开单的策略，默认为空
close_on_stuck: 20  # 超时20分钟未收到K线时自动全部平仓，默认20。（仅实盘生效）
open_vol_rate: 1 # 未指定数量开单时，最大允许开单数量/平均蜡烛成交量的比值，默认1
min_open_rate: 0.5 # 最小开单比率，余额不足单笔金额时，余额/单笔金额高于此比率允许开单，默认0.5即50%
low_cost_action: ignore # 开单金额不足最小金额时的动作：ignore/keepBig/keepAll
max_simul_open: 0 # 在一个bar上最大同时打开订单数量
bt_net_cost: 15 # 回测时下单延迟，可用于模拟滑点，单位：秒，默认15
relay_sim_unfinish: false  # 交易新品种时(回测/实盘)，是否从开始时间未平仓订单接力开始交易
order_bar_max: 500  # 查找开始时间未平仓订单向前模拟最大bar数量
ntp_lang_code: none  # ntp真实时间同步，默认none不启用，支持的代码：zh-CN, zh-HK, zh-TW, ja-JP, ko-KR, zh-SG, global(表示全球ntp服务器：google、apple、facebook...)
wallet_amounts:  # 钱包余额，用于回测
  USDT: 10000
stake_currency: [USDT, TUSD]  # 限定只交易定价币为这些的交易对
fatal_stop:  # 全局止损，当全局损失达到限制时，禁止下单
  '1440': 0.1  # 一天损失10%
  '180': 0.2  # 3小时损失20%
  '30': 0.3  # 半小时损失30%
fatal_stop_hours: 8  # 触发全局止损时，禁止开单的小时；默认8
time_start: "20240701"  # K线起始时间，支持时间戳、日期、日期时间等，用于回测、数据导出等
time_end: "20250701"
run_timeframes: [5m]  # 机器人允许运行的所有时间周期。策略会从中选择适合的最小周期，此处优先级低于run_policy
run_policy:  # 运行的策略，可以多个策略同时运行；也可以一个策略配置不同参数同时运行多个版本
  - name: Demo  # 策略名称
    run_timeframes: [5m]  # 此策略支持的时间周期，提供时覆盖根层级的run_timeframes
    refine_tf: 1m  # 撮合周期，字符串或数字，数字表示相对timeframes减少倍数：'1m', '5m', '3-6', 5
    filters:  # 可使用pairlists中的所有过滤器
    - name: OffsetFilter  # 偏移限定数量选择。一般用在最后
      offset: 10  # 从第10个开始取
      limit: 30  # 最多取30个
    max_pair: 999  # 此策略允许的最大标的数量
    max_open: 10  # 此策略最大开单数量
    max_simul_open: 0  # 在一个bar上，最大同时打开订单数量
    order_bar_max: 0  # 非0时覆盖全局默认order_bar_max
    stake_rate: 1 # 此策略的开单倍率
    stop_loss: 0  # 此策略的止损比率，如 5% 或 0.05
    dirt: any # any/long/short
    pairs: [BTC]
    params: {atr: 15}
    pair_params:
      BTC: {atr:14}
    strat_perf: # 和根strat_perf配置相同
      enable: false
strat_perf:
  enable: false # 是否启用策略币对效果追踪，自动降低亏损较多的币种开单金额
  min_od_num: 5 # 最小5，默认5，少于5个不计算性能
  max_od_num: 30 # 最大job订单数，最小8，默认30
  min_job_num: 10 # 最小标的数量，默认10，最小7
  mid_weight: 0.2 # 收益中间档的开单权重
  bad_weight: 0.1 # 收益较差档开单权重
pairs: [SOL, UNFI, SFP]  # 给定交易币种，如不为空，pairlists会被忽略
pairmgr:
  cron: '25 1 0 */2 * *' # 秒 分钟 小时 天 月 星期
  offset: 0  # 标的列表忽略前n个
  limit: 999 # 标的列表最多保留n个
  force_filters: false  # 是否对pairs应用pairlists，默认false
  pos_on_rotation: hold  # hold/close 品种列表切换时，持仓保留还是立刻平仓
  use_latest: false  # 未启用cron时，是否使用最新时间刷新品种，仅对回测生效
pairlists:  # 交易对过滤器，按从上到下的顺序逐个过滤应用。
  - name: VolumePairList  # 按成交量价值倒序排序所有交易对
    limit: 100  # 取前100个
    limit_rate: 1 # 按比例截取
    min_value: 100000  # 最低成交量价值
    cache_secs: 7200  # 缓存时间
    back_period: 3d  # 计算成交量时间周期
  - name: PriceFilter  # 价格过滤器
    max_unit_value: 100  # 最大允许的单位价格变动对应的价值(针对定价货币，一般是USDT)。
    precision: 0.0015  # 按价格精度过滤交易对，默认要求价格变动最小单位是0.1%
    min: 0.001  # 最低价格
    max: 100000  # 最高价格
  - name: RateOfChangeFilter  # 波动性过滤器
    back_days: 5  # 回顾的K线天数
    min: 0.03  # 最小价格变动比率
    max: 10  # 最大价格变动比率
    cache_secs: 1440  # 缓存时间，秒
  - name: SpreadFilter  # 流动性过滤器
    max_ratio: 0.005  # 公式：1-bid/ask，买卖价差占价格的最大比率
  - name: CorrelationFilter  # 相关性过滤器
    min: -1  # 用于过滤当前币种与全市场平均相关性；默认0，表示不启用
    max: 1  # 用于过滤当前币种与全市场平均相关性；默认0，表示不启用
    timeframe: 5m  # 用于计算相关性的数据周期
    back_num: 70  # 计算相关性回顾数据长度
    sort: asc  # asc/desc/""
    top_n: 50  # 只返回相关性最低的前n个币种，默认0不限制
  - name: VolatilityFilter  # 波动性过滤器，公式：std(log(c/c1)) * sqrt(back_days)
    back_days: 10  # 回顾的K线天数
    max: 1  # 波动分数最大值，此值越大，允许一些在1d级别上变化非常剧烈的标的
    min: 0.05  # 波动分数最小值，此值越小，允许一些在1d级别上变化非常小的标的
  - name: AgeFilter  # 按标的的上市天数过滤
    min: 5
  - name: BlockFilter
    pairs: [BTC]
  - name: OffsetFilter  # 偏移限定数量选择。一般用在最后
    reverse: false  # reverse array
    offset: 10  # 从第10个开始取
    rate: 0.5  # 50% of array
    limit: 30  # 最多取30个
  - name: ShuffleFilter  # 随机打乱
    seed: 42  # 随机数种子，可选
accounts:
  user1:  # 这里是账户名字，可任意，会在rpc消息中使用
    no_trade: false  # 是否禁止此账户交易
    stake_rate: 1  # 开单金额倍率，相对于默认的
    leverage: 0  # 合约杠杆，优先级高于默认的
    max_stake_amt: 0  # 允许的单笔最大金额
    max_pair: 0  # 此账户允许的最大品种数量
    max_open_orders: 0  # 此账户允许的最大同时持仓订单数
    binance:
      prod:  # 生产网络的key和secret，指定env: prod时此项必填
        api_key: ${bnb_user1_key}
        api_secret: ${bnb_user1_secret}
      test:  # 测试网络的key和secret，指定env: test时此项必填
        api_key: vvv
        api_secret: vvv
    okx:
      prod:
        api_key: ${okx_user1_key}
        api_secret: ${okx_user1_secret}
        password: ${okx_user1_password}
    bybit:
      prod:
        api_key: ${bybit_user1_key}
        api_secret: ${bybit_user1_secret}
    rpc_channels:  # 通过社交app控制，或发通知到社交app
      - name: wx_bot
        to_user: ChannelUserID
    api_server:  # 通过Dashboard访问的密码和角色
      pwd: abc
      role: admin  # 可选admin/guest
exchange:  # 交易所配置
  name: binance  # 当前使用的交易所
  binance:  # 这里传入banexg初始化交易所的参数，key会自动从蛇形转为驼峰。
    # proxy: http://127.0.0.1:10808
    fees:
      linear:  # 键可以是：linear/inverse/main(spot or margin)
        taker: 0.0005
        maker: 0.0002
database:  # 数据库配置(QuestDB，启动时自动下载安装)
  retention: all
  max_pool_size: 50  # 连接池最大大小
  auto_create: true  # 数据库不存在时，是否自动创建
  url: postgresql://admin:quest@127.0.0.1:8812/qdb?sslmode=disable
  qdb_mem_pct: 0.3 # QuestDB内存使用占比，范围0~1
  qdb_max_mem_mb: 16384 # 数据库最大内存使用量，单位MB
spider_addr: 127.0.0.1:6789  # 爬虫监听的端口和地址
rpc_channels:  # 支持的全部rpc渠道
  # 邮件通知渠道
  mail_notify:  # 渠道名称（可自定义）
    type: mail  # 渠道类型：mail
    disable: false  # 是否禁用此渠道
    msg_types: [exception]  # 消息类型：exception（异常）
    accounts: []  # 指定账户（空表示所有账户）
    keywords: []  # 关键词过滤（空表示不过滤）
    retry_delay: 1000  # 重试延迟（毫秒）
    min_intv_secs: 300  # 最小发送间隔（秒）
    touser: 'recipient@abc.com'  # 收件人邮箱地址
  # 企业微信通知渠道
  wx_notify:  # rpc的渠道名
    corp_id: ww0f5246485154fgf
    agent_id: '100034234'
    corp_secret: brfgnkerntkjbvheokrgmkg-BfgmierBfgvcx
    touser: '@all'
    type: wework  # rpc类型
    msg_types: [exception]  # 允许发送的消息类型
    accounts: []  # 允许的账户，为空允许所有
    keywords: []  # 消息过滤关键词
    retry_delay: 1000  # 重试间隔
    disable: true  # 是否禁用
webhook:  # 发送消息的配置
  entry:  # 入场消息
    content: "{name} {action}\n标的：{pair} {timeframe}\n信号：{strategy}  {enter_tag}\n价格：{price:.5f}\n花费：{value:.2f}"
  exit:  # 出场消息
    content: "{name} {action}\n标的：{pair} {timeframe}\n信号：{strategy}  {exit_tag}\n价格：{price:.5f}\n得到：{value:.2f}\n利润：{profit:.2f}"
  status:  # 机器人状态消息：启动，停止等
    content: '{name}: {status}'
  exception:
    content: '{name}: {status}'
mail:  # 邮件通知配置
  enable: false  # 是否启用邮件功能
  host: smtp.example.com  # SMTP服务器地址
  port: 465  # SMTP端口（通常为465或587）
  username: user1@example.com  # 邮箱用户名
  password: your_password  # 邮箱密码或应用密码
bt_in_live:  # 实盘定时回测对比功能
  cron: "0 0 * * *"  # Cron表达式，每天0点执行
  account: "account1"  # 指定执行回测的账户
  mail_to:  # 邮件通知收件人列表
    - "trader1@example.com"
    - "manager@example.com"
show_lang_code: "zh-CN"  # 显示语言代码，支持 zh-CN, en-US 等
api_server:  # 供外部通过api控制机器人
  enable: true
  bind_ip: 0.0.0.0
  port: 8081
  jwt_secret_key: bnlkrehnt40uyvjgnb234ro97gvb24
  users:
    - user: ban
      pwd: 123
      allow_ips: []
      acc_roles:
        user1: admin  # 这里的键对应accounts，值可选admin/guest

```

## 重要细节配置
#### 配置单笔订单金额
最低开单金额取决于交易所和货币对，通常会列在交易所支持页面上。

机器人启动时会自动加载交易所市场信息，即包含所有品种的开单金额等限制。banbot内部同时限制了最小单笔交易金额是10USD。

您可通过`stake_pct`或`stake_amount`配置单笔交易金额，注意对于期货合约，这两个参数指的是名义价值，而非占用的保证金。

如果您需要对不同的账户使用不同的交易金额，您可在`exchange.[exg_name].account_prods.user1.stake_rate`配置金额倍率，默认为1；改为`<1`则会减少此账户的开单金额。`>1`会增加此账号开单金额

同样，您也可以对不同策略设置不同的开单金额倍率`TradeStrat.StakeRate`

#### 全局止损
您可通过`fatal_stop`设置当机器人在所有策略任务综合x分钟内损失达到百分之多少时，停止交易`fatal_stop_hours`个小时

#### 运行环境
`env`的可选值：
- `prod`：生产网络
- `test`：测试网络
- `dry_run`：模拟实盘交易，本地模拟撮合，不会提交到交易所，仅当运行`trade`进行实时交易时有效

注意目前prod和test环境下品种ID并未区分，如已下载了K线，切换env时需要先删除已有K线数据，否则会出现混淆

#### 品种过滤和定期刷新
您除了通过`pairs`配置固定品种外，还可以配置`pairlists`来动态过滤并确定交易品种。如需启用，请确保`pairs`为空。

`pairlists`提供了若干个过滤器，可按交易量、价格、波动性、流动性、相关性等维度进行过滤。

如果您需要定期重新计算品种列表，可以配置`pairmgr.cron`，当`force_filters`设置为`true`时，会强制应用`pairlists`过滤器到`pairs`中。

`pairmgr`和`pairlists`都同时支持实盘和回测，如果您在回测中启用时，会使用历史数据进行过滤，这可以确保您在不同时间按同样配置启动回测/实盘交易时，动态计算的品种列表完全一致。

#### 真实时间同步设置
请求交易所的需要鉴权的接口时一般需要带时间戳，如果本地计算机的时间与网络时间误差过大，可能会导致请求失败。

banbot提供了`ntp_lang_code`配置项，可用于自动检测本地时间误差（向指定的NTP服务器发起请求计算时间误差），然后可使用真实网络时间发起交易所请求。

`ntp_lang_code`的可选值：
- `none`：不启用
- `zh-CN`：中国
- `zh-HK`：香港
- `zh-TW`：台湾
- `ja-JP`：日本
- `ko-KR`：韩国
- `zh-SG`：新加坡
- `global`：全球ntp服务器：google、apple、facebook...

#### 多账户配置
您可以在`accounts`中配置多个账户，每个账户可以配置不同的参数。

每个账户可以配置的参数：
- `no_trade`：是否禁止此账户交易
- `stake_rate`：开单金额倍率，全局`stake_rate`乘以账户`stake_rate`为实际开单金额倍率
- `leverage`：合约杠杆
- `max_stake_amt`：允许的单笔最大金额
- `max_pair`：此账户允许的最大品种数量
- `max_open_orders`：此账户允许的最大同时持仓订单数

回测或模拟实盘时，只会取第一个账户的配置进行交易。

#### 订单接力入场
对于大周期持仓时间较长的策略，可能一个订单持仓一两个月，您启动机器人时可能按交易信号应该已经有一些持仓了，要完全按信号交易您应该立即入场并持有对应的持仓而不是等待下一个信号发出才入场。

`relay_sim_unfinish`配置为`true`时，会在开始交易某个品种时，自动回测前面500根K线，得到未平仓订单，然后按这些订单立即入场。

此配置项在回测和实盘中都有效。

## 邮件通知配置

banbot支持邮件通知功能，可以在发生异常或其他重要事件时发送邮件通知。

### 基础邮件配置

在配置文件中添加邮件服务器配置：

```yaml
mail:
  enable: true                    # 是否启用邮件功能
  host: smtp.example.com         # SMTP服务器地址
  port: 465                      # SMTP端口（通常为465或587）
  username: user1@example.com    # 邮箱用户名
  password: your_password        # 邮箱密码或应用密码
```

> **授权密码**: 一般需要使用授权密码而非登录密码  
> **防火墙**: 确保服务器放行SMTP端口

### 邮件通知渠道配置

在`rpc_channels`中配置邮件通知渠道：

```yaml
rpc_channels:
  mail_notify:                   # 渠道名称（可自定义）
    type: mail                   # 渠道类型：mail
    disable: false               # 是否禁用此渠道
    msg_types: [exception]       # 消息类型：exception（异常）
    accounts: []                 # 指定账户（空表示所有账户）
    keywords: []                 # 关键词过滤（空表示不过滤）
    retry_delay: 1000           # 重试延迟（毫秒）
    min_intv_secs: 300          # 最小发送间隔（秒）
    touser: 'recipient@abc.com'  # 收件人邮箱地址
```
> **发送频率**: 建议设置`min_intv_secs`避免频繁发送邮件

## 实盘定时回测对比功能

此功能允许在实盘运行期间定时执行回测，并将回测订单结果与实盘订单进行对比，帮助确认策略在实盘中的表现是否符合预期。

### 配置实盘回测

```yaml
bt_in_live:
  cron: "30 3 0 * * *"           # Cron表达式(秒 分钟 小时 日 月 星期)，每天00:03:30执行
  account: "user1"               # 指定执行回测的账户
  mail_to:                       # 邮件通知收件人列表
    - "trader1@example.com"
    - "manager@example.com"
```
