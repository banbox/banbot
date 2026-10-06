# v0.6.0-beta.7

因子引擎新增 18 类常用技术指标，共 22 个可独立引用的输出算子。Go 因子图和 YAML 表达式均可使用，Batch 与持续运行的 Session 共用已发布 banta v0.4.1 的指标定义。

- 均线：SMA、RMA、WMA、VWMA。
- 动量：RSI、ROC、MOM、MACD 线/信号/柱、CCI、Stoch、Williams %R。
- 波动与区间：TR、ATR、布林带上/中/下轨、滚动 Highest/Lowest。
- 成交量：OBV、MFI。

新指标按完整有效的输入元组推进：缺失、NULL、非数值及非有限输入保留原有效性，不推进该指标状态。多输入节点隔离底层缓存，避免共享价格但使用不同成交量或高低价时串扰；Session 保留有界状态，候选计算可以独立复制及提交。原 EMA、StdDev、Return、Lag 的既有语义继续保留。

ROC 输出百分数，Return 输出比例；CCI 输入是调用者提供的单个数值序列，经典典型价可写为 `(high + low + close) / 3`。各指标周期、输入数量、参数及预热在编译时校验，具体接口见 [指标指南](https://github.com/banbox/banbot/blob/v0.6.0-beta.7/doc/factor_indicators.md)。

后端版本升级到 **v0.6.0-beta.7**；本次未修改前端，`UIVersion` 继续使用 **v0.6.0-beta.5**。

## 验证

发布验收使用 `GOWORK=off` 及已发布 banta v0.4.1：34 个正式 Go 包测试通过，`go vet ./factor/... ./core ./entry ./execution ./runtime ./runtimeplan ./config` 与模块校验通过。Windows、Linux amd64 主程序构建通过，Windows 版本输出为 `banbot v0.6.0-beta.7`。中英文 bandoc 构建、指标指南 YAML 的 CLI validate/explain、本地文档链接、gofmt 及 diff 检查通过。

专项测试覆盖全部 22 个输出对 tav 的数值一致性、平盘/上涨/下跌、周期 1 与短历史、缺失/NULL/Inf、多输入缓存隔离、跨块与因果前缀、动态资产池、状态保留上限、候选复制/提交延续及极端有限数值溢出。RSI 的增量初始化采用与 tav 相同的累加顺序，避免两种执行模式在溢出边界产生不同有效性。

当前 Windows 环境 `CGO_ENABLED=0`，race 未运行。被忽略的临时实验 `tmp/legacy-dualma-replay` 不属于发布测试包；真实交易所、外部数据库与完整性能基准未验收。本次没有新增或升级依赖。

源码对比：[v0.6.0-beta.6 → v0.6.0-beta.7](https://github.com/banbox/banbot/compare/v0.6.0-beta.6...v0.6.0-beta.7)。
