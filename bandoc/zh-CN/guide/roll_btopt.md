# 滚动优化回测

滚动优化先在过去窗口搜索参数，再对紧接着的样本外窗口回测，随后向前移动并重复。它用于检查参数更新方案的历史适应性，不能保证未来收益。当前 `bt-opt` 与 `tool test-pickers` 仅支持时序 `TradeStrat` 路径，不提供因子/混合策略优化。

## 配置窗口并运行

在 YAML 中设置完整时间范围、标的、钱包和待优化策略；参数须通过 `pol.Def` / `pol.DefInt` 定义。时间范围必须容纳回顾区间及至少一个后续测试窗口，并准备对应预热数据。

```bash
bot bt-opt --datadir /path/to/data --config experiment.yml --review-period 12M --run-period 2M --opt-rounds 20 --picker score
bot bt-opt --datadir /path/to/data --config experiment.yml --review-period 12M --run-period 2M --opt-rounds 20 --picker goodAvg
```

`--review-period` 默认 `3y`，`--run-period` 默认 `6M`，后者不得短于 1 小时；`M` 表示月份单位，`m` 表示分钟，周期按程序的时长换算处理，不是日历逐月调度。首次样本外回测从配置起点加回顾时长开始，最后一个测试窗口截到配置终点。

例如总区间为三年、回顾一年、运行两个月时，第一年仅用于训练，之后每隔两个月使用之前一年的数据重新优化。每次挑选参数只看该窗口之前的数据；策略本身和品种筛选仍需避免未来信息。

`--sampler`、`--opt-rounds`、`--each-pairs`、`--concur` 和 `--picker` 与[超参优化](./hyperopt.md)相同。`bt-opt` 另有 `--alpha`（默认 1）平滑相邻窗口选中参数，以及 `--pair-picker` 做已注册的标的选择。时间范围和标的使用 YAML 设置；此命令不接受 `--timerange`、`--pairs` 或 `--out`。

## 输出和重复运行

输出位于 `<datadir>/backtest/bt_opt_<hash>/`，启动日志会打印具体路径：

| 路径 | 内容 |
| --- | --- |
| `opt_<训练起点秒时间戳>.log` | 每个训练窗口的搜索日志；并发时可能带编号后缀 |
| `detail/` | 优化候选 JSON 明细 |
| `<picker>/` | 滚动样本外回测的累计报告 |
| `config.yml` | 保存此次任务的配置 |

滚动回测在窗口间结转钱包余额并累积已完成订单及报告；不是把每段收益率简单相加。已有可用的优化日志会被复用。续跑前保留原配置、数据和日志；需要独立实验时核对启动日志的输出路径并隔离数据目录，避免误用旧搜索结果。

## 比较参数挑选方式

```bash
bot tool test-pickers --datadir /path/to/data --config experiment.yml --review-period 12M --run-period 2M --opt-rounds 20 --picker score,good3,goodAvg
```

该命令复用每个训练窗口的搜索结果，对指定挑选器分别执行后续窗口回测；输出根目录另有 `pickerScores.csv` 和 `pickerRanks.csv`，记录每个窗口的分数和排名。它是窗口间的挑选器比较，不是每个挑选器各自结转资金的完整累计报告。默认 `--picker good3` 只测试一种；显式 `--picker ''` 才测试全部已注册项。

优先评估留出区间的收益、回撤、交易量和各窗口稳定性。对比挑选器本身也会消耗样本外数据，最终还需保留未用于选择方法的区间。不同的策略、数据和成本假设可能得出不同结果，不能把某个示例图的收益当成当前实验的预期。
