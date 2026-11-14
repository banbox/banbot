# 超参数优化

`bot optimize` 搜索时序 `TradeStrat` 策略通过 `pol.Def` / `pol.DefInt` 声明的参数范围。它不提供 factor 或混合策略的超参搜索；因子实验请使用不同显式参数配置分别运行[研究/回测](./factor.md)。

## 运行搜索

先确认策略已编译注册，配置了 `run_policy`、标的、历史时间范围和回测钱包。确保数据覆盖训练区间及预热区间，再创建输出文件的父目录：

```bash
mkdir -p /path/to/results
bot optimize --datadir /path/to/data --config experiment.yml --out /path/to/results/opt.log --opt-rounds 30 --sampler bayes --picker good3
```

`--out` 是日志文件，不是报告目录，必须提供；当前省略它仅告警并返回，不能据此认定已完成搜索。配置分层见[配置指南](./configuration.md)。

| 参数 | 含义与默认值 |
| --- | --- |
| `--opt-rounds` | 每个优化任务的轮数，默认 30；实际次数可能因采样器收敛或搜索空间而提前结束 |
| `--sampler` | `bayes`（默认）、`tpe`、`random`、`cmaes`、`ipop-cmaes`、`bipop-cmaes` |
| `--picker` | 结果挑选方法，默认 `good3` |
| `--each-pairs` | 按品种分别搜索 |
| `--concur` | 并发任务数，默认 1；多策略组可通过编译后的可执行文件启动子进程并行，不是单组内每轮并发 |
| `--bt-strict` | 覆盖严格回测设置；覆盖证据要求见[回测指南](./backtest.md) |

优化命令没有 `--timerange` / `--pairs` 参数；通过 YAML 或 `--config-data` 设置训练区间和标的。`run_policy[].dirt` 为空时多空一起运行，`long` / `short` 限定方向；`any` 会分别搜索多、空、双向情况，最终可能生成多个策略条目。

## 读取与应用结果

日志包含参数、`loss`、订单数、收益、回撤及其他指标。`loss = -score`，因此较低 loss 对应较高分数，分数并非收益百分比。搜索结果的 YAML 策略片段会打印到标准输出；手工核对后，将完整 `run_policy` 列表写入独立覆盖配置，使用未参与搜索的区间执行 `bot backtest`。

顺序搜索写入 `--out` 文件；多组子进程并发时写入 `<out>.1`、`<out>.2` 等日志。试验明细写入日志同级的 `detail/` JSON；不要把搜索日志当作完整的普通回测报告。

可重新排序文件或收集目录：

```bash
bot tool collect-opt --datadir /path/to/data --config experiment.yml --in /path/to/results/opt.log
bot tool collect-opt --datadir /path/to/data --config experiment.yml --in /path/to/results --picker good3
```

文件输入仅对该文件排序并写回；目录输入递归收集以 `.log` 结尾的文件，输出所选策略 YAML。并发日志名若以 `.log.1` 等结尾，不会被目录扫描自动收集。此工具不是只读命令；保留需要归档的原日志副本。

## 挑选器与过拟合

候选按分数降序处理。`good*` 使用非负分数组；若没有非负分数，则回退到负分数组，不保证结果盈利。

| 挑选器 | 行为 |
| --- | --- |
| `score` | 选择最高分候选 |
| `good2` / `good3` / `good4` / `good5` / `good7` | 取候选序列约 20% / 30% / 40% / 50% / 70% 位置 |
| `goodAvg` | 对整个候选序列取参数平均 |
| `good0t3` / `good1t4` / `good2t5` | 对前 0–30% / 10–40% / 20–50% 区间取参数平均 |
| `good3t7` / `good0t7` / `good3t10` | 对前 30–70% / 0–70% / 30–100% 区间取参数平均 |

平均参数不一定是搜索中实际评估过的参数组，应重新回测。默认 `good3` 不是普遍最优的承诺；比较样本外收益、回撤和交易量，不要只挑训练分数最高的结果。

滚动比较使用 `bot tool test-pickers --picker score,good3,goodAvg`。默认只测 `good3`；要测试所有已注册挑选器，显式传入 `--picker ''`。具体窗口和输出见[滚动回测](./roll_btopt.md)。反复按同一留出区间挑选方法仍可能造成过拟合。
