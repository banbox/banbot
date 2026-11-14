# AI 与外部模型

## ML/DL 与 gRPC

Banbot 提供 gRPC 数据服务入口：

```shell
bot tool data-server --config strategy.yml
```

在 Go 策略项目中定义特征，由 Python 等客户端获取数据并训练模型；自行部署预测服务后，由 Go 策略准备特征、调用模型并生成交易意图。特征、训练、模型服务和策略决策需要由您的项目实现。可参考示例策略项目的 `rpc_ai` 目录。

历史训练与在线推理应使用相同字段、预热、缺失值和数据可见时间规则。自定义数据接入见[自定义时序数据](../guide/custom_data.md)。

## LLM 模型管理库

`llm` 包提供模型配置继承、环境变量替换、并发限制、超时、统计和失败回退。配置位于 `llm_models`，完整字段见[配置](../guide/configuration.md)。

当前普通回测和实盘入口不会自动调用 LLM；仅添加配置不会让策略自动获得预测或交易信号。策略项目或工具需要显式调用 `llm.NewLLMManager(configProvider, getModels)`，再调用 `manager.Call(ctx, systemPrompt, userPrompt, options)`。`*config.Config` 实现模型配置访问接口，模型顺序由调用方提供。

模型缓存是进程级资源，不提供不同任务同名模型配置的完全隔离。外部请求存在耗时和失败，应由调用方决定超时、重试及如何将结果传回策略；不要假定回测重跑会从外部模型得到相同结果。
