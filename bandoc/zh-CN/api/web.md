# web 包与子包

web 是 HTTP 装配入口，不拥有另一套策略引擎或账户模型。开发回测和实时监控使用不同 typed dependencies 与生命周期。

## 开发界面

web.NewDevCommandWithFactory(factory) 将 entry 提供的 server factory 注入命令。web/dev 的 RuntimeFactory 接受 context、exchangeName、market，返回 data.RuntimeDeps、cleanup 和 error；临时数据工具任务使用自己的状态，外部依赖按借用合同释放。

开发 API 调用同一 entry 回测预检和执行工厂，统一配置、字段来源和 TS/CS/mixed 路由不在 Web 重建。配置加载只读，显式编辑保存使用冲突检查和原子写；读取旧 YAML 不自动迁移生产数据。

## 实时监控

web.StartApiWithRuntimeDeps(lifecycle, deps biz.RuntimeDeps) 与 web/live.StartApiWithRuntimeDeps 绑定当前任务。HTTP、认证、WebSocket、订单/钱包查询都使用实例状态；生命周期 owner 在取消时 Stop，然后 Join 等待 handler/writer。

监控发送对慢 WebSocket 客户端有界，不阻塞交易 callback；停止时不再接纳新 handler，等待已接纳工作后清理。web.StartApi() 是兼容 facade，新多任务流程使用显式版本。

OrderArgs、ForceExitArgs、CloseArgs、JobItem、LoginRequest 等 API 请求/响应类型属于 web/live，不是 live 包公共策略 API。具体路由和验证以该服务器实际注册为准。

见[entry](entry.md)、[runtime](runtime.md)、[实盘](../guide/live_trading.md)。
