# runtime 包

runtime 负责组装任务状态和资源借用。Context 只传播取消、deadline 与 I/O；具体状态经字段、receiver 和 RuntimeDeps 传递，不通过 Context.Value 查服务。

## Process 与 Runtime

Process 拥有账户 registry/共享账户服务、Runtime 构造与关闭协调、scheduler claim，以及按存储身份共享的 SID allocator/registry。一个 Process 可创建多个 Runtime，物理账户发送权由 owner 控制；不把多个 Runtime 视作无条件可并行共享可变外部服务。

Runtime 拥有 Core、Clock、Market、Symbols、Batch、Strategies、Orders、Trading、Catalog、Notifications 和可选 FactorState。Config 为只读 Snapshot；Accounts 是独立可变执行配置，由同一 AccountsMu 协调。Storage/Exchange 是外部依赖，Runtime 字段不是关闭权声明。

## 构造与依赖投影

- NewProcess() *Process 创建进程协调 owner。
- (*Process).NewRuntime(opts Options) (*Runtime, error) 创建任务；构造失败释放本次取得的资源和 claims。
- Runtime.BizDeps() 投影为 biz.RuntimeDeps，交易器、provider、wallet、报告与过滤器直接使用同一 owner 的具体状态。
- 因子配置安装使用 runner.CloneConfig；共享 Plan/ComputationGroup 与回调等保持借用身份，配置容器独立复制。

嵌入程序优先复用 entry 的标准命令装配；手工构造必须提供一致的 config、clock、symbols、storage、exchange、callback owner 和账户配置，不得缺字段后回退查 globals。

## 停止、关闭与等待

`Stop` 只关闭接纳并传播取消，不执行完整资源关闭。资源所有者调用 `Close` 请求关闭，再调用 `Join` 等待关闭完成；没有请求 `Close` 时，`Join` 是 no-op。回调内调用 `Close` 会异步完成关闭，须由外部 owner 调用 `Join`，避免等待自身。`Close` 协调 stop、已注册工作等待、reset 与借用释放；`OnClose` 注册 stop，`OnCloseWait` 注册 join。自建 goroutine 只有接入 callback/lifecycle 才在此范围内。Web/RPC server 的 Stop/Join 是各自组件生命周期，不能套用为 Runtime 的关闭顺序。

Process.Close 等待其任务/构造并释放共享账户和 registry；入口随后关闭自己创建的 Storage/Exchange。borrowed scheduler 不 Stop，owned scheduler 的唯一 claim 防止多 owner 双重关闭。共享账户 borrower 退出不能关闭其他策略仍使用的物理账户。

## 兼容边界与验证

各领域还有兼容 facade，但当前 runtime 不提供 WithLegacy/LockLegacy；runtimeplan.Inspect 使用局部状态，不安装 globals。新业务路径使用显式 deps；兼容函数不授予多任务隔离保证。

见[entry](entry.md)、[biz](biz.md)、[com](com.md)、[rpc](rpc.md)、[web](web.md)及[因子](factor.md)。局部/default/race 测试不能代替真实数据库与 venue 会话验收。
