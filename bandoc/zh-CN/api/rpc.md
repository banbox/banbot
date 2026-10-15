# rpc 包

通知和远程控制通过每个 Runtime 的 rpc.Session 绑定任务，避免通知频道、订单管理器、钱包或语言配置跨任务串用。

## Session

NewSession(snapshot *config.Snapshot, accounts map[string]*config.AccountConfig) *Session 根据配置快照、执行账户及目录创建会话。Core、Clock、Orders、Wallets 必须绑定同一任务；通知消息和远程命令使用这些实例依赖。

Session.Start() 构造已配置频道并启动消费；Stop/Join 与 Runtime 生命周期连接，关闭后不再接纳发送，等待已接纳消费者。SMTP sender 从会话 mail 配置构造，不使用另一个 Runtime 的全局 mail 设置。

Telegram、企业微信、webhook 和 email 的配置形式保留。远程交易指令使用所属会话的订单/钱包接口，不是查找“当前 Runtime”。兼容通知函数不保证多任务隔离。

本地 Session/lifecycle 测试不表示真实邮件或通知平台验收。见[runtime](runtime.md)。
