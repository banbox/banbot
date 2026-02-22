# rpc 包

rpc 包提供了远程过程调用相关的功能，主要用于消息通知和异常上报。

## 主要结构体

### IWebHook
`IWebHook` 是一个接口，定义了WebHook通知渠道的基本行为：
- `GetName() string` - 获取WebHook名称
- `IsDisable() bool` - 检查WebHook是否被禁用
- `SetDisable(val bool)` - 设置WebHook的禁用状态
- `CleanUp()` - 清理WebHook资源
- `SendMsg(msgType string, account string, payload map[string]string) bool` - 发送消息
- `ConsumeForever()` - 持续消费消息队列

### WebHook
`WebHook` 是基础的WebHook实现结构体，包含以下主要字段：
- `name string` - WebHook名称
- `Config map[string]interface{}` - WebHook配置
- `MsgTypes map[string]bool` - 支持的消息类型映射
- `Accounts map[string]bool` - 支持的账户映射
- `Queue chan map[string]string` - 消息队列
- `RetryNum int` - 重试次数
- `RetryDelay int` - 重试间隔(秒)
- `Disable bool` - 是否禁用
- `ChlType string` - 渠道类型

### WeWork
`WeWork` 是企业微信WebHook实现，继承自WebHook，包含以下特有字段：
- `cred *WeCred` - 企业微信认证信息
- `agentId string` - 应用ID
- `toUser string` - 接收消息的用户
- `toParty string` - 接收消息的部门
- `toTag string` - 接收消息的标签

## 主要功能

### InitRPC
初始化RPC服务，主要用于初始化WebHook通知渠道。

返回：
- `*errs.Error` - 初始化过程中的错误，如果成功则返回nil

### SendMsg
发送消息到已配置的通知渠道。

参数：
- `msg map[string]interface{}` - 消息内容，包含以下字段：
  - `type` - 消息类型
  - `account` - 账户标识（可选）
  - `status` - 消息状态内容
  - 其他字段根据webhook配置进行模板渲染

### CleanUp
清理RPC相关资源，关闭所有通知渠道。

### NewExcNotify
创建一个新的异常通知处理器，用于捕获和发送错误日志。

返回：
- `*ExcNotify` - 异常通知处理器实例

### TrySendExc
尝试发送异常信息，会对相同的异常进行合并处理。

参数：
- `cacheKey string` - 异常缓存的key，通常是调用位置
- `content string` - 异常内容

### NewWebHook
创建一个新的WebHook实例。

参数：
- `name string` - WebHook名称
- `item map[string]interface{}` - WebHook配置，包含以下字段：
  - `type` - WebHook类型
  - `msg_types` - 支持的消息类型列表
  - `accounts` - 支持的账户列表
  - `keywords` - 关键词过滤列表
  - `retry_num` - 重试次数
  - `retry_delay` - 重试间隔(秒)
  - `disable` - 是否禁用

返回：
- `*WebHook` - WebHook实例

### NewWeWork
创建一个新的企业微信WebHook实例。

参数：
- `name string` - WebHook名称
- `item map[string]interface{}` - 企业微信配置，包含以下字段：
  - `corp_id` - 企业ID
  - `corp_secret` - 应用密钥
  - `agent_id` - 应用ID
  - `touser` - 接收消息的用户
  - `toparty` - 接收消息的部门
  - `totag` - 接收消息的标签

返回：
- `*WeWork` - 企业微信WebHook实例
