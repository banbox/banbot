# BanIO 数据通信

BanIO 是 utils/banio.go 中的 TCP 服务器/客户端通信层，支持消息读写、订阅广播、请求结果等待、断线重连、压缩及可选加密。ServerIO 管理连接，ClientIO 嵌入 BanConn；创建资源的 owner 负责停止与等待退出。

## 消息和序列化

```go
type IOMsg struct {
    Action string
    Data any
    NoEncrypt bool
}
type IOMsgRaw struct {
    Action string
    Data []byte
    NoEncrypt bool
}
```

`WriteMsg` 将普通 payload 经 JSON 编码；字符串/字节数据直接使用其内容，之后压缩并按设置加密。外层 IOMsgRaw 使用 gob 封装。接收者拿到解码后的 payload 字节，再按 Action 解析对应 schema。此机制不是任意 Go 对象的深复制协议，JSON map 不自动保留整数宽度、指针或自定义 Go 类型；需要具体类型时由 schema 和解码校验保证。NULL 与缺键不能隐式转换为零。

Spider 的当前消息是 `data.NotifySeries{TFSecs,Interval,Rows []*orm.DataSeries}`，`data.SeriesMsg` 增加 ExgName/Market/Pair。数据通过 Values map 传递自定义列，不再使用旧 Arr []*banexg.Kline。SeriesWatcher 解码 JSON payload，feeder 的内存缓冲与复权/聚合仍需遵守类型和 NULL 语义；不能因为传了完整 DataSeries 就宣称 JSON map 无损往返。

## BanConn

| 方法 | 用途 |
| --- | --- |
| `WriteMsg(*IOMsg)` / `Write(*IOMsgRaw)` | 编码/发送消息 |
| `ReadMsg()` | 接收 IOMsgRaw |
| `SetData/GetData/PopData/DeleteData` | 连接本地标签/订阅状态 |
| `SetAesKey/GetAesKey` | 加密配置 |
| `SetWait/GetWaitChan/CloseWaitChan` | 请求等待通道 |
| `SendWaitRes` | 发送请求结果 |
| `WaitResult(ctx,key,timeout)` | 可取消的等待，返回 `([]byte,*errs.Error)` |
| `GetRemote/GetRemoteHost/IsClosed` | 连接信息 |
| `RunForever` | 消息循环 |
| `SetContext/Stop/Join/Close` | 具体 BanConn 生命周期 |

`Listens map[string]ConnCB` 保存消息回调，DoConnect/ReInitConn 处理重连。共享标签与连接列表应通过方法访问，不直接并发修改 map。Stop 封闭输入并取消循环，owner 在回调外 Join 等待生产者和已接纳 handler；不要让 handler 等待自己退出。

客户端改变服务器订阅标签：

```go
err := conn.WriteMsg(&utils.IOMsg{Action: "subscribe", Data: []string{"key1"}})
if err != nil { return err }
err = conn.WriteMsg(&utils.IOMsg{Action: "unsubscribe", Data: []string{"key1"}})
```

该片段嵌入已有 `conn utils.IBanConn`、返回 `*errs.Error` 的函数。服务端 Broadcast 对已订阅 msg.Action 的连接推送。

## ClientIO

```go
NewClientIOWithContext(ctx context.Context, addr, aesKey string) (*ClientIO, *errs.Error)
NewClientIOWithState(state *core.State, addr, aesKey string, contexts ...context.Context) (*ClientIO, *errs.Error)
```

显式任务使用所属 context/state；包级 NewClientIO 是兼容入口。GetVal(key,timeout) 和 SetVal(*KeyValExpire) 访问服务器键值缓存，不是 SeriesStore，也不提供时序/PIT 查询。

## ServerIO

```go
NewServerIO(addr, aesKey string) *ServerIO
NewBanServer(addr, aesKey string) *ServerIO
```

第二个参数是 AES key，不是旧文档中的 name。`RunForever(intvSecs,timeoutSecs)` 开始服务；`ListenAddr` 返回实际地址。`AddConnection/RemoveConnection/ConnectionsSnapshot` 管理连接快照，`WrapConn` 包装 socket，`InitConn` 设置 Action 回调。`SetVal/GetVal` 管理可过期键值；`Broadcast` 发送订阅消息。停止服务调用 Stop，再由 owner Join 等待退出。

## 请求结果等待

调用方 SetWait 分配 request key，发送含该 key 的业务消息，再用 `WaitResult(ctx,key,timeout)` 等待。对端通过 SendWaitRes 回传；取消、超时和连接关闭必须处理错误并清理等待状态。BanIO 只提供通信机制，稳定业务 ID、幂等和策略执行证据属于上层执行领域。
