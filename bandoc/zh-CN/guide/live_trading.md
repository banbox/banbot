banbot支持您无需修改策略直接进行实时模拟交易或实盘交易。本文档帮助您部署交易机器人到生产环境。  
币安服务器位于日本东京，为保证长期实时运行时网络稳定，建议您将机器人部署在离交易所服务器较近的服务器上。

::: warning 风险提示
banbot现在实盘用户尚不够多，常见实盘用例测试通过，但很有可能还有未覆盖到的bug可能导致资金损失；使用本软件风险自负。作者和所有关联方对您的交易结果不承担任何责任。
如果您要开始实盘，请确保策略或yml配置中设置合适的止损以免出现较大亏损，并先用小资金试运行一段时间。
:::

::: tip 技术支持
我们提供特惠的实盘服务器，并提供实盘技术支持服务，新加坡4C8G每月仅60元，日本4C6G每月85元。[去选购](https://banbot.site/zh-CN/services)
:::

## 1. 编译
**从WebUI编译**  
您可执行`bot`直接启动WebUI，然后打开[/zh-CN/setting/](http://127.0.0.1:8000/zh-CN/setting/)，左侧选择【编译代码】，右侧选择目标系统和架构，执行编译并下载即可。

**从终端编译**  
`GOOS`的可选值：linux, darwin, windows, ...  
`GOARCH`的可选值：amd64, arm64, ...

::: code-group
```shell [Linux]
export GOARCH="amd64"
export GOOS="linux"
go build -o bot
```

```shell [MacOS]
export GOARCH="amd64"
export GOOS="darwin"
go build -o bot
```

```shell [Windows Power Shell]
# build for windows
$env:GOARCH="amd64"
$env:GOOS="windows"
go build -o bot.exe

# build for linux
$env:GOARCH="amd64"
$env:GOOS="linux"
go build -o bot
```

```shell [Windows CMD]
# build for windows
set GOARCH=amd64
set GOOS=windows
go build -o bot.exe

# build for linux
set GOARCH=amd64
set GOOS=linux
go build -o bot
```
:::

## 2. 编辑配置
上传编译好的单个可执行文件到目标服务器的`/ban`目录下，创建目录`/ban/data`作为数据目录，设置环境变量`BanDataDir=/ban/data`。

然后执行`/ban/bot init`即可生成yml配置文件到`/ban/data`。

然后编辑`/ban/data/config.local.yml`文件，设置交易所密钥、单笔金额、交易策略和周期等。

您可以配置多组策略和多个账户，如果您需要运行多组策略到不同账户，您可创建多个yml配置文件，分别启动多个机器人进程。

详细配置您可参考[配置文件](./configuration.md)

::: tip 提示
建议使用子账户实盘，仅转入必要的资金，如果使用`stake_pct`，且账户中有其他资产，会使用全部资产计算开单金额，会导致计算有误。  
config.local.yml的优先级高于config.yml，您可在这两个配置文件中任意修改您的配置。启动时无需指定这两个配置文件路径，会自动读取。  
dry-run是使用1分钟K线撮合本地订单，所以订单状态会有1分钟的延迟
:::

## 3. 启动交易机器人
您需要启动爬虫进程和机器人进程。爬虫进程用于订阅更新K线、订单簿等公开数据并保存到数据库。
机器人进程从爬虫进程订阅K线等数据，并执行交易策略。您可启动多个机器人进程以便运行不同策略。

策略和交易账户只消耗很小的资源，对2核2G的VPS，可启动多个机器人，单个机器人可配置上百个策略和上百个交易账户。

**启动爬虫**  
执行`bot spider`即可立即启动爬虫进程，推荐后台启动并转储日志：
```shell
nohup /ban/bot spider > /tmp/spider.log 2>&1 &
```
**启动交易机器人**  
执行`bot trade`即可启动实时交易机器人，推荐后台启动并转储日志：
```shell
nohup /ban/bot trade -config @demo.yml > /tmp/trade.log 2>&1 &
```
::: tip Tip
spider进程日志会自动保存到`@logs/spider.log`；机器人进程日志会自动保存到`@logs/[bot_name].log`；日志文件大小超过300M会自动轮换。

但这些文件只能记录正常运行的日志输出，如果机器人意外终止，退出时的panic错误日志无法记录，故推荐您启动spider和机器人时将stdout和stderr输出重定向到某个文件。
:::

## 4. 消息通知、DashBoard和监控
### 消息通知
您可在yml中配置`rpc_channels`消息通知，当机器人启动、停止、入场、平仓时发送消息到您的社交APP。目前支持：
- **企业微信**: 通过企业微信机器人发送消息
- **邮件通知**: 通过SMTP发送邮件通知

**企业微信通知设置**

您微信中关注企业微信插件（登录企业微信PC管理后台查看二维码），即可从微信收到上面消息。

**邮件通知设置**

配置邮件通知可以在系统异常时及时收到邮件提醒：

```yaml
# 邮件服务配置
mail:
  enable: true
  host: smtp.qq.com
  port: 465
  username: your_email@qq.com
  password: your_authorization_code

# 邮件通知渠道
rpc_channels:
  mail_exception:
    type: mail
    disable: false
    msg_types: [exception]
    min_intv_secs: 300  # 5分钟内最多发送一次
    touser: 'alert@company.com'
```

**Telegram通知设置**

banbot支持两种Telegram通知方式：官方机器人和自定义机器人。

**方式一：官方机器人（推荐）**

使用banbot官方Telegram机器人，无需申请Bot Token，配置最简单：

```yaml
rpc_channels:
  telegram_bot:
    type: telegram
    disable: false
    secret: ""  # 可选：自定义UUID v4格式密钥，留空自动生成
    msg_types: [entry, exit, status, exception]
    min_intv_secs: 5
```

启动机器人后，日志会输出类似以下信息：
```
Manage Your Bot On https://t.me/trade_banbot?start=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

点击链接或在Telegram中搜索`@trade_banbot`，发送`/start xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`即可绑定。

::: tip 提示
官方机器人方式无需配置代理，通过加密通道连接，适合网络受限环境。
:::

**方式二：自定义机器人**

创建专属Telegram机器人，拥有完全控制权：

```yaml
rpc_channels:
  telegram_bot:
    type: telegram
    disable: false
    token: "123456789:ABCdefGHIjklMNOpqrsTUVwxyz"  # 必填：Bot Token
    chat_id: "987654321"                           # 必填：聊天ID
    proxy: "http://127.0.0.1:7897"                 # 可选：代理地址
    msg_types: [entry, exit, status, exception]
    min_intv_secs: 5
```

获取Bot Token和Chat ID：
1. 在Telegram中找到`@BotFather`，发送`/newbot`创建机器人并获取Token
2. 将机器人添加到聊天，发送任意消息后访问：
   ```
   https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
   ```
   在返回的JSON中找到`chat.id`字段值

::: warning 注意
自定义机器人需要能访问Telegram API。如网络受限，需配置`proxy`代理地址。
:::

**通用配置说明**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `type` | 渠道类型，必须为`telegram` | - |
| `disable` | 是否禁用此通知渠道 | `false` |
| `msg_types` | 消息类型：`entry`入场、`exit`平仓、`status`状态、`exception`异常 | `[]` |
| `min_intv_secs` | 最小发送间隔（秒），避免消息过于频繁 | `0` |
| `retry_delay` | 发送失败后重试延迟（秒） | `30` |

**Telegram控制命令**

配置完成后可通过以下命令远程控制机器人：

| 命令 | 功能 |
|------|------|
| `/menu` | 显示交互式操作菜单（推荐） |
| `/account` | 查看账户列表和切换账户 |
| `/switch [账户名]` | 切换到指定账户 |
| `/orders` | 查看当前账户的订单列表 |
| `/close [订单ID\|all]` | 平仓指定订单或所有订单 |
| `/status` | 查看当前账户的交易状态 |
| `/disable [小时]` | 禁止当前账户开单（默认1小时） |
| `/enable` | 重新启用当前账户开单 |
| `/wallet` | 查看当前账户钱包余额 |
| `/help` | 显示帮助信息 |

::: tip 多账户管理
如配置了多个交易账户，使用`/account`查看所有账户，通过`/switch`切换账户后，所有操作（查看订单、平仓、禁用开单等）都针对当前激活账户。
:::

### DashBoard UI
您可在yml中配置`api_server`，这将会在启动机器人时同时启动一个web服务，然后您可在机器人启动后通过配置的账号密码访问机器人的DashBoard，查看交易概况并管理机器人。
```yaml
api_server:
  enable: enable
  bind_ip: 127.0.0.1
  port: 8001  # 需在防火墙放行或通过nginx反向代理
  jwt_secret_key: '[very_strong_secret]'
  users:
    - user: ban
      pwd: '123'
      allow_ips: []  # 允许的客户端ip
      acc_roles: 
        user1: admin  # user1对应accounts中的用户key
```
上面的web服务是以http启动的，建议您将`bind_ip`设为服务器本地或内网ip，以避免未授权的访问。

如果您需要从本地主机访问服务器的Dashboard，您可通过ssh连接到运行机器人的服务器，然后通过端口转发将对本地端口的请求转发到服务器端口：
```shell
ssh -L [本地端口]:[服务器bind_ip]:[服务器端口] [用户名]@[服务器公网地址]
# e.g. 
ssh -L 8001:127.0.0.1:8001 your_username@remote_server_address
```
::: tip Tip 
如果您需要将dashboard暴露到互联网，强烈建议您通过nginx等配置https，并设置一个较强的账号和密码。
:::

![image](/img/dash_login_cn.png)
> 本地启动实盘，或ssh端口转发后，机器人的访问url都是127.0.0.1:[port]，其中port是`api_server.port`的值。  
> 如果您通过nginx反向代理，这两个地址应当是从nginx访问的地址，比如"https://bot.myserver.com/8001"

### 实盘回测对比
banbot支持在实盘运行期间定时执行同样配置的回测，将回测订单记录逐个与实盘未平仓订单和交易所持仓进行对比，检查实盘是否有订单漏开、错开等情况：

```yaml
# 实盘回测配置
bt_in_live:
  cron: "0 0 * * *"  # 回测频率，corn表达式，每天0点执行
  account: "account1"
  mail_to: ["trader@company.com"]  # 对比结果将发送到配置的邮箱
mail:  # 您还需要设置邮件发送方，具体可参考上面邮件通知
  enable: true
```

### 监控存活
您可使用`crontab`定期执行[check_bot.sh](https://github.com/banbox/banbot/blob/main/doc/check_bot.sh)脚本，检查机器人存活情况，发送邮件通知。
```shell
# for centos 8
dnf install mailx
vi /etc/mail.rc
```
示例配置如下：
```text
set smtp=smtps://smtp.gmail.com:465
set smtp-auth=login
set smtp-auth-user=your_email@gmail.com
set from=your_email@gmail.com
set smtp-auth-password=your_email_password
set ssl-verify=ignore
```
测试邮件发送功能：
```shell
echo "Test email body" | mail -s "Test email subject" recipient@example.com
```
打开crontab：
```shell
crontab -e
```
添加一行：
```text
*/5 * * * * /ban/check_bot.sh trade user1@example.com user2@example.com
```
赋予脚本执行权限：
```shell
chmod +x /ban/check_bot.sh
```
