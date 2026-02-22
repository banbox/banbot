## docker一键启动
推荐使用[runbanbot(docker)](https://gitee.com/banbox/runbanbot/)一键启动，无需配置，开箱即用；

启动成功后，浏览器访问[127.0.0.1:8000/zh-CN](http://127.0.0.1:8000/zh-CN)即可打开Web UI；

您可在WebUI中添加/修改策略、回测、数据管理等；也可使用VsCode或GoLand打开`docker-compose.yml`同目录下的`strats`策略项目，直接编辑策略（这需要本地安装golang）

如果您使用runbanbot启动，则无需再阅读当前页面。安装完成后，您可能希望了解[banbot从入门到进阶](https://www.bilibili.com/video/BV1b72CBXEQu/)

如果希望本地安装启动，您可按照下面步骤快速在本地配置并运行banbot：

## Step 1. 本地安装
您需要安装Golang开发环境，建议使用最新版本。banbot使用QuestDB存储K线等公开数据，启动时会自动下载安装，无需您手动配置。

您也可以查看B站的[安装教程视频](https://www.bilibili.com/video/BV1QuLozqEzg/)

### 安装golang
请从golang的官网下载[安装](https://go.dev/doc/install)

::: tip TIP
如果您的网络环境不能直接访问golang.org，请配置[国内源](https://learnku.com/go/wikis/38122)
:::

## Step 2. 获取示例策略项目并编译
您可使用git直接拉取示例项目：`git clone https://github.com/banbox/banstrats`  
如果您没有安装git工具，也可直接打开[banstrats](https://github.com/banbox/banstrats)网页，下载zip压缩包并解压。

准备好示例策略项目后，进入项目根目录（有`go.mod`的文件夹），在终端（命令行）执行下面命令：

::: code-group
```shell [Linux/MacOS]
# 初始化依赖
go mod tidy

# 将策略和banbot编译为单个可执行文件
go build -o bot
```
```shell [Windows]
# 初始化依赖
go mod tidy

# 将策略和banbot编译为单个可执行文件
go build -o bot.exe
```
:::
<img style="width:480px;margin-top:10px" src="/img/compile.jpg"/>

当您在策略项目下执行`go build`时，您的策略将和banbot源码一起打包为单个可执行文件，您可使用此文件回测、实盘、或启动WebUI等。

## Step 3. 配置环境变量
为方便后续使用，您需要设置环境变量`BanDataDir`和`BanStratDir`。

`BanDataDir`建议设置为一个空目录。是banbot运行过程中回测结果、前端UI资源文件保存的目录。不建议设置为策略项目的子目录。

`BanStratDir`即您的交易策略项目(banstrats)的路径，每次回测时，banbot会自动保存您此次回测对应的策略代码版本，方便您及时恢复到之前的某个版本。

比如设置环境变量如下：

::: code-group
```shell [MacOS]
nano ~/.zshrc
# 如果您使用的Catalina以前的版本，请使用nano ~/.bash_profile
# 在文件末尾追加：
export BanDataDir=/Users/YourName/bandata
export BanStratDir=/Users/YourName/banstrats
# 然后Ctrl+O保存，Ctrl+X退出
# 执行下面命令使修改生效
source ~/.zshrc
```
```shell [Linux]
nano ~/.bashrc
# 在文件末尾追加：
export BanDataDir=/home/YourName/bandata
export BanStratDir=/home/YourName/banstrats
# 然后Ctrl+O保存，Ctrl+X退出
# 执行下面命令使修改生效
source ~/.bashrc
```
```text [Windows]
BanDataDir=E:\quant\bandata
BanStratDir=E:\quant\banstrats
```
:::
注意一般修改后需要打开新的终端窗口才会生效。

## Step 4. 修改配置文件
在策略项目下，只需执行`bot.exe init`即可在`BanDataDir`下自动初始化配置文件`config.yml`和`config.local.yml`
<img style="width:780px;margin-top:10px" src="/img/init_config.jpg"/>

然后编辑`config.local.yml`文件，设置交易所密钥、单笔金额、交易策略和周期等。

示例：
```yaml
stake_amount: 100
wallet_amounts:
  USDT: 1000
time_start: "20240701"
time_end: "20250701"
pairs: ['ETH']
run_policy:
  - name: ma:demo
    run_timeframes: [15m]
accounts:
  user1:
    binance: # or okx/bybit
      prod:
        api_key: vvv
        api_secret: vvv
# 中国大陆不能直接访问币安api，请设置vpn代理，或设置HTTP_PROXY/HTTPS_PROXY，以及系统代理配置
exchange:
  binance:
    proxy: http://127.0.0.1:10808  # 设置为no，禁用系统代理
```

## Step 5. 启动WebUI
在策略项目下，只需执行`bot.exe`即可启动WebUI并自动打开浏览器。

您也可以访问`http://127.0.0.1:8000/zh-CN` 体验。

<img style="width:780px;margin-top:10px" src="/img/run_webui.jpg"/>

接下来您可能希望了解[banbot从入门到进阶](https://www.bilibili.com/video/BV1b72CBXEQu/)

管理策略项目时，我们推荐使用AI IDE；您也可以使用`Visual Studio Code`或`GoLand`
