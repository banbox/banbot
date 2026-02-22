如果您已通过[runbanbot](https://gitee.com/banbox/runbanbot)配置并启动了banbot，无需再查看此页面进行项目初始化。

如果您已通过[快速开始](quick_local.md)完成安装并启动banbot，也无需再查看此页面。

本页面仅针对本地安装golang，且打算从零初始化新策略项目的专家。

## 初始化新项目

如您确定创建一个空白的策略项目，可打开终端，输入以下命令：
```shell
# mystrats是项目名称，可随意更换
mkdir mystrats
cd mystrats
go mod init mystrats
go get github.com/banbox/banbot
```
您无需下载banbot的源代码，而只需创建一个用于管理交易策略的项目，通过`go get`添加banbot依赖，您即可享受banbot带来的全部便利。

首先我们创建一个`ma`文件夹，存放均线类策略，在`ma`中创建`demo.go`文件，实现我们的第一个简单的均线交叉买卖策略：
```go
package ma

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

func init() {
	// 注册策略到banbot中，后续在配置文件中使用ma:demo即可引用此策略
	// `init`函数是go中的特殊函数，会在当前包被导入时立刻执行
	strat.AddStratGroup("ma", map[string]strat.FuncMakeStrat{
		"demo": Demo,
	})
}

func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		WarmupNum: 100,
		OnBar: func(s *strat.StratJob) {
			e := s.Env
			ma5 := ta.SMA(e.Close, 5)
			ma20 := ta.SMA(e.Close, 20)
			maCrx := ta.Cross(ma5, ma20)

			if maCrx == 1 {
				s.OpenOrder(&strat.EnterReq{Tag: "open"})
			} else if maCrx == -1 {
				s.CloseOrders(&strat.ExitReq{Tag: "exit"})
			}
		},
	}
}
```
然后在项目根目录下创建一个`main.go`文件，写入以下代码，即完成了入口启动文件配置
```go
package main

import (
	"github.com/banbox/banbot/entry"
	_ "mystrats/ma"
)

func main() {
	entry.RunCmd()
}
```
::: tip 风格提示
为保持各类策略的可维护性，我们建议将相似策略放在同一个`go package`包中，不建议在`main.go`中直接实现策略。具体目录结构请参考[示例策略项目](https://github.com/banbox/banstrats)

在进行回测时，banbot会自动备份此次回测的策略源代码，方便追溯不同版本策略的效果。
:::
