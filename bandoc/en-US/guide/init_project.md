If you have already configured and started banbot through [runbanbot](https://gitee.com/banbox/runbanbot), you do not need to view this page for project initialization.

If you have already completed the installation and startup of banbot through the [Quick Start](quick_local.md), you also do not need to view this page.

This page is only for experts who have installed Golang locally and intend to initialize a new strategy project from scratch.

## Initialize a New Project

If you are sure you want to create an empty strategy project, open the terminal and enter the following commands:

```shell
# mystrats is the project name and can be changed freely
mkdir mystrats
cd mystrats
go mod init mystrats
go get github.com/banbox/banbot
```

You do not need to download the source code of banbot; simply create a project to manage your trading strategies, and by adding the banbot dependency via `go get`, you can enjoy all the conveniences provided by banbot.

First, we create a `ma` folder to store the moving average strategy, create a `demo.go` file in `ma`, and implement our first simple moving average crossover buying and selling strategy:
```go
package ma

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

func init() {
	// Register the strategy in Banbot, and use ma: demo in the configuration file to reference this strategy later
	// The `init`function is a special function in Go that will be executed immediately when the current package is imported
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
Then create a `main.go` file in the project root directory and write the following code to complete the entry startup file configuration
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
::: tip Style Tips
To maintain the maintainability of various strategies, we recommend placing similar strategies in the same `go package` package, and do not recommend implementing strategies directly in `main.go`. For the specific directory structure, please refer to [Sample Strategy Project](https://github.com/banbox/banstrats)

When backtesting, banbot will automatically back up the strategy source code for this backtest, making it easier to trace the effects of different versions of strategies.
:::
