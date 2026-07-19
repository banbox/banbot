package web

import (
	"github.com/banbox/banbot/web/dev"
	"github.com/banbox/banbot/web/live"
	"github.com/banbox/banexg/errs"
	"github.com/spf13/cobra"
)

/*
RunDev

Run web UI robot panel
运行web ui机器人面板
*/
func RunDev(args []string) error {
	return dev.Run(args)
}

func NewCommand() *cobra.Command {
	return dev.NewCommand()
}

/*
StartApi

start web monitoring panel for live trade
为实时交易启动web监控面板
*/
func StartApi() *errs.Error {
	return live.StartApi()
}
