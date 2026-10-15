package web

import (
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/web/dev"
	"github.com/banbox/banbot/web/live"
	"github.com/banbox/banexg/errs"
	"github.com/spf13/cobra"
)

type DevServerFactory = dev.ServerFactory

// RunDev preserves the public Web utility entry. Pass a typed factory to keep
// the invocation isolated from process-global runtime state.
func RunDev(args []string, factories ...DevServerFactory) error {
	return dev.Run(args, factories...)
}

// NewCommand preserves the public Cobra command API with optional explicit
// runtime construction.
func NewCommand(factories ...DevServerFactory) *cobra.Command {
	return dev.NewCommand(factories...)
}

func NewDevCommandWithFactory(factory DevServerFactory) *cobra.Command {
	return dev.NewCommandWithFactory(factory)
}

/*
StartApi

start web monitoring panel for live trade
为实时交易启动web监控面板
*/
func StartApi() *errs.Error {
	return live.StartApi()
}

type ApiServer = live.ApiServer

func StartApiWithRuntimeDeps(lifecycle live.ServerLifecycle, deps biz.RuntimeDeps) (*ApiServer, *errs.Error) {
	return live.StartApiWithRuntimeDeps(lifecycle, deps)
}
