package dev

import (
	"context"
	"fmt"
	"os"

	utils2 "github.com/banbox/banexg/utils"

	"github.com/banbox/banbot/utils"

	"github.com/banbox/banbot/web/ui"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/cors"

	"github.com/banbox/banbot/web/base"
	"github.com/gofiber/fiber/v2"
	"github.com/spf13/cobra"
)

// ServerFactory is implemented by the entry layer. It owns runtime/session
// construction and returns a cleanup function for borrowed resources.
type ServerFactory func(*CmdArgs) (*DevServer, func(), error)

// Run executes the developer Web command. Supplying a factory keeps all
// runtime ownership explicit; the optional form preserves the historical API
// while returning a clear error when no runtime factory is available.
func Run(args []string, factories ...ServerFactory) error {
	if args == nil {
		args = os.Args[1:]
	}
	command := NewCommand(factories...)
	command.SetArgs(args)
	return command.Execute()
}

// NewCommand preserves the public command-construction API. Embedders should
// pass one typed factory so each command invocation owns an isolated runtime.
func NewCommand(factories ...ServerFactory) *cobra.Command {
	return newCommand(firstServerFactory(factories))
}

// NewCommandWithFactory constructs the developer Web command without using
// package-global runtime setup. Entry supplies the explicit server runtime.
func NewCommandWithFactory(factory ServerFactory) *cobra.Command {
	return newCommand(factory)
}

func firstServerFactory(factories []ServerFactory) ServerFactory {
	if len(factories) > 1 {
		panic("at most one dev server factory may be supplied")
	}
	if len(factories) == 1 {
		return factories[0]
	}
	return nil
}

func newCommand(factory ServerFactory) *cobra.Command {
	isDocker := utils.IsDocker()
	ag := &CmdArgs{}
	defHost := "127.0.0.1"
	if isDocker {
		defHost = "0.0.0.0"
	}
	command := &cobra.Command{
		Use:   "web",
		Short: "run the Web UI",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if factory == nil {
				return fmt.Errorf("typed dev server factory is required")
			}
			return runWebWithFactory(ag, factory)
		},
	}
	command.Flags().IntVar(&ag.Port, "port", 8000, "port to listen on")
	command.Flags().StringVar(&ag.Host, "host", defHost, "host IP to bind")
	command.Flags().StringVar(&ag.LogLevel, "level", "info", "logging level")
	command.Flags().StringVar(&ag.TimeZone, "tz", "", "timezone")
	command.Flags().StringVar(&ag.DataDir, "datadir", "", "path to the data directory")
	command.Flags().StringVar(&ag.Password, "password", "", "password required to access the Web UI")
	command.Flags().StringVar(&ag.ConfigData, "config-data", "", "inline YAML config")
	command.Flags().StringArrayVar((*[]string)(&ag.Configs), "config", nil, "config path; may be repeated")
	command.Flags().StringVar(&ag.LogFile, "logfile", "", "log file path; defaults to the system temp directory")
	return command
}

func runWebWithFactory(ag *CmdArgs, factory ServerFactory) error {
	if err := validateWebAuth(ag.Host, ag.Password); err != nil {
		return err
	}
	server, cleanup, err := factory(ag)
	if err != nil {
		return err
	}
	if server == nil || server.Data == nil {
		if cleanup != nil {
			cleanup()
		}
		return fmt.Errorf("typed dev server and data dependencies are required")
	}
	defer func() {
		server.Stop()
		server.Join()
		if cleanup != nil {
			cleanup()
		}
	}()
	if err := server.collectBtResults(); err != nil {
		return err
	}
	server.startBtTaskScheduler()

	app := fiber.New(fiber.Config{AppName: "banbot", ErrorHandler: base.ErrHandler, JSONEncoder: utils2.Marshal})
	app.Use(cors.New(cors.Config{AllowOrigins: "*"}))
	if ag.Password != "" {
		app.Use(basicauth.New(basicauth.Config{Users: map[string]string{"banbot": ag.Password}, Realm: "BanBot WebUI"}))
	}
	base.RegApiKlineWithRuntimeDeps(app.Group("/api/kline"), *server.Data)
	base.RegApiCsvAt(app.Group("/api/kline"), server.DataDir())
	hub := base.NewWsHub(server.Data)
	defer func() { hub.Close(); hub.Join() }()
	base.RegApiWebsocketWithHub(app.Group("/api/ws"), hub)
	server.RegAPI(app.Group("/api/dev"))
	if err := ui.ServeStatic(app); err != nil {
		return err
	}
	return listenWithContext(server.ctx, app, fmt.Sprintf("%s:%v", ag.Host, ag.Port))
}

func listenWithContext(ctx context.Context, app *fiber.App, address string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			_ = app.Shutdown()
		case <-stopped:
		}
	}()
	return app.Listen(address)
}
