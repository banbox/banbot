package dev

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	utils2 "github.com/banbox/banexg/utils"

	"github.com/banbox/banbot/utils"

	"github.com/banbox/banbot/web/ui"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/cors"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/web/base"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/gofiber/fiber/v2"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func Run(args []string) error {
	if args == nil {
		args = os.Args[1:]
	}
	command := NewCommand()
	command.SetArgs(args)
	return command.Execute()
}

func NewCommand() *cobra.Command {
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
			return runWeb(ag)
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

func runWeb(ag *CmdArgs) error {
	if err_ := validateWebAuth(ag.Host, ag.Password); err_ != nil {
		return err_
	}

	// 检查并设置日志文件输出
	if ag.LogFile == "" {
		cacheDir, err_ := utils2.GetCacheDir()
		if err_ != nil {
			return err_
		}
		logDir := filepath.Join(cacheDir, "banbot")
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory: %v", err)
		}
		logFileName := time.Now().Format("20060102150405") + ".log"
		ag.LogFile = filepath.Join(logDir, logFileName)
	}
	// 初始化基础数据
	core.SetRunMode(core.RunModeLive)
	banArg := &config.CmdArgs{
		DataDir:     ag.DataDir,
		LogLevel:    ag.LogLevel,
		TimeZone:    ag.TimeZone,
		Configs:     ag.Configs,
		ConfigData:  ag.ConfigData,
		Logfile:     ag.LogFile,
		AutoCompact: true,
	}
	var err2 *errs.Error
	if err2 = biz.SetupComsExg(banArg); err2 != nil {
		return err2
	}
	if utils.IsDocker() {
		// docker内运行banbot时，启动时外部传入了额外配置，更新到config.local.yml
		err2 = config.UpdateLocal(ag.Configs, ag.ConfigData, false)
		if err2 != nil {
			return err2
		}
	}
	err_ := collectBtResults()
	if err_ != nil {
		return err_
	}
	startBtTaskScheduler()
	num := len(orm.GetAllExSymbols())
	log.Info("loaded symbols", zap.Int("num", num))

	// 新建web应用
	app := fiber.New(fiber.Config{
		AppName:      "banbot",
		ErrorHandler: base.ErrHandler,
		JSONEncoder:  utils2.Marshal,
	})

	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
	}))
	if ag.Password != "" {
		app.Use(basicauth.New(basicauth.Config{
			Users: map[string]string{"banbot": ag.Password},
			Realm: "BanBot WebUI",
		}))
	}

	// 注册API路由
	base.RegApiKline(app.Group("/api/kline"))
	base.RegApiCsv(app.Group("/api/kline"))
	base.RegApiWebsocket(app.Group("/api/ws"))
	regApiDev(app.Group("/api/dev"))

	// 添加静态文件服务
	err_ = ui.ServeStatic(app)
	if err_ != nil {
		return err_
	}

	// 启动k线监听和websocket推送
	//go base.RunReceiver()

	bindUrl := fmt.Sprintf("%s:%v", ag.Host, ag.Port)
	lang := utils.GetSystemLanguage()
	openUrl := "http://" + bindUrl
	if lang == "zh-CN" {
		openUrl += "/zh-CN"
	}

	// 延迟500ms打开浏览器
	if utils.IsDocker() {
		log.Info("please open browser to: " + openUrl)
	} else {
		utils.OpenBrowserDelay(openUrl, 500)
	}

	return app.Listen(bindUrl)
}
