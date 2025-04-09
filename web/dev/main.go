package dev

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/banbox/banbot/utils"

	"github.com/banbox/banbot/web/ui"
	"github.com/gofiber/fiber/v2/middleware/cors"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/web/base"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

func Run(args []string) error {
	isDocker := utils.IsDocker()
	var ag = &CmdArgs{}
	var f = flag.NewFlagSet("web", flag.ExitOnError)
	f.IntVar(&ag.Port, "port", 8000, "port to listen")
	defHost := "127.0.0.1"
	if isDocker {
		defHost = "0.0.0.0"
	}
	f.StringVar(&ag.Host, "host", defHost, "bind host ip")
	f.StringVar(&ag.LogLevel, "level", "info", "log level")
	f.StringVar(&ag.TimeZone, "tz", "", "timezone")
	f.StringVar(&ag.DataDir, "datadir", "", "Path to data dir.")
	f.Var(&ag.Configs, "config", "config path to use, Multiple -config options may be used")
	f.StringVar(&ag.LogFile, "logfile", "", "log file path, default: system temp dir")
	f.StringVar(&ag.DBFile, "db", "dev.db", "db file path")
	if args == nil {
		args = os.Args[1:]
	}
	err_ := f.Parse(args)
	if err_ != nil {
		return err_
	}

	// 检查并设置日志文件输出
	if ag.LogFile == "" {
		logDir := filepath.Join(os.TempDir(), "banbot")
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory: %v", err)
		}
		logFileName := time.Now().Format("20060102150405") + ".log"
		ag.LogFile = filepath.Join(logDir, logFileName)
	}

	// 初始化基础数据
	core.SetRunMode(core.RunModeLive)
	banArg := &config.CmdArgs{
		DataDir:  ag.DataDir,
		LogLevel: ag.LogLevel,
		TimeZone: ag.TimeZone,
		Configs:  ag.Configs,
		Logfile:  ag.LogFile,
	}
	core.DevDbPath = ag.DBFile
	var err2 *errs.Error
	if err2 = biz.SetupComsExg(banArg); err2 != nil {
		return err2
	}
	err_ = collectBtResults()
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
	})

	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
	}))

	// 注册API路由
	base.RegApiKline(app.Group("/api/kline"))
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
	if isDocker {
		log.Info("please open browser to: " + openUrl)
	} else {
		utils.OpenBrowserDelay(openUrl, 500)
	}

	return app.Listen(bindUrl)
}
