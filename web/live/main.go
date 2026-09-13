package live

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banexg/utils"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/web/base"
	"github.com/banbox/banbot/web/ui"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"go.uber.org/zap"
)

type ServerLifecycle interface {
	OnClose(func())
	OnCloseWait(func())
}

type apiServer struct {
	app            *fiber.App
	hub            *base.WsHub
	done           chan struct{}
	shutdownDone   chan struct{}
	stopOnce       sync.Once
	shutdownFinish sync.Once
	stateMu        sync.Mutex
	listenFinished bool
	shutdownStart  bool
}

// ApiServer exposes the lifecycle owner without exposing its implementation fields.
type ApiServer = apiServer

func (s *apiServer) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.hub != nil {
			s.hub.Close()
		}
		s.stateMu.Lock()
		if s.listenFinished {
			s.stateMu.Unlock()
			s.finishShutdown()
			return
		}
		s.shutdownStart = true
		s.stateMu.Unlock()
		go func() {
			defer s.finishShutdown()
			if s.app != nil {
				_ = s.app.Shutdown()
			}
		}()
	})
}

func (s *apiServer) Join() {
	if s != nil {
		<-s.done
		<-s.shutdownDone
		if s.hub != nil {
			s.hub.Join()
		}
	}
}

func newAPIServer(app *fiber.App, listen func() error) *apiServer {
	return newAPIServerWithHub(app, listen, nil)
}
func newAPIServerWithHub(app *fiber.App, listen func() error, hub *base.WsHub) *apiServer {
	server := &apiServer{
		app:          app,
		hub:          hub,
		done:         make(chan struct{}),
		shutdownDone: make(chan struct{}),
	}
	go func() {
		defer close(server.done)
		if err := listen(); err != nil && !isShutdownError(err) {
			log.Error("run api fail", zap.Error(err))
		}
		server.stateMu.Lock()
		server.listenFinished = true
		shutdownStarted := server.shutdownStart
		server.stateMu.Unlock()
		if !shutdownStarted {
			if server.hub != nil {
				server.hub.Close()
			}
			server.finishShutdown()
		}
	}()
	return server
}

func (s *apiServer) finishShutdown() {
	if s == nil {
		return
	}
	s.shutdownFinish.Do(func() { close(s.shutdownDone) })
}

func isShutdownError(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection")
}

// StartApiWithRuntimeDeps starts an API server whose routes use one explicit
// runtime.
func StartApiWithRuntimeDeps(lifecycle ServerLifecycle, deps biz.RuntimeDeps) (*apiServer, *errs.Error) {
	return startApiWithDeps(lifecycle, &deps)
}

func startApiWithLifecycle(lifecycle ServerLifecycle) (*apiServer, *errs.Error) {
	cfg := config.APIServer
	if cfg == nil || !cfg.Enable {
		return nil, nil
	}
	return startAPIServer(lifecycle, cfg, nil)
}

func startApiWithDeps(lifecycle ServerLifecycle, deps *biz.RuntimeDeps) (*apiServer, *errs.Error) {
	if deps == nil || deps.ConfigView() == nil {
		return nil, errs.NewMsg(errs.CodeParamRequired, "runtime configuration is required")
	}
	cfg := deps.ConfigView().APIServer
	if cfg == nil || !cfg.Enable {
		return nil, nil
	}
	if err := validateAPIRuntimeDeps(deps); err != nil {
		return nil, err
	}
	return startAPIServer(lifecycle, cfg, deps)
}

func validateAPIRuntimeDeps(deps *biz.RuntimeDeps) *errs.Error {
	if deps == nil || deps.Core == nil || deps.Clock == nil || deps.Symbols == nil || deps.Storage == nil || deps.Catalog == nil || deps.Exchange == nil || deps.Strategies == nil || deps.Orders == nil || deps.Trading == nil {
		return errs.NewMsg(core.ErrBadConfig, "runtime api requires core, clock, symbols, storage, catalog, exchange, strategies, orders, and trading state")
	}
	return nil
}

func startAPIServer(lifecycle ServerLifecycle, cfg *config.APIServerConfig, deps *biz.RuntimeDeps) (*apiServer, *errs.Error) {
	app := fiber.New(fiber.Config{
		AppName:      "banbot",
		ErrorHandler: base.ErrHandler,
		JSONEncoder:  utils.Marshal,
	})

	app.Use(cors.New(cors.Config{
		AllowOrigins:     strings.Join(cfg.CORSOrigins, ", "),
		AllowMethods:     "*",
		AllowHeaders:     "*",
		AllowCredentials: len(cfg.CORSOrigins) > 0,
		ExposeHeaders:    "*",
	}))

	// register routes 注册路由
	if deps == nil {
		base.RegApiKline(app.Group("/api/kline"))
		base.RegApiCsv(app.Group("/api/kline"))
	} else {
		base.RegApiKlineWithRuntimeDeps(app.Group("/api/kline"), *deps.DataDeps())
		dataDir := deps.Config.DataDir
		base.RegApiCsvAt(app.Group("/api/kline"), dataDir)
	}
	var hub *base.WsHub
	if deps == nil {
		base.RegApiWebsocket(app.Group("/api/ws"))
	} else {
		hub = base.NewWsHub(deps.DataDeps())
		base.RegApiWebsocketWithHub(app.Group("/api/ws"), hub)
	}
	if deps == nil {
		regApiBiz(app.Group("/api/bot", AuthMiddleware(cfg.JWTSecretKey)))
		regApiPub(app.Group("/api"))
	} else {
		handlers := newAPIHandlers(deps)
		handlers.regApiBiz(app.Group("/api/bot", handlers.authMiddleware(cfg.JWTSecretKey)))
		handlers.regApiPub(app.Group("/api"))
	}

	// 添加静态文件服务
	var err_ error
	if deps == nil {
		err_ = ui.ServeStatic(app)
	} else {
		sysLang := ""
		if deps.Core != nil {
			sysLang = deps.Core.SysLang
		}
		err_ = ui.ServeStaticAt(app, deps.Config.DataDir, sysLang)
	}
	if err_ != nil {
		if hub != nil {
			hub.Close()
			hub.Join()
		}
		return nil, errs.New(errs.CodeRunTime, err_)
	}

	addr := fmt.Sprintf("%s:%v", cfg.BindIPAddr, cfg.Port)
	log.Info("serve bot api at", zap.String("addr", addr))
	server := newAPIServerWithHub(app, func() error { return app.Listen(addr) }, hub)
	if lifecycle != nil {
		lifecycle.OnClose(server.Stop)
		lifecycle.OnCloseWait(server.Join)
	}
	return server, nil
}

func StartApi() *errs.Error {
	server, err := startApiWithLifecycle(nil)
	if err != nil || server == nil {
		return err
	}
	go func() {
		server.Join()
	}()
	return nil
}

func (s *apiServer) PublishSeries(msg *data.SeriesMsg) {
	if s != nil && s.hub != nil {
		s.hub.Publish(msg)
	}
}
