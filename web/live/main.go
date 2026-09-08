package live

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/banbox/banbot/legacygate"
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
	done           chan struct{}
	shutdownDone   chan struct{}
	stopOnce       sync.Once
	shutdownFinish sync.Once
	gateRelease    sync.Once
	releaseGate    func()
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
	s.releaseLegacyGate()
	s.stopOnce.Do(func() {
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
		defer s.releaseLegacyGate()
		<-s.done
		<-s.shutdownDone
	}
}

func newAPIServer(app *fiber.App, listen func() error, releaseGate ...func()) *apiServer {
	server := &apiServer{
		app:          app,
		done:         make(chan struct{}),
		shutdownDone: make(chan struct{}),
	}
	if len(releaseGate) > 0 {
		server.releaseGate = releaseGate[0]
	}
	go func() {
		defer server.releaseLegacyGate()
		defer close(server.done)
		if err := listen(); err != nil && !isShutdownError(err) {
			log.Error("run api fail", zap.Error(err))
		}
		server.stateMu.Lock()
		server.listenFinished = true
		shutdownStarted := server.shutdownStart
		server.stateMu.Unlock()
		if !shutdownStarted {
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

func (s *apiServer) releaseLegacyGate() {
	if s == nil {
		return
	}
	s.gateRelease.Do(func() {
		if s.releaseGate != nil {
			s.releaseGate()
		}
	})
}

func isShutdownError(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection")
}

func StartApiWithLifecycle(lifecycle ServerLifecycle) (*apiServer, *errs.Error) {
	unlock := legacygate.Lock()
	server, err := startApiWithLifecycle(lifecycle, unlock)
	if err != nil || server == nil {
		unlock()
	}
	return server, err
}

// StartApiWithLifecycleInLegacySession starts the API while the caller already
// owns legacygate. The caller keeps that session alive for the server lifetime.
func StartApiWithLifecycleInLegacySession(lifecycle ServerLifecycle) (*apiServer, *errs.Error) {
	return startApiWithLifecycle(lifecycle)
}

func startApiWithLifecycle(lifecycle ServerLifecycle, releaseGate ...func()) (*apiServer, *errs.Error) {
	cfg := config.APIServer
	if cfg == nil || !cfg.Enable {
		return nil, nil
	}
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
	base.RegApiKline(app.Group("/api/kline"))
	base.RegApiCsv(app.Group("/api/kline"))
	base.RegApiWebsocket(app.Group("/api/ws"))
	regApiBiz(app.Group("/api/bot", AuthMiddleware(cfg.JWTSecretKey)))
	regApiPub(app.Group("/api"))

	// 添加静态文件服务
	err_ := ui.ServeStatic(app)
	if err_ != nil {
		return nil, errs.New(errs.CodeRunTime, err_)
	}

	addr := fmt.Sprintf("%s:%v", cfg.BindIPAddr, cfg.Port)
	log.Info("serve bot api at", zap.String("addr", addr))
	server := newAPIServer(app, func() error { return app.Listen(addr) })
	if lifecycle != nil {
		lifecycle.OnClose(server.Stop)
		lifecycle.OnCloseWait(server.Join)
	}
	return server, nil
}

// StartApiInLegacySession starts the legacy API while the caller already owns
// legacygate. Legacy entrypoints use this form because legacygate is not
// re-entrant; the caller keeps the session alive for the server lifetime.
func StartApiInLegacySession() *errs.Error {
	_, err := startApiWithLifecycle(nil)
	return err
}

func StartApi() *errs.Error {
	unlock := legacygate.Lock()
	server, err := startApiWithLifecycle(nil, unlock)
	if err != nil || server == nil {
		unlock()
		return err
	}
	go func() {
		server.Join()
	}()
	return nil
}
