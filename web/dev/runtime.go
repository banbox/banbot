package dev

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/orm/ormu"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DevServer owns mutable developer-web state for one configured runtime.
// Route migration can bind existing handlers to this instance incrementally.
type DevServer struct {
	Data        *data.RuntimeDeps
	configPaths []string

	runtimeFor  RuntimeFactory
	maintenance DataToolsRunner

	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	notify          chan *ormu.Task
	taskMu          sync.Mutex
	runningBtTasks  map[int64]*exec.Cmd
	taskStatusCache map[int64]*taskStatusInfo
	ordersMu        sync.Mutex
	cacheOrders     []*ormo.InOutOrder
	cachePath       string
	dataTools       DataToolsManager

	wsMu    sync.RWMutex
	stopped atomic.Bool
	status  ServerStatus
	clients map[*WsClient]struct{}
	buildMu sync.Mutex
}

// RuntimeFactory resolves one complete, identity-scoped data runtime.
type RuntimeFactory func(context.Context, string, string) (*data.RuntimeDeps, func(), *errs.Error)

// DataToolsRunner owns maintenance execution outside the web package.
type DataToolsRunner func(context.Context, *data.RuntimeDeps, *DataToolsArgs, *utils.StagedPrg) *errs.Error

// DevDeps is the typed construction contract supplied by the entry layer.
type DevDeps struct {
	Data        *data.RuntimeDeps
	ConfigPaths []string
	RuntimeFor  RuntimeFactory
	Maintenance DataToolsRunner
}

func NewDevServer(deps DevDeps) (*DevServer, *errs.Error) {
	if deps.Data == nil || deps.Data.Config == nil || deps.Data.Config.DataDir == "" || deps.Data.Storage == nil || deps.Data.Symbols == nil || deps.Data.Core == nil || deps.Data.Clock == nil || deps.Data.Catalog == nil || deps.Data.Exchange == nil {
		return nil, errs.NewMsg(errs.CodeParamRequired, "complete dev runtime dependencies are required")
	}
	return newDevServer(deps), nil
}

func newDevServer(deps DevDeps) *DevServer {
	parent := context.Background()
	if deps.Data != nil && deps.Data.Core != nil {
		parent = deps.Data.Core.Context()
	}
	ctx, cancel := context.WithCancel(parent)
	return &DevServer{Data: deps.Data,
		configPaths: append([]string(nil), deps.ConfigPaths...),
		runtimeFor:  deps.RuntimeFor, maintenance: deps.Maintenance, ctx: ctx, cancel: cancel,
		notify: make(chan *ormu.Task, 100), runningBtTasks: make(map[int64]*exec.Cmd), taskStatusCache: make(map[int64]*taskStatusInfo), clients: make(map[*WsClient]struct{})}
}

func (s *DevServer) PubDBPath() string {
	if s == nil || s.Data == nil || s.Data.Config == nil {
		return ""
	}
	return filepath.Join(s.Data.Config.DataDir, orm.DbPub+".db")
}

func (s *DevServer) DataDir() string {
	if s == nil || s.Data == nil || s.Data.Config == nil {
		return ""
	}
	return s.Data.Config.DataDir
}

func (s *DevServer) StrategyDir() string {
	if s == nil || s.Data == nil || s.Data.Config == nil {
		return ""
	}
	return s.Data.Config.StrategyDir
}

func (s *DevServer) BacktestDir() string { return filepath.Join(s.DataDir(), "backtest") }

func (s *DevServer) symbols() *orm.SymbolState {
	if s != nil && s.Data != nil {
		return s.Data.Symbols
	}
	return nil
}

func (s *DevServer) dataConn(ctx context.Context) (*orm.Queries, *pgxpool.Conn, *errs.Error) {
	var storage *orm.Storage
	if s != nil && s.Data != nil {
		storage = s.Data.Storage
	}
	if storage == nil {
		return nil, nil, errs.NewMsg(errs.CodeParamRequired, "dev server storage is required")
	}
	return storage.Conn(ctx)
}

func dataConnFor(ctx context.Context, deps *data.RuntimeDeps) (*orm.Queries, *pgxpool.Conn, *errs.Error) {
	if deps == nil || deps.Storage == nil {
		return nil, nil, errs.NewMsg(errs.CodeParamRequired, "dev server storage is required")
	}
	sess, conn, err := deps.Storage.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return sess.WithSeriesSymbolState(deps.Symbols).WithExchange(deps.Exchange), conn, nil
}

func (s *DevServer) identity() (string, string, error) {
	if s == nil || s.Data == nil {
		return "", "", fmt.Errorf("dev server data identity is required")
	}
	return s.Data.ResolveIdentity()
}

func (s *DevServer) dataFor(ctx context.Context, name, market string) (*data.RuntimeDeps, func(), *errs.Error) {
	if s == nil || s.Data == nil {
		return nil, nil, errs.NewMsg(errs.CodeParamRequired, "dev server data dependencies are required")
	}
	currentName, currentMarket, err := s.Data.ResolveIdentity()
	if err != nil {
		return nil, nil, errs.New(errs.CodeRunTime, err)
	}
	if name == "" {
		name = currentName
	}
	if market == "" {
		market = currentMarket
	}
	if name == currentName && market == currentMarket {
		return s.Data, func() {}, nil
	}
	if s.runtimeFor == nil {
		return nil, nil, errs.NewMsg(errs.CodeParamRequired, "dev server runtime factory is required")
	}
	deps, cleanup, runErr := s.runtimeFor(ctx, name, market)
	if runErr != nil {
		return nil, nil, runErr
	}
	if cleanup == nil {
		cleanup = func() {}
	}
	if deps == nil {
		cleanup()
		return nil, nil, errs.NewMsg(errs.CodeRunTime, "dev server runtime dependencies are required")
	}
	childName, childMarket, identityErr := deps.ResolveIdentity()
	if identityErr != nil {
		cleanup()
		return nil, nil, errs.New(errs.CodeRunTime, identityErr)
	}
	if childName != name || childMarket != market {
		cleanup()
		return nil, nil, errs.NewMsg(errs.CodeParamInvalid,
			"dev server runtime identity %q/%q does not match request %q/%q", childName, childMarket, name, market)
	}
	return deps, cleanup, nil
}

func (s *DevServer) ParsePath(path string) string {
	if s == nil || s.Data == nil || s.Data.Config == nil {
		return path
	}
	return s.Data.Config.ParsePath(path)
}

func (s *DevServer) Conn() (*ormu.Queries, *orm.TrackedDB, *errs.Error) {
	if s.PubDBPath() == "" {
		return nil, nil, errs.NewMsg(errs.CodeParamRequired, "dev server data directory is required")
	}
	queries, conn, err := ormu.ConnAt(s.PubDBPath())
	return queries, conn, err
}

func (s *DevServer) Stop() {
	if s == nil {
		return
	}
	if !s.stopped.CompareAndSwap(false, true) {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wsMu.RLock()
	clients := make([]*WsClient, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.wsMu.RUnlock()
	for _, client := range clients {
		client.interrupt()
	}
}
func (s *DevServer) Join() {
	if s != nil {
		s.wg.Wait()
	}
}
