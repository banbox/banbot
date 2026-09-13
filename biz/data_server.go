package biz

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type FnFeaStream = func(exsList []*orm.ExSymbol, req *SubReq, rsp FeaFeeder_SubFeaturesServer) error

var featureGenerators = map[string]FnFeaStream{}
var featureGeneratorsMu sync.RWMutex

// RegisterFeaGenerator registers or replaces the process-wide implementation
// for a feature task. Task registration is process-level; each invocation still
// receives symbols and dependencies owned by its request runtime.
func RegisterFeaGenerator(task string, generator FnFeaStream) {
	featureGeneratorsMu.Lock()
	featureGenerators[task] = generator
	featureGeneratorsMu.Unlock()
}

// GetFeaGenerator returns the registered process-wide task implementation.
func GetFeaGenerator(task string) (FnFeaStream, bool) {
	featureGeneratorsMu.RLock()
	generator, ok := featureGenerators[task]
	featureGeneratorsMu.RUnlock()
	return generator, ok
}

// SnapshotFeaGenerators returns a detached registry view for diagnostics.
func SnapshotFeaGenerators() map[string]FnFeaStream {
	featureGeneratorsMu.RLock()
	result := make(map[string]FnFeaStream, len(featureGenerators))
	for task, generator := range featureGenerators {
		result[task] = generator
	}
	featureGeneratorsMu.RUnlock()
	return result
}

// DataServerRuntimeFactory constructs an isolated runtime for one requested
// exchange identity. cleanup is called after the RPC stream ends.
type DataServerRuntimeFactory func(context.Context, string, string) (*data.RuntimeDeps, func(), *errs.Error)

// DataServer owns the gRPC listener and resolves every request through its
// runtime factory. It holds no process-global exchange or symbol state.
type DataServer struct {
	*UnimplementedFeaFeederServer
	runtimeFor   DataServerRuntimeFactory
	logger       *zap.Logger
	server       *grpc.Server
	listener     net.Listener
	done         chan struct{}
	shutdown     chan struct{}
	lifecycle    sync.Mutex
	stopOnce     sync.Once
	doneOnce     sync.Once
	shutdownOnce sync.Once
	started      bool
	stopped      bool
}

func NewDataServer(runtimeFor DataServerRuntimeFactory, logger *zap.Logger) (*DataServer, *errs.Error) {
	if runtimeFor == nil {
		return nil, errs.NewMsg(errs.CodeParamRequired, "data server runtime factory is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &DataServer{runtimeFor: runtimeFor, logger: logger, done: make(chan struct{}), shutdown: make(chan struct{})}, nil
}

// Serve listens until the supplied context is cancelled or Stop is called.
// It is safe to call Stop before Serve starts.
func (s *DataServer) Serve(ctx context.Context, address string) *errs.Error {
	if s == nil || s.runtimeFor == nil {
		return errs.NewMsg(errs.CodeParamRequired, "data server runtime factory is required")
	}
	s.lifecycle.Lock()
	if s.started {
		s.lifecycle.Unlock()
		return errs.NewMsg(errs.CodeRunTime, "data server is already started")
	}
	if s.stopped {
		s.lifecycle.Unlock()
		return nil
	}
	s.started = true
	s.lifecycle.Unlock()
	defer s.finishServe()
	if address == "" {
		address = ":6789"
	}
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return errs.New(errs.CodeNetFail, err)
	}
	maxMsgSize := 100 * 1024 * 1024
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxMsgSize), grpc.MaxSendMsgSize(maxMsgSize), grpc.WaitForHandlers(true))
	RegisterFeaFeederServer(server, s)
	s.lifecycle.Lock()
	if s.stopped {
		s.lifecycle.Unlock()
		_ = lis.Close()
		return nil
	}
	s.listener, s.server = lis, server
	s.lifecycle.Unlock()
	s.logger.Info("data server ready", zap.String("address", lis.Addr().String()))
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		select {
		case <-ctx.Done():
			s.Stop()
		case <-s.done:
		}
	}()
	err = server.Serve(lis)
	s.lifecycle.Lock()
	stopped := s.stopped
	s.lifecycle.Unlock()
	if !stopped {
		s.Stop()
	}
	<-s.shutdown
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
		return errs.New(errs.CodeNetFail, err)
	}
	return nil
}

func (s *DataServer) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		s.lifecycle.Lock()
		s.stopped = true
		started := s.started
		server, listener := s.server, s.listener
		s.lifecycle.Unlock()
		if !started {
			s.finishServe()
			s.finishShutdown()
			return
		}
		go func() {
			if server != nil {
				server.Stop()
			}
			if listener != nil {
				_ = listener.Close()
			}
			s.finishShutdown()
		}()
	})
}

func (s *DataServer) Join() { // Serve is synchronous; kept as the owner lifecycle surface.
	if s == nil || s.done == nil {
		return
	}
	<-s.done
	s.lifecycle.Lock()
	stopped := s.stopped
	s.lifecycle.Unlock()
	if stopped {
		<-s.shutdown
	}
}

func (s *DataServer) finishServe() {
	if s == nil || s.done == nil {
		return
	}
	s.doneOnce.Do(func() { close(s.done) })
}

func (s *DataServer) finishShutdown() {
	if s == nil || s.shutdown == nil {
		return
	}
	s.shutdownOnce.Do(func() { close(s.shutdown) })
}

// Address returns the bound listener address while the server is running.
func (s *DataServer) Address() string {
	if s == nil {
		return ""
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *DataServer) resolveFeatures(ctx context.Context, req *SubReq) ([]*orm.ExSymbol, FnFeaStream, func(), error) {
	if req == nil || req.Exchange == "" || req.Market == "" || req.Task == "" {
		return nil, nil, nil, errs.NewMsg(errs.CodeParamRequired, "exchange, market, and task are required")
	}
	deps, cleanup, err := s.runtimeFor(ctx, req.Exchange, req.Market)
	if err != nil {
		return nil, nil, nil, err
	}
	if cleanup == nil {
		cleanup = func() {}
	}
	if deps == nil || deps.Symbols == nil || deps.Exchange == nil {
		cleanup()
		return nil, nil, nil, errs.NewMsg(errs.CodeRunTime, "data server runtime dependencies are incomplete")
	}
	exchange, market, identityErr := deps.ResolveIdentity()
	if identityErr != nil {
		cleanup()
		return nil, nil, nil, errs.New(errs.CodeRunTime, identityErr)
	}
	if exchange != req.Exchange || market != req.Market {
		cleanup()
		return nil, nil, nil, errs.NewMsg(errs.CodeParamInvalid,
			"data server runtime identity %q/%q does not match request %q/%q", exchange, market, req.Exchange, req.Market)
	}
	codes, dups := utils.UniqueItems(req.Codes)
	if len(dups) > 0 {
		s.logger.Info("found duplicate codes", zap.Int("valid", len(codes)), zap.Strings("dups", dups))
	}
	exsList := make([]*orm.ExSymbol, 0, len(codes))
	for _, code := range codes {
		exs, symbolErr := deps.Symbols.GetExSymbol(deps.Exchange, code)
		if symbolErr != nil {
			cleanup()
			return nil, nil, nil, symbolErr
		}
		exsList = append(exsList, exs)
	}
	gen, ok := GetFeaGenerator(req.Task)
	if !ok || gen == nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("unsupported data task: %s", req.Task)
	}
	return exsList, gen, cleanup, nil
}

// SubFeatures subscribes to one registered feature generator using symbols
// owned by the request runtime. Runtime cleanup happens after the stream,
// including generator failures and client cancellation.
func (s *DataServer) SubFeatures(req *SubReq, rsp FeaFeeder_SubFeaturesServer) error {
	if rsp == nil {
		return errs.NewMsg(errs.CodeParamRequired, "feature response stream is required")
	}
	exsList, gen, cleanup, err := s.resolveFeatures(rsp.Context(), req)
	if err != nil {
		return err
	}
	defer cleanup()
	return gen(exsList, req, rsp)
}
