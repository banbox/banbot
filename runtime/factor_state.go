package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/banbox/banbot/factor/runner"
)

// FactorState owns a task's replay lifecycle. Its bounded sessions and mutable
// account consumers belong to this task; no global computation cache exists.
type FactorState struct {
	ctx              context.Context
	cancel           context.CancelFunc
	mu               sync.Mutex
	started, stopped bool
	work             sync.WaitGroup
	configs          []runner.Config
	sinks            []runner.Sink
	outputs          []runner.Output
}

func (r *Runtime) InstallFactorReplay(configs []runner.Config, sinks []runner.Sink, outputs []runner.Output) error {
	if r == nil || len(configs) == 0 || len(configs) != len(sinks) || len(configs) != len(outputs) {
		return errors.New("runtime: complete factor replay composition required")
	}
	owned := make([]runner.Config, len(configs))
	for index, cfg := range configs {
		raw, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &owned[index]); err != nil {
			return err
		}
		owned[index].Plan, owned[index].ComputationGroup = cfg.Plan, cfg.ComputationGroup
		owned[index].PortfolioBuilder = cfg.PortfolioBuilder
		owned[index].HistoricalInput, owned[index].ObserveBatch = cfg.HistoricalInput, cfg.ObserveBatch
	}
	r.closeMu.Lock()
	if r.FactorState != nil || r.Context().Err() != nil {
		r.closeMu.Unlock()
		return errors.New("runtime: factor component already installed or task stopped")
	}
	ctx, cancel := context.WithCancel(r.Context())
	state := &FactorState{ctx: ctx, cancel: cancel, configs: owned, sinks: append([]runner.Sink(nil), sinks...), outputs: append([]runner.Output(nil), outputs...)}
	r.FactorState = state
	r.closeMu.Unlock()
	r.OnClose(state.Stop)
	r.OnCloseWait(state.Join)
	return nil
}

func (f *FactorState) Run(ctx context.Context) ([]runner.Result, error) {
	f.mu.Lock()
	if f.started || f.stopped {
		f.mu.Unlock()
		return nil, errors.New("runtime: factor replay is stopped or already run")
	}
	f.started = true
	f.work.Add(1)
	f.mu.Unlock()
	defer f.work.Done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(f.ctx, cancel)
	defer stop()
	if f.ctx.Err() != nil {
		return nil, f.ctx.Err()
	}
	return runner.RunMany(ctx, f.configs, f.sinks, f.outputs)
}
func (f *FactorState) Stop() { f.mu.Lock(); f.stopped = true; f.cancel(); f.mu.Unlock() }
func (f *FactorState) Join() { f.work.Wait() }
