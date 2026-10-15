package entry

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg"
)

func TestFactorLiveBindingPreparationFailureJoinsEveryResource(t *testing.T) {
	prepareErr, firstCloseErr, partialCloseErr := errors.New("prepare failed"), errors.New("first close failed"), errors.New("partial close failed")
	var closed []string
	factory := func(_ context.Context, _ banexg.BanExchange, _ *config.Snapshot, c runner.Config) (FactorLiveBinding, error) {
		binding := FactorLiveBinding{Close: func() error {
			closed = append(closed, c.AccountID)
			if c.AccountID == "first" {
				return firstCloseErr
			}
			return partialCloseErr
		}}
		if c.AccountID == "second" {
			return binding, prepareErr
		}
		return binding, nil
	}
	accounts := []string{"first", "second", "never"}
	configs := map[string]runner.Config{}
	for _, account := range accounts {
		configs[account] = runner.Config{AccountID: account}
	}
	bindings, err := prepareFactorLiveBindings(context.Background(), factory, nil, nil, accounts, configs)
	if len(bindings) != 0 || !errors.Is(err, prepareErr) || !errors.Is(err, firstCloseErr) || !errors.Is(err, partialCloseErr) {
		t.Fatalf("partial preparation/cleanup lost: %v, %v", bindings, err)
	}
	if !reflect.DeepEqual(closed, []string{"second", "first"}) {
		t.Fatalf("close order or leak: %v", closed)
	}
}

func TestFactorLiveBindingPreparationCancellationClosesPreparedResources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var prepared, closed int
	factory := func(context.Context, banexg.BanExchange, *config.Snapshot, runner.Config) (FactorLiveBinding, error) {
		prepared++
		cancel()
		return FactorLiveBinding{Close: func() error { closed++; return nil }}, nil
	}
	bindings, err := prepareFactorLiveBindings(ctx, factory, nil, nil, []string{"first", "second"}, map[string]runner.Config{})
	if !errors.Is(err, context.Canceled) || len(bindings) != 0 || prepared != 1 || closed != 1 {
		t.Fatalf("cancellation leaked binding or prepared another account: %v %d %d %d", err, len(bindings), prepared, closed)
	}
}

func TestFactorLiveBindingPreparationTransfersOwnershipUntilAccountJoin(t *testing.T) {
	var joined bool
	closed := 0
	factory := func(context.Context, banexg.BanExchange, *config.Snapshot, runner.Config) (FactorLiveBinding, error) {
		return FactorLiveBinding{Close: func() error {
			if !joined {
				t.Error("binding closed before admitted account work joined")
			}
			closed++
			return nil
		}}, nil
	}
	bindings, err := prepareFactorLiveBindings(context.Background(), factory, nil, nil, []string{"first"}, map[string]runner.Config{})
	if err != nil || closed != 0 || len(bindings) != 1 {
		t.Fatalf("successful binding prematurely closed: %v %d", err, closed)
	}
	process := runtimeProcessForBindingTest(t, &joined)
	if err := closeFactorLiveSession(&explicitEntrySession{process: process}, bindings); err != nil || closed != 1 {
		t.Fatalf("joined binding cleanup: %v %d", err, closed)
	}
}

func runtimeProcessForBindingTest(t *testing.T, joined *bool) *runtime.Process {
	t.Helper()
	process := runtime.NewProcess()
	rt, err := process.NewRuntime(runtime.Options{Mode: "other"})
	if err != nil {
		t.Fatal(err)
	}
	rt.OnCloseWait(func() { *joined = true })
	return process
}
