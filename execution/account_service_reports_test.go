package execution

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type setupFailureAdapter struct {
	*fakeExecutionAdapter
	reports func(context.Context) (<-chan BanexgStreamReport, error)
}

func (a *setupFailureAdapter) Reports(ctx context.Context) (<-chan BanexgStreamReport, error) {
	return a.reports(ctx)
}

func reportSetupService(t *testing.T, reports func(context.Context) (<-chan BanexgStreamReport, error)) *SharedAccountBorrow {
	t.Helper()
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewSharedAccount(owner, SharedExecutionOptions{StorePath: filepath.Join(t.TempDir(), "execution.db"), SenderLeaseDir: t.TempDir(), Adapter: &setupFailureAdapter{fakeExecutionAdapter: &fakeExecutionAdapter{}, reports: reports}})
	if err != nil {
		t.Fatal(err)
	}
	service.ready = true
	borrow := service.Borrow()
	t.Cleanup(func() { borrow.Release(); registry.Close(); service.Close() })
	return borrow
}

func TestPrivateReportRegistrationFailureJoinsAcceptedSource(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "unsettled-hint"}[queued], func(t *testing.T) {
			failure := errors.New("private registration failed after opening source")
			joined := make(chan struct{})
			borrow := reportSetupService(t, func(ctx context.Context) (<-chan BanexgStreamReport, error) {
				stream := make(chan BanexgStreamReport, 1)
				if queued {
					stream <- BanexgStreamReport{UnassignedExchangeID: "manual-order"}
				}
				go func() { <-ctx.Done(); close(joined); close(stream) }()
				return stream, failure
			})
			err := borrow.StartReports()
			if !errors.Is(err, failure) {
				t.Fatal("registration error lost", err)
			}
			select {
			case <-joined:
			default:
				t.Fatal("failed registration returned before accepted source joined")
			}
			if borrow.service.ready || borrow.service.reportDone != nil {
				t.Fatal("failed registration retained admission or a registered worker")
			}
			snapshot, readErr := borrow.Snapshot(context.Background())
			if readErr != nil || snapshot.RiskFrozen != queued {
				t.Fatal("unsettled hints did not preserve durable recovery freeze", snapshot, readErr)
			}
			if queued && !strings.Contains(err.Error(), "recovery hints remain unsettled") {
				t.Fatal("unsettled source error missing", err)
			}
			closeErr := borrow.service.Close()
			if queued != (closeErr != nil) {
				t.Fatal("source shutdown evidence lost at service close", closeErr)
			}
			opts := borrow.service.opts
			reopened, openErr := OpenStoreWithLeaseDir(opts.StorePath, borrow.service.store.Account(), opts.SenderLeaseDir)
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer reopened.Close()
			recovered, readErr := reopened.Snapshot(context.Background())
			if readErr != nil || recovered.RiskFrozen != queued {
				t.Fatal("source registration recovery freeze did not survive reopen", recovered, readErr)
			}
		})
	}
}

func TestPrivateReportNilRegistrationClosesAdmission(t *testing.T) {
	borrow := reportSetupService(t, func(context.Context) (<-chan BanexgStreamReport, error) { return nil, nil })
	if err := borrow.StartReports(); err == nil || borrow.service.ready {
		t.Fatal("nil registered stream retained account admission", err)
	}
}

func TestPrivateReportCompletedWorkerCannotMasqueradeAsRegistered(t *testing.T) {
	borrow := reportSetupService(t, func(context.Context) (<-chan BanexgStreamReport, error) {
		stream := make(chan BanexgStreamReport)
		close(stream)
		return stream, nil
	})
	if err := borrow.StartReports(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-borrow.service.reportDone:
	case <-time.After(time.Second):
		t.Fatal("closed source worker did not join")
	}
	if err := borrow.StartReports(); err == nil {
		t.Fatal("dead report worker reported successful registration")
	}
}
