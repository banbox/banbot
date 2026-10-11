package statistics

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// DispatchReduce processes immutable job indices asynchronously, reducing in
// index order. Only workers results are retained, even when an early job is
// slow. Mapper results must own mutable data until consumed by the reducer.
// A mapper must honor cancellation; no worker is abandoned on return.
// Reducer state must be privately owned: errors return zero, never partial success.
func DispatchReduce[R, A any](ctx context.Context, jobs, workers int, initial A, mapJob func(context.Context, int) (R, error), reduce func(A, R) (A, error)) (A, error) {
	var zero A
	if ctx == nil || jobs < 1 || workers < 1 || mapJob == nil || reduce == nil {
		return zero, errors.New("statistics: invalid dispatch specification")
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	workers = min(workers, jobs)
	type result struct {
		value R
		err   error
	}
	// Bounded windows keep both callback concurrency and reordered outputs bounded.
	for first := 0; first < jobs; first += workers {
		if err := ctx.Err(); err != nil {
			return zero, context.Cause(ctx)
		}
		size := min(workers, jobs-first)
		slots := make([]chan result, size)
		var wg sync.WaitGroup
		for offset := range size {
			slots[offset] = make(chan result, 1)
			wg.Add(1)
			go func(offset int) {
				defer wg.Done()
				value, err := mapJob(ctx, first+offset)
				if err != nil {
					cancel(fmt.Errorf("statistics: job%d: %w", first+offset, err))
				}
				slots[offset] <- result{value, err}
			}(offset)
		}
		var failure error
		for offset := range size {
			got := <-slots[offset]
			if got.err != nil {
				failure = errors.Join(failure, fmt.Errorf("statistics: job%d: %w", first+offset, got.err))
			}
			if failure == nil && ctx.Err() == nil {
				var err error
				initial, err = reduce(initial, got.value)
				if err != nil {
					failure = err
					cancel(err)
				}
			}
		}
		wg.Wait()
		if cause := context.Cause(ctx); cause != nil {
			return zero, errors.Join(cause, failure)
		}
		if failure != nil {
			return zero, failure
		}
		if err := ctx.Err(); err != nil {
			return zero, err
		}
	}
	return initial, nil
}
