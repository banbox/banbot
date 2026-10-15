package execution

import "context"

// RefreshWorkingOrders repairs missed stream updates without repeatedly
// querying settled history. Startup still verifies the complete durable history.
// Expired resting orders are canceled and their final cumulative fills recovered.
func (b *SharedAccountBorrow) RefreshWorkingOrders(ctx context.Context, now int64) error {
	return b.use(func(s *SharedAccount) error {
		s.ready = false
		operationCtx, done := joinedOperationContext(b.ctx, ctx)
		defer done()
		snapshot, err := s.store.Snapshot(operationCtx)
		if err != nil {
			return err
		}
		executor := s.executorFor(operationCtx)
		for _, order := range snapshot.Orders {
			if order.State == OrderPrepared {
				continue
			}
			if err := executor.Recover(order.Intent.ID); err != nil {
				return err
			}
			current, err := s.store.Order(operationCtx, order.Intent.ID)
			if err != nil {
				return err
			}
			if terminalOrder(string(current.State)) {
				continue
			}
			plan, err := s.store.Plan(operationCtx, current.Intent.PlanID)
			if err != nil {
				return err
			}
			if now >= plan.ExpiresMS {
				if err := executor.Cancel(current.Intent.ID, now); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
