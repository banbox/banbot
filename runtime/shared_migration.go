package runtime

import (
	"errors"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/execution"
)

// CutoverLegacyExecution closes and joins the actual old runtime before account
// migration. Close clears its manager registries only after accepted callbacks
// and provider/network lifecycle hooks finish. Audit strings grant no authority.
func (r *Runtime) CutoverLegacyExecution(legacy *Runtime, request execution.LegacyMigration) (bool, error) {
	if r == nil || legacy == nil || r == legacy || r.sharedExecution == nil || r.sharedOrderBridge == nil || legacy.sharedExecution != nil {
		return false, errors.New("runtime: distinct legacy and shared runtime required")
	}
	legacy.Close()
	legacy.Join()
	return biz.ImportLegacyMigration(r.sharedExecution, request, r.sharedOrderBridge, func() error {
		select {
		case <-legacy.Done():
		default:
			return errors.New("runtime: legacy runtime is still accepting work")
		}
		if len(legacy.Trading.OrderManagersSnapshot()) != 0 {
			return errors.New("runtime: legacy order manager remains active")
		}
		return nil
	})
}
