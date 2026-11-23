package execution

import (
	"context"
	"testing"
)

// A native trigger or limit off the instrument tick would be refused while the
// plan is prepared (conditionalReservationFor), on every autonomous tick. The
// protection must instead fall back to the local, quote-compared protection.
func TestPendingNativeOffTickPricesFallBackToLocal(t *testing.T) {
	for _, field := range []string{"trigger", "limit"} {
		t.Run(field, func(t *testing.T) {
			service, _, definitions := pendingNativeFixture(t)
			key := string(StopLossProtection)
			trigger := definitions[0].Order.Protections[key]
			if field == "trigger" {
				definitions[0].Order.ProtectionLevels[key] = intentPrice("95.005")
			} else {
				trigger.Conditions.Limit = intentPrice("94.995")
				definitions[0].Order.Protections[key] = trigger
			}
			requests, err := service.CoordinatePending(context.Background(), definitions, pendingTestQuote("100"), 12)
			if err != nil {
				t.Fatal(err)
			}
			status := definitions[0].Order.ProtectionPlacement[key]
			if status.Effective != "local" || status.Reason != "conditional_price_off_tick" || len(requests[0].ConditionalReservations) != 0 {
				t.Fatal("off-tick native price not downgraded to local", status, requests)
			}
		})
	}
}
