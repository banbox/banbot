package config

import (
	"errors"
	"strings"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

func TestRunPolicyRequireAndRequiref(t *testing.T) {
	pol := &RunPolicyConfig{
		Name:   "demo:ma",
		Params: map[string]float64{"fast": 30, "slow": 10},
	}
	fast := pol.DefInt("fast", 10, nil)
	slow := pol.DefInt("slow", 30, nil)

	// Passing constraints must not panic.
	pol.Require(fast > 0, "fast must be positive")
	pol.Requiref(slow > 0, "slow(%d) must be positive", slow)

	t.Run("Require failure produces BadConfig error", func(t *testing.T) {
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			pol.Require(fast < slow, "fast must be less than slow")
		}()
		if recovered == nil {
			t.Fatal("expected Require to panic when condition is false")
		}
		badParam, ok := AsBadParamError(recovered)
		if !ok || badParam == nil {
			t.Fatalf("recovered = %T (%v), want *BadParamError", recovered, recovered)
		}
		if badParam.Policy != "demo:ma" {
			t.Fatalf("Policy = %q, want %q", badParam.Policy, "demo:ma")
		}
		if badParam.Error() != "[BadConfig] fast must be less than slow" {
			t.Fatalf("Error() = %q, want %q", badParam.Error(), "[BadConfig] fast must be less than slow")
		}
		banErr := badParam.BanError()
		if banErr == nil || banErr.Code != core.ErrBadConfig || banErr.CodeName() != "BadConfig" {
			t.Fatalf("BanError() = %#v, want Code=ErrBadConfig(BadConfig)", banErr)
		}
		if banErr.Short() != "[BadConfig] fast must be less than slow" {
			t.Fatalf("Short() = %q", banErr.Short())
		}
		if !strings.HasPrefix(banErr.Error(), "[BadConfig] fast must be less than slow\n") {
			t.Fatalf("BanError().Error() = %q, want [BadConfig] prefix with stack", banErr.Error())
		}
		var unwrapped *errs.Error
		if !errors.As(badParam, &unwrapped) || unwrapped.Code != core.ErrBadConfig {
			t.Fatalf("errors.As(*errs.Error) failed: %v", unwrapped)
		}
	})

	t.Run("Require preserves literal percent signs", func(t *testing.T) {
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			pol.Require(false, "callback rate must be <= 50%")
		}()
		badParam, ok := AsBadParamError(recovered)
		if !ok || badParam.Error() != "[BadConfig] callback rate must be <= 50%" {
			t.Fatalf("unexpected Require message with %%: %v", recovered)
		}
	})

	t.Run("Requiref formats message as BadConfig", func(t *testing.T) {
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			pol.Requiref(fast < slow, "require fast(%d) < slow(%d)", fast, slow)
		}()
		badParam, ok := AsBadParamError(recovered)
		if !ok || badParam.Error() != "[BadConfig] require fast(30) < slow(10)" {
			t.Fatalf("unexpected Requiref error: %v", recovered)
		}
	})

	t.Run("nil receiver is safe", func(t *testing.T) {
		var nilPol *RunPolicyConfig
		nilPol.Require(true, "unused")
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			nilPol.Require(false, "nil receiver constraint")
		}()
		badParam, ok := AsBadParamError(recovered)
		if !ok || badParam.BanError().Short() != "[BadConfig] nil receiver constraint" {
			t.Fatalf("unexpected nil receiver error: %v", recovered)
		}
	})
}
