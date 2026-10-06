package runner

import (
	"errors"
	"github.com/banbox/banbot/factor"
	"slices"
	"strings"
	"sync"
)

type PortfolioPolicyFactory = factor.PortfolioPolicyFactory

var portfolioPolicies = struct {
	sync.RWMutex
	factories map[string]PortfolioPolicyFactory
}{factories: map[string]PortfolioPolicyFactory{"lifecycle-v1": factor.NewLifecyclePolicy}}

func RegisterPortfolioPolicy(name string, factory PortfolioPolicyFactory) error {
	if name == "" || !strings.Contains(name, "-v") || factory == nil {
		return errors.New("runner: custom portfolio policy requires versioned name and factory")
	}
	portfolioPolicies.Lock()
	defer portfolioPolicies.Unlock()
	if _, ok := portfolioPolicies.factories[name]; ok {
		return errors.New("runner: portfolio policy already registered")
	}
	portfolioPolicies.factories[name] = factory
	return nil
}

// NewPortfolioPolicy creates a private instance for one run/strategy.
func NewPortfolioPolicy(c factor.PortfolioPolicyConfig) (factor.PortfolioPolicy, error) {
	portfolioPolicies.RLock()
	f, ok := portfolioPolicies.factories[c.Policy]
	portfolioPolicies.RUnlock()
	if !ok {
		return nil, errors.New("runner: unknown portfolio policy")
	}
	return f(factor.ClonePortfolioPolicyConfig(c))
}
func PortfolioPolicyCatalog() []string {
	portfolioPolicies.RLock()
	defer portfolioPolicies.RUnlock()
	names := []string{}
	for name := range portfolioPolicies.factories {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
