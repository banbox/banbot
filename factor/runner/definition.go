package runner

import (
	"errors"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"slices"
	"sync"
)

// DefinitionBuilder compiles registered Go definitions; parameters remain in
// Config/Manifest rather than a second expression language.
type DefinitionBuilder func(Config) (*factor.Plan, research.ComboSpec, error)

// PortfolioBuilder accepts frozen inference and budget evidence, never labels.
type PortfolioBuilder func(factor.Frame, factor.Universe, factor.PortfolioSpec, research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error)

var definitions = struct {
	sync.RWMutex
	builders          map[string]DefinitionBuilder
	portfolios        map[string]PortfolioBuilder
	portfolioIdentity map[string]string
}{builders: map[string]DefinitionBuilder{
	"momentum-vol": func(c Config) (*factor.Plan, research.ComboSpec, error) { return research.MomentumVolPlan(c.Factor) },
}}

func RegisterPortfolioBuilder(name string, builder PortfolioBuilder) error {
	if name == "" || name == "top-bottom-k-v1" || builder == nil {
		return errors.New("runner: custom portfolio builder needs unique versioned name")
	}
	definitions.Lock()
	defer definitions.Unlock()
	if definitions.portfolios == nil {
		definitions.portfolios = map[string]PortfolioBuilder{}
	}
	if _, exists := definitions.portfolios[name]; exists {
		return errors.New("runner: portfolio builder already registered")
	}
	definitions.portfolios[name] = builder
	return nil
}

// RegisterPortfolioBuilderIdentity binds immutable native builder parameters
// to strategy identity. Existing stateless builders retain their old contract.
func RegisterPortfolioBuilderIdentity(name, hash string) error {
	if name == "" || hash == "" {
		return errors.New("runner: builder identity requires name and content hash")
	}
	definitions.Lock()
	defer definitions.Unlock()
	if definitions.portfolios[name] == nil {
		return errors.New("runner: builder must be registered before its identity")
	}
	if definitions.portfolioIdentity == nil {
		definitions.portfolioIdentity = map[string]string{}
	}
	if prior := definitions.portfolioIdentity[name]; prior != "" && prior != hash {
		return errors.New("runner: builder identity already bound")
	}
	definitions.portfolioIdentity[name] = hash
	return nil
}

func portfolioBuilderIdentity(name string) string {
	definitions.RLock()
	defer definitions.RUnlock()
	return definitions.portfolioIdentity[name]
}
func portfolioBuilder(name string) (PortfolioBuilder, bool) {
	definitions.RLock()
	defer definitions.RUnlock()
	builder, ok := definitions.portfolios[name]
	return builder, ok
}

func RegisterDefinition(name string, builder DefinitionBuilder) error {
	if name == "" || builder == nil {
		return errors.New("runner: definition needs name and builder")
	}
	definitions.Lock()
	defer definitions.Unlock()
	if _, exists := definitions.builders[name]; exists {
		return errors.New("runner: definition already registered")
	}
	definitions.builders[name] = builder
	return nil
}
func definitionBuilder(name string) (DefinitionBuilder, bool) {
	if name == "" {
		name = "momentum-vol"
	}
	definitions.RLock()
	defer definitions.RUnlock()
	builder, ok := definitions.builders[name]
	return builder, ok
}

// CompileDefinition exposes the same definition resolution used by both drivers.
func CompileDefinition(c Config) (*factor.Plan, research.ComboSpec, error) { return compileDecision(c) }

// DefinitionCatalog returns names from this executable's actual registrations.
// Copies keep clients from mutating the registry and sorting stabilizes the UI.
func DefinitionCatalog() (builders, portfolios []string) {
	definitions.RLock()
	defer definitions.RUnlock()
	for name := range definitions.builders {
		builders = append(builders, name)
	}
	portfolios = append(portfolios, "top-bottom-k-v1")
	for name := range definitions.portfolios {
		portfolios = append(portfolios, name)
	}
	slices.Sort(builders)
	slices.Sort(portfolios)
	return builders, portfolios
}
