package runner

import (
	"errors"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"slices"
	"strings"
	"sync"
)

// DefinitionBuilder compiles registered Go definitions; parameters remain in
// Config/Manifest rather than a second expression language.
type DefinitionBuilder func(Config) (*factor.Plan, research.ComboSpec, error)

// PortfolioBuilder accepts frozen inference and budget evidence, never labels.
type PortfolioBuilder func(factor.Frame, factor.Universe, factor.PortfolioSpec, research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error)

// PortfolioFactory creates private strategy state for one replay/live run.
// The factory's versioned name and Manifest.Parameters identify its behavior.
// Use a PortfolioPolicy for state that must be committed with execution fills.
type PortfolioFactory func(Config) (PortfolioBuilder, error)

// PortfolioValidator checks parameters without constructing state or resources.
// It receives an owned Config copy.
type PortfolioValidator func(Config) error

type portfolioFactoryRegistration struct {
	create   PortfolioFactory
	validate PortfolioValidator
}

var definitions = struct {
	sync.RWMutex
	builders          map[string]DefinitionBuilder
	portfolios        map[string]PortfolioBuilder
	portfolioIdentity map[string]string
	factories         map[string]portfolioFactoryRegistration
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
	if _, exists := definitions.portfolios[name]; exists || definitions.factories[name].create != nil {
		return errors.New("runner: portfolio builder already registered")
	}
	definitions.portfolios[name] = builder
	return nil
}

// RegisterPortfolioFactory optionally accepts one pure parameter validator.
// Without it, preflight checks identity and registration; constructor errors
// are checked by PreparePortfolioConfig before execution resource assembly.
func RegisterPortfolioFactory(name string, factory PortfolioFactory, validators ...PortfolioValidator) error {
	if len(validators) > 1 || len(validators) == 1 && validators[0] == nil {
		return errors.New("runner: portfolio factory accepts at most one non-nil validator")
	}
	if name == "" || !strings.Contains(name, "-v") || factory == nil || name == "top-bottom-k-v1" {
		return errors.New("runner: portfolio factory needs unique versioned name")
	}
	definitions.Lock()
	defer definitions.Unlock()
	if definitions.portfolios[name] != nil || definitions.factories[name].create != nil {
		return errors.New("runner: portfolio factory already registered")
	}
	if definitions.factories == nil {
		definitions.factories = map[string]portfolioFactoryRegistration{}
	}
	registration := portfolioFactoryRegistration{create: factory}
	if len(validators) == 1 {
		registration.validate = validators[0]
	}
	definitions.factories[name] = registration
	return nil
}

func newPortfolioBuilder(name string, config Config) (PortfolioBuilder, bool, error) {
	definitions.RLock()
	factory, builder := definitions.factories[name], definitions.portfolios[name]
	definitions.RUnlock()
	if factory.create != nil {
		owned, err := CloneConfig(config)
		if err != nil {
			return nil, true, err
		}
		builder, err = factory.create(owned)
		if err == nil && builder == nil {
			err = errors.New("runner: portfolio factory returned nil builder")
		}
		return builder, true, err
	}
	return builder, builder != nil, nil
}

func validatePortfolioBuilder(name string, config Config) (PortfolioBuilder, bool, error) {
	definitions.RLock()
	factory, builder := definitions.factories[name], definitions.portfolios[name]
	definitions.RUnlock()
	if factory.create == nil {
		return builder, builder != nil, nil
	}
	if factory.validate != nil {
		owned, err := CloneConfig(config)
		if err != nil {
			return nil, true, err
		}
		if err := factory.validate(owned); err != nil {
			return nil, true, err
		}
	}
	return nil, true, nil
}

// PreparePortfolioConfig constructs private portfolio state once for one run.
// Call after preflight and before borrowing accounts or opening execution
// stores, and pass the returned Config through that run only.
func PreparePortfolioConfig(c Config) (Config, error) {
	c, err := resolveDecisionPortfolio(c)
	if err != nil {
		return c, err
	}
	name := c.Manifest.Portfolio.Builder
	if c.PortfolioBuilder == nil && name != "" && name != "top-bottom-k-v1" {
		c.PortfolioBuilder, _, err = newPortfolioBuilder(name, c)
	}
	return c, err
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
	for name := range definitions.factories {
		portfolios = append(portfolios, name)
	}
	slices.Sort(builders)
	slices.Sort(portfolios)
	return builders, portfolios
}
