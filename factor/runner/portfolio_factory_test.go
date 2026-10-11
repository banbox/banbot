package runner

import (
	"errors"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
)

func TestPortfolioFactoryOwnsEachRunsStateAndConfig(t *testing.T) {
	name := "test-private-factory-v1"
	err := RegisterPortfolioFactory(name, func(c Config) (PortfolioBuilder, error) {
		count := 0
		c.Manifest.Parameters["window"] = 100
		return func(_ factor.Frame, _ factor.Universe, _ factor.PortfolioSpec, _ research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
			count++
			return nil, []factor.Diagnostic{{Code: "count", Detail: string(rune('0' + count))}}, nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	c := Config{Manifest: research.ManifestSpec{Parameters: map[string]float64{"window": 3}}}
	a, ok, err := newPortfolioBuilder(name, c)
	if !ok || err != nil {
		t.Fatal(err)
	}
	b, ok, err := newPortfolioBuilder(name, c)
	if !ok || err != nil {
		t.Fatal(err)
	}
	call := func(builder PortfolioBuilder) string {
		_, d, err := builder(factor.Frame{}, factor.Universe{}, factor.PortfolioSpec{}, research.PortfolioDefinition{})
		if err != nil {
			t.Fatal(err)
		}
		return d[0].Detail
	}
	if call(a) != "1" || call(a) != "2" || call(b) != "1" || c.Manifest.Parameters["window"] != 3 {
		t.Fatal("run state or caller config shared")
	}
	if err = RegisterPortfolioBuilder(name, a); err == nil {
		t.Fatal("factory collision accepted")
	}
	if err = RegisterPortfolioFactory(name, nil); err == nil {
		t.Fatal("nil factory accepted")
	}
}

func TestPortfolioFactoryPropagatesConstructionFailure(t *testing.T) {
	want := errors.New("invalid training window")
	if err := RegisterPortfolioFactory("test-factory-error-v1", func(Config) (PortfolioBuilder, error) { return nil, want }); err != nil {
		t.Fatal(err)
	}
	_, ok, err := newPortfolioBuilder("test-factory-error-v1", Config{})
	if !ok || !errors.Is(err, want) {
		t.Fatal("lost factory error")
	}
	if err := RegisterPortfolioFactory("test-factory-nil-v1", func(Config) (PortfolioBuilder, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, err = newPortfolioBuilder("test-factory-nil-v1", Config{}); err == nil {
		t.Fatal("nil builder accepted")
	}
}
