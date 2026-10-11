package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
)

func TestFactoryLifecyclePreflightAndReplayIsolation(t *testing.T) {
	c := archiveConfig(t, false)
	name := fmt.Sprintf("lifecycle-test-%d-v1", riskTestSequence.Add(1))
	c.Manifest.Portfolio.Builder = name
	constructed := 0
	var calls [][]int
	if err := RegisterPortfolioFactory(name, func(Config) (PortfolioBuilder, error) {
		index := constructed
		constructed++
		calls = append(calls, nil)
		count := 0
		return func(f factor.Frame, u factor.Universe, s factor.PortfolioSpec, p research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
			count++
			calls[index] = append(calls[index], count)
			return factor.TopBottomKNotional(f, "score", u, s, p.K, p.LongNotional, p.ShortNotional)
		}, nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, validate := range []func(Config) error{ValidateStaticConfig, func(c Config) error { return ValidateReplayConfig(c, false) }, ValidateLiveConfig} {
		if err := validate(c); err != nil {
			t.Fatal(err)
		}
	}
	if constructed != 0 {
		t.Fatalf("preflight constructed %d discarded instances", constructed)
	}
	for run := 1; run <= 2; run++ {
		if _, err := Run(context.Background(), c, nil, nil); err != nil {
			t.Fatal(err)
		}
		if constructed != run || c.PortfolioBuilder != nil {
			t.Fatalf("run %d constructed %d instances or mutated caller", run, constructed)
		}
	}
	if len(calls[0]) == 0 || !reflect.DeepEqual(calls[0], calls[1]) || calls[0][0] != 1 {
		t.Fatalf("run states not independent: %v", calls)
	}
}

func TestFactoryLifecycleValidatorRejectsBeforeConstruction(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Parameters = map[string]float64{"window": 3}
	name := fmt.Sprintf("validator-test-%d-v1", riskTestSequence.Add(1))
	c.Manifest.Portfolio.Builder = name
	want := errors.New("invalid training window")
	constructed := 0
	if err := RegisterPortfolioFactory(name, func(Config) (PortfolioBuilder, error) {
		constructed++
		return nil, nil
	}, func(owned Config) error {
		owned.Manifest.Parameters["window"] = 99
		return want
	}); err != nil {
		t.Fatal(err)
	}
	for _, validate := range []func(Config) error{ValidateStaticConfig, func(c Config) error { return ValidateReplayConfig(c, false) }, ValidateLiveConfig} {
		if err := validate(c); !errors.Is(err, want) {
			t.Fatalf("validator error lost: %v", err)
		}
	}
	if _, err := PreparePortfolioConfig(c); !errors.Is(err, want) || constructed != 0 || c.Manifest.Parameters["window"] != 3 {
		t.Fatalf("validator constructed state or changed caller: %v %d", err, constructed)
	}
}

func TestFactoryLifecycleLegacyErrorsPrecedeExecutionResources(t *testing.T) {
	for _, nilBuilder := range []bool{false, true} {
		t.Run(fmt.Sprint(nilBuilder), func(t *testing.T) {
			c := paperConfig(t, archiveConfig(t, false))
			c.Mode = Events
			name := fmt.Sprintf("assembly-error-test-%d-v1", riskTestSequence.Add(1))
			c.Manifest.Portfolio.Builder = name
			want := errors.New("constructor failed")
			if err := RegisterPortfolioFactory(name, func(Config) (PortfolioBuilder, error) {
				if nilBuilder {
					return nil, nil
				}
				return nil, want
			}); err != nil {
				t.Fatal(err)
			}
			if err := ValidateReplayConfig(c, true); err != nil {
				t.Fatalf("preflight invoked legacy constructor: %v", err)
			}
			if _, err := Run(context.Background(), c, nil, nil); err == nil || !nilBuilder && !errors.Is(err, want) {
				t.Fatalf("construction failure lost: %v", err)
			}
			for _, path := range []string{c.Execution.StorePath, c.Execution.SenderLeaseDir} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("constructor failure opened execution resource %s: %v", path, err)
				}
			}
		})
	}
}

func TestFactoryLifecyclePreparedLiveAndStatelessCompatibility(t *testing.T) {
	c := archiveConfig(t, false)
	name := fmt.Sprintf("prepared-live-test-%d-v1", riskTestSequence.Add(1))
	c.Manifest.Portfolio.Builder = name
	constructed := 0
	builder := func(f factor.Frame, u factor.Universe, s factor.PortfolioSpec, p research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		return factor.TopBottomKNotional(f, "score", u, s, p.K, p.LongNotional, p.ShortNotional)
	}
	if err := RegisterPortfolioFactory(name, func(Config) (PortfolioBuilder, error) { constructed++; return builder, nil }); err != nil {
		t.Fatal(err)
	}
	prepared, err := PreparePortfolioConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	live, err := NewLive(prepared, &liveSink{}, func() int64 { return 3600001 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	live.Stop()
	if err := live.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
	if constructed != 1 || c.PortfolioBuilder != nil {
		t.Fatalf("prepared state constructed again or caller mutated: %d", constructed)
	}
	stateless := fmt.Sprintf("stateless-test-%d", riskTestSequence.Add(1))
	if err := RegisterPortfolioBuilder(stateless, builder); err != nil {
		t.Fatal(err)
	}
	c.Manifest.Portfolio.Builder = stateless
	if resolved, err := resolveDecisionPortfolio(c); err != nil || resolved.PortfolioBuilder == nil {
		t.Fatalf("stateless registration contract changed: %v", err)
	}
}
