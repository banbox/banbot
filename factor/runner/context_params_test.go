package runner

import (
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

func TestFactorContextHelpersAndSourceFields(t *testing.T) {
	ctx := FactorContext{TimeFrame: "1h", Params: &Params{}}
	plan, err := factor.Compile(map[string]*factor.Node{
		"ohlcv":   factor.Add(factor.Add(ctx.Open(), ctx.High()), factor.Add(ctx.Low(), ctx.Close())),
		"flow":    factor.Add(factor.Add(ctx.Volume(), ctx.Quote()), ctx.BuyVolume()),
		"funding": ctx.SourceField("funding_rate", "rate"),
		"oi":      ctx.SourceField("open_interest", "open_interest"), // alias -> "oi"
		"basis":   ctx.SourceField("premium_kline", "close"),
		"btcFund": ctx.RefSourceField("BTC/USDT:USDT", "funding_rate", "rate"),
	})
	if err != nil {
		t.Fatalf("compile FactorContext plan on 1h: %v", err)
	}
	inputs := plan.Inputs()
	var sawFunding8h, sawOI1h, sawPremium1h bool
	for _, in := range inputs {
		switch in.Source {
		case "funding_rate":
			if in.TimeFrame == "8h" && in.AsOfLatest {
				sawFunding8h = true
			}
		case "open_interest":
			if in.TimeFrame == "1h" && len(in.Fields) == 1 && in.Fields[0] == "oi" {
				sawOI1h = true
			}
		case "premium_kline":
			if in.TimeFrame == "1h" {
				sawPremium1h = true
			}
		}
	}
	if !sawFunding8h || !sawOI1h || !sawPremium1h {
		t.Fatalf("unexpected compiled inputs: %+v", inputs)
	}

	ctx3d := FactorContext{TimeFrame: "3d", Params: &Params{}}
	if _, err := factor.Compile(map[string]*factor.Node{
		"oi3d": ctx3d.SourceField("open_interest", "oi_value"),
	}); err != nil {
		t.Fatalf("compile open_interest on 3d via AsOf 1d: %v", err)
	}
}

func TestParameterizedBasketsAndResidualRegistrations(t *testing.T) {
	bReg, err := BuildBasketsParamsRegistration("test-baskets-params", func(p *Params) (BasketsSpec, error) {
		return BasketsSpec{
			Baskets: []BasketDef{{Name: "b1", Symbols: []string{"BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"}}},
			Window:  p.Int("window", 60),
			Band: statistics.BandConfig{
				Entry: p.Float("entry", 2.0),
				Exit:  p.Float("exit", 0.5),
			},
		}, p.Err()
	})
	if err != nil {
		t.Fatalf("BuildBasketsParamsRegistration: %v", err)
	}
	badCfg := Config{Manifest: research.ManifestSpec{Parameters: map[string]float64{"window": 2}}}
	if err := bReg.Validator(badCfg); err == nil {
		t.Fatalf("expected baskets validator to reject window=2")
	}

	rReg, err := BuildResidualStatArbParamsRegistration("test-residual-params", func(p *Params) (ResidualStatArbSpec, error) {
		return ResidualStatArbSpec{
			FactorModel: "pca",
			NumFactors:  p.Int("factors", 2),
			Window:      p.Int("window", 30),
			EntryS:      p.Float("entry", 1.25),
		}, p.Err()
	})
	if err != nil {
		t.Fatalf("BuildResidualStatArbParamsRegistration: %v", err)
	}
	badResCfg := Config{Manifest: research.ManifestSpec{Parameters: map[string]float64{"window": 3}}}
	if err := rReg.Validator(badResCfg); err == nil {
		t.Fatalf("expected residual statarb validator to reject window=3")
	}
}
