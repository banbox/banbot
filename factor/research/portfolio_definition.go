package research

import (
	"encoding/json"
	"errors"
	"github.com/banbox/banbot/factor"
)

// ClonePortfolioDefinition preserves legacy JSON identity when policy is absent.
func ClonePortfolioDefinition(d PortfolioDefinition) PortfolioDefinition {
	d.PolicyParams = append(json.RawMessage(nil), d.PolicyParams...)
	if d.Rebalance != nil {
		value := *d.Rebalance
		d.Rebalance = &value
	}
	if d.Selection != nil {
		value := factor.ClonePortfolioPolicyConfig(factor.PortfolioPolicyConfig{Selection: *d.Selection}).Selection
		d.Selection = &value
	}
	if d.Holding != nil {
		value := factor.ClonePortfolioPolicyConfig(factor.PortfolioPolicyConfig{Holding: *d.Holding}).Holding
		d.Holding = &value
	}
	if d.Transition != nil {
		value := factor.ClonePortfolioPolicyConfig(factor.PortfolioPolicyConfig{Transition: *d.Transition}).Transition
		d.Transition = &value
	}
	if d.Allocation != nil {
		value := factor.ClonePortfolioPolicyConfig(factor.PortfolioPolicyConfig{Allocation: *d.Allocation}).Allocation
		d.Allocation = &value
	}
	return d
}

func (d PortfolioDefinition) PolicyConfig() (factor.PortfolioPolicyConfig, error) {
	c := factor.PortfolioPolicyConfig{Policy: d.Policy, PolicyParams: d.PolicyParams, LongNotional: d.LongNotional, ShortNotional: d.ShortNotional}
	if d.Rebalance != nil {
		c.Rebalance = *d.Rebalance
	}
	if d.Selection != nil {
		c.Selection = *d.Selection
	} else {
		if d.LongNotional > 0 {
			c.Selection.LongK = d.K
		}
		if d.ShortNotional > 0 {
			c.Selection.ShortK = d.K
		}
	}
	if d.Holding != nil {
		c.Holding = *d.Holding
	}
	if d.Transition != nil {
		c.Transition = *d.Transition
	}
	if d.Allocation != nil {
		c.Allocation = *d.Allocation
	}
	return factor.NormalizePortfolioPolicyConfig(c)
}

func (d PortfolioDefinition) ValidatePolicy() error {
	if d.Policy == "" {
		if d.PolicyParams != nil || d.Rebalance != nil || d.Selection != nil || d.Holding != nil || d.Transition != nil || d.Allocation != nil {
			return errors.New("research: portfolio lifecycle fields require an explicit policy")
		}
		return nil
	}
	_, err := d.PolicyConfig()
	return err
}

// ResolvePolicy makes defaults part of reproducible strategy identity only for
// strategies that explicitly opt into the new policy contract.
func (d PortfolioDefinition) ResolvePolicy() (PortfolioDefinition, error) {
	if d.Policy == "" {
		return ClonePortfolioDefinition(d), d.ValidatePolicy()
	}
	c, err := d.PolicyConfig()
	if err != nil {
		return d, err
	}
	d = ClonePortfolioDefinition(d)
	if d.Policy == "lifecycle-v1" {
		d.Rebalance = &c.Rebalance
		d.Selection = &c.Selection
		d.Holding = &c.Holding
		d.Transition = &c.Transition
		d.Allocation = &c.Allocation
	}
	return d, nil
}
