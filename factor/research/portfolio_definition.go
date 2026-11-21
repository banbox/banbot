package research

import (
	"encoding/json"
	"errors"
	"math"

	"github.com/banbox/banbot/factor"
)

// ClonePortfolioDefinition preserves legacy JSON identity when policy is absent.
func ClonePortfolioDefinition(d PortfolioDefinition) PortfolioDefinition {
	d.PolicyParams = append(json.RawMessage(nil), d.PolicyParams...)
	if d.LongMinScore != nil {
		v := *d.LongMinScore
		d.LongMinScore = &v
	}
	if d.ShortMaxScore != nil {
		v := *d.ShortMaxScore
		d.ShortMaxScore = &v
	}
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

// EffectivePolicy returns the explicit policy name, or promotes to "lifecycle-v1"
// when lifecycle/allocation sub-blocks are configured without custom policy_params.
func (d PortfolioDefinition) EffectivePolicy() string {
	if d.Policy != "" {
		return d.Policy
	}
	if d.PolicyParams == nil && (d.Rebalance != nil || d.Selection != nil || d.Holding != nil || d.Transition != nil || d.Allocation != nil) {
		return "lifecycle-v1"
	}
	return ""
}

func (d PortfolioDefinition) PolicyConfig() (factor.PortfolioPolicyConfig, error) {
	c := factor.PortfolioPolicyConfig{Policy: d.EffectivePolicy(), PolicyParams: d.PolicyParams, LongNotional: d.LongNotional, ShortNotional: d.ShortNotional}
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
	if c.Selection.LongMinScore == nil && d.LongMinScore != nil {
		v := *d.LongMinScore
		c.Selection.LongMinScore = &v
	}
	if c.Selection.ShortMaxScore == nil && d.ShortMaxScore != nil {
		v := *d.ShortMaxScore
		c.Selection.ShortMaxScore = &v
	}
	if c.Selection.UnfilledSlotPolicy == "" && d.UnfilledSlotPolicy != "" {
		c.Selection.UnfilledSlotPolicy = d.UnfilledSlotPolicy
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
	if c.Allocation.AssetCap == 0 && d.AssetCap > 0 {
		c.Allocation.AssetCap = d.AssetCap
	}
	return factor.NormalizePortfolioPolicyConfig(c)
}

func (d PortfolioDefinition) ValidatePolicy() error {
	if d.LongMinScore != nil && (math.IsNaN(*d.LongMinScore) || math.IsInf(*d.LongMinScore, 0)) {
		return errors.New("research: non-finite portfolio long_min_score")
	}
	if d.ShortMaxScore != nil && (math.IsNaN(*d.ShortMaxScore) || math.IsInf(*d.ShortMaxScore, 0)) {
		return errors.New("research: non-finite portfolio short_max_score")
	}
	if d.UnfilledSlotPolicy != "" && d.UnfilledSlotPolicy != "cash" && d.UnfilledSlotPolicy != "rescale" {
		return errors.New("research: unknown portfolio unfilled_slot_policy")
	}
	if d.AssetCap < 0 || math.IsNaN(d.AssetCap) || math.IsInf(d.AssetCap, 0) {
		return errors.New("research: invalid portfolio asset_cap")
	}
	if d.EffectivePolicy() == "" {
		if d.PolicyParams != nil {
			return errors.New("research: portfolio policy_params requires an explicit policy")
		}
		return nil
	}
	_, err := d.PolicyConfig()
	return err
}

// ResolvePolicy makes defaults part of reproducible strategy identity only for
// strategies that opt into the policy contract (explicitly or via sub-blocks).
func (d PortfolioDefinition) ResolvePolicy() (PortfolioDefinition, error) {
	ep := d.EffectivePolicy()
	if ep == "" {
		return ClonePortfolioDefinition(d), d.ValidatePolicy()
	}
	c, err := d.PolicyConfig()
	if err != nil {
		return d, err
	}
	d = ClonePortfolioDefinition(d)
	d.Policy = ep
	if d.Policy == "lifecycle-v1" {
		d.Rebalance = &c.Rebalance
		d.Selection = &c.Selection
		d.Holding = &c.Holding
		d.Transition = &c.Transition
		d.Allocation = &c.Allocation
	}
	return d, nil
}
