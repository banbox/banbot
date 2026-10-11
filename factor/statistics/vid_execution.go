package statistics

import (
	"errors"
	"math"
	"sort"
)

type PairedFill struct{ Price, Quantity float64 }
type FillPair struct {
	BidderIndex, TargetIndex int
	TargetQuantity           float64
}
type PairedExecution struct {
	Pairs                                        []FillPair
	MatchedTarget, BidderEquivalent, TargetTotal float64
	AverageSpread                                float64
	AverageSatisfies, FullyPaired                bool
}

// PairExecutions verifies Vidyamurthy Ch10's monotone bipartite fill graph.
// Put-on requires ratio*bidder+cash-target >= limit; unwind requires <= limit.
// Quantities are actual fills in asset units; bidder quantity is converted to
// target-equivalent units. The source lazy allocation sorts sales ascending
// and purchases descending, then saturates the first feasible remaining edge.
// This is a post-fill verifier; it does not promise simultaneous broker fills.
func PairExecutions(bidder, target []PairedFill, ratio, cash, limit float64, putOn bool) (PairedExecution, error) {
	if len(bidder) == 0 || len(target) == 0 || len(bidder) > 10000 || len(target) > 10000 || len(bidder)*len(target) > 1000000 || !finite(ratio) || ratio <= 0 || !finite(cash) || !finite(limit) {
		return PairedExecution{}, errors.New("statistics: invalid paired execution specification")
	}
	type node struct {
		price, remaining float64
		index            int
	}
	b := make([]node, len(bidder))
	t := make([]node, len(target))
	var out PairedExecution
	var bidderValue, targetValue float64
	for i, fill := range bidder {
		if !finite(fill.Price) || !finite(fill.Quantity) || fill.Price <= 0 || fill.Quantity <= 0 {
			return out, errors.New("statistics: invalid bidder fill")
		}
		b[i] = node{fill.Price * ratio, fill.Quantity / ratio, i}
		out.BidderEquivalent += b[i].remaining
		bidderValue += b[i].price * b[i].remaining
	}
	for i, fill := range target {
		if !finite(fill.Price) || !finite(fill.Quantity) || fill.Price <= 0 || fill.Quantity <= 0 {
			return out, errors.New("statistics: invalid target fill")
		}
		t[i] = node{fill.Price, fill.Quantity, i}
		out.TargetTotal += fill.Quantity
		targetValue += fill.Price * fill.Quantity
	}
	if !finite(out.BidderEquivalent) || out.BidderEquivalent <= 0 || !finite(out.TargetTotal) || !finite(bidderValue) || !finite(targetValue) {
		return PairedExecution{}, errors.New("statistics: paired execution overflow")
	}
	out.AverageSpread = bidderValue/out.BidderEquivalent + cash - targetValue/out.TargetTotal
	if !finite(out.AverageSpread) {
		return PairedExecution{}, errors.New("statistics: average spread overflow")
	}
	out.AverageSatisfies = out.AverageSpread >= limit
	sellers, buyers := b, t
	minimum := limit - cash
	if !putOn {
		sellers, buyers = t, b
		minimum = cash - limit
		out.AverageSatisfies = out.AverageSpread <= limit
	}
	if !finite(minimum) {
		return PairedExecution{}, errors.New("statistics: spread bound overflow")
	}
	sort.SliceStable(sellers, func(i, j int) bool { return sellers[i].price < sellers[j].price })
	sort.SliceStable(buyers, func(i, j int) bool { return buyers[i].price > buyers[j].price })
	for i := range sellers {
		for j := range buyers {
			if sellers[i].remaining == 0 {
				break
			}
			if buyers[j].remaining == 0 || sellers[i].price-buyers[j].price < minimum {
				continue
			}
			quantity := math.Min(sellers[i].remaining, buyers[j].remaining)
			pair := FillPair{sellers[i].index, buyers[j].index, quantity}
			if !putOn {
				pair.BidderIndex, pair.TargetIndex = buyers[j].index, sellers[i].index
			}
			out.Pairs = append(out.Pairs, pair)
			out.MatchedTarget += quantity
			sellers[i].remaining -= quantity
			buyers[j].remaining -= quantity
		}
	}
	tolerance := 1e-12 * math.Max(out.BidderEquivalent, out.TargetTotal)
	out.FullyPaired = math.Abs(out.BidderEquivalent-out.MatchedTarget) <= tolerance && math.Abs(out.TargetTotal-out.MatchedTarget) <= tolerance
	return out, nil
}
