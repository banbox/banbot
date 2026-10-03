package execution

import (
	"errors"
	"math/big"
	"sort"

	"github.com/shopspring/decimal"
)

// QuantitySteps shrinks toward zero to integral lot-step units. No float
// tolerance is used; callers convert venue floats only at the adapter boundary.
func QuantitySteps(quantity, step decimal.Decimal) (int64, error) {
	if !step.IsPositive() {
		return 0, errors.New("execution: quantity step must be positive")
	}
	units, _ := quantity.QuoRem(step, 0)
	n := units.BigInt()
	if !n.IsInt64() {
		return 0, errors.New("execution: quantity exceeds int64 step units")
	}
	return n.Int64(), nil
}

// AllocateSteps allocates positive fill units proportionally to frozen positive
// demands. Whole-unit remainders go in stable identity order, independent of map
// iteration. Allocation never exceeds a demand.
func AllocateSteps(total int64, demands map[string]int64) (map[string]int64, error) {
	if total < 0 {
		return nil, errors.New("execution: negative allocation")
	}
	keys := make([]string, 0, len(demands))
	sum := new(big.Int)
	for id, demand := range demands {
		if id == "" || demand <= 0 {
			return nil, errors.New("execution: allocation requires stable identities and positive demands")
		}
		keys = append(keys, id)
		sum.Add(sum, big.NewInt(demand))
	}
	if sum.Cmp(big.NewInt(total)) < 0 {
		return nil, errors.New("execution: fill exceeds allocated demand")
	}
	sort.Strings(keys)
	result := make(map[string]int64, len(keys))
	left := total
	for _, id := range keys {
		n := new(big.Int).Mul(big.NewInt(total), big.NewInt(demands[id]))
		n.Quo(n, sum)
		result[id] = n.Int64()
		left -= result[id]
	}
	for _, id := range keys {
		if left == 0 {
			break
		}
		if result[id] < demands[id] {
			result[id]++
			left--
		}
	}
	return result, nil
}
