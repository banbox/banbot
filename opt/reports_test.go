package opt

import (
	"testing"

	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/utils"
)

func TestGroupByProfitsHandlesEmptyKMeansClusters(t *testing.T) {
	const orderCount = 10
	orders := make([]*ormo.InOutOrder, orderCount)
	rates := make([]float64, orderCount)
	for i := range orders {
		orders[i] = &ormo.InOutOrder{
			IOrder: &ormo.IOrder{
				ProfitRate: 0.01,
				Profit:     1,
				Leverage:   1,
			},
			Enter: &ormo.ExOrder{Average: 1, Filled: 1},
		}
		rates[i] = orders[i].ProfitRate
	}

	clusters := utils.KMeansVals(rates, 4)
	hasEmptyCluster := false
	for _, cluster := range clusters.Clusters {
		hasEmptyCluster = hasEmptyCluster || len(cluster.Items) == 0
	}
	if !hasEmptyCluster {
		t.Fatal("expected duplicate profit rates to produce an empty KMeans cluster")
	}

	result := &BTResult{}
	result.groupByProfits(orders)
	if len(result.ProfitGrps) != 1 {
		t.Fatalf("profit groups = %d, want 1", len(result.ProfitGrps))
	}
}
