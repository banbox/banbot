package opt

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg/errs"
	"github.com/spf13/cobra"
)

func TestPublicToolCompatibilityFacadesRemainAvailable(t *testing.T) {
	var factors func([]string) error = BtFactors
	var factorCommand func() *cobra.Command = NewBtFactorsCommand
	var compare func([]string) error = CompareExgBTOrders
	var compareCommand func() *cobra.Command = NewCompareExgBTOrdersCommand
	var build func(*config.CmdArgs) *errs.Error = BuildBtResult
	var cut func([]*ormo.InOutOrder, int64, int64) (map[string][]*ormo.InOutOrder, *errs.Error) = CutOrdersInRange
	if factors == nil || factorCommand() == nil || compare == nil || compareCommand() == nil || build == nil || cut == nil {
		t.Fatal("public tool compatibility facade is missing")
	}
}
