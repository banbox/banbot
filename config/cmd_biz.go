package config

import (
	"github.com/banbox/banbot/utils"
)

func (this *CmdArgs) Init() {
	this.TimeFrames = utils.SplitSolid(this.RawTimeFrames, ",")
	this.Pairs = utils.SplitSolid(this.RawPairs, ",")
	this.Tables = utils.SplitSolid(this.RawTables, ",")
}