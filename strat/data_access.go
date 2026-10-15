package strat

import "github.com/banbox/banbot/orm"

// NewDataSub derives a subscription identity from an independent series.
// A KLineSeriesInfo can have an extension label as its Name; for those columns,
// declare DataSub{Source: "kline", TimeFrame: info.TimeFrame} explicitly instead.
func NewDataSub(info *orm.SeriesInfo) *DataSub {
	if info == nil {
		return nil
	}
	return &DataSub{Source: orm.NormalizeSeriesSource(info.Name), TimeFrame: info.TimeFrame}
}

// Data returns the DataHub view identified by sub. A nil ExSymbol uses the
// current job's already-bound symbol. When the job has a runtime catalog,
// explicit symbols are checked against it, without consulting a global catalog.
func (s *StratJob) Data(sub *DataSub) *DataFields {
	if s == nil || sub == nil {
		return nil
	}
	exs := sub.ExSymbol
	if exs == nil {
		if s.Symbol == nil {
			return nil
		}
		return s.DataHub.Get(sub.TimeFrame, sub.Source, s.Symbol.ID)
	}
	if s.symbols != nil {
		canonical := s.symbols.GetSymbolByID(exs.ID)
		if canonical == nil || (canonical != exs && (canonical.Exchange != exs.Exchange || canonical.Market != exs.Market || canonical.Symbol != exs.Symbol)) {
			return nil
		}
		exs = canonical
	}
	return s.DataHub.Get(sub.TimeFrame, sub.Source, exs.ID)
}
