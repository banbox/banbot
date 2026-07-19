package strat

import (
	"fmt"
	"strings"

	"github.com/banbox/banbot/orm"
)

// DataRole identifies how an OnData event relates to the strategy job.
type DataRole uint8

const (
	// DataRoleMain is the job's primary K-line subscription.
	DataRoleMain DataRole = iota + 1
	// DataRoleInfo is an auxiliary K-line subscription.
	DataRoleInfo
	// DataRoleCustom is a non-K-line time-series subscription.
	DataRoleCustom
)

// DataEvent is the unified payload passed to TradeStrat.OnData.
// DataFields is embedded so handlers can read fields directly from the event.
type DataEvent struct {
	*DataFields
	Role   DataRole
	Symbol *orm.ExSymbol
}

// IsMain reports whether the event belongs to the job's primary subscription.
func (e DataEvent) IsMain() bool {
	return e.Role == DataRoleMain
}

// IsKline reports whether the event is a primary or auxiliary K-line.
func (e DataEvent) IsKline() bool {
	return e.Role == DataRoleMain || e.Role == DataRoleInfo
}

// FnOnData handles one unified strategy data event.
type FnOnData func(s *StratJob, data DataEvent)

// DataHandlers routes each data role to an optional dedicated handler.
type DataHandlers struct {
	Main   FnOnData
	Info   FnOnData
	Custom FnOnData
}

func validateDataCallbacks(stgy *TradeStrat) {
	if stgy == nil || stgy.OnData == nil {
		return
	}
	legacy := make([]string, 0, 2)
	if stgy.OnBar != nil {
		legacy = append(legacy, "OnBar")
	}
	if stgy.OnInfoBar != nil {
		legacy = append(legacy, "OnInfoBar")
	}
	if len(legacy) > 0 {
		panic(fmt.Sprintf("%s: OnData cannot be combined with %s; route main and info events inside OnData (for example with strat.RouteData)", stgy.Name, strings.Join(legacy, " and ")))
	}
}

// RouteData builds an OnData callback that invokes at most one role handler.
func RouteData(handlers DataHandlers) FnOnData {
	return func(s *StratJob, data DataEvent) {
		var handler FnOnData
		switch data.Role {
		case DataRoleMain:
			handler = handlers.Main
		case DataRoleInfo:
			handler = handlers.Info
		case DataRoleCustom:
			handler = handlers.Custom
		}
		if handler != nil {
			handler(s, data)
		}
	}
}
