package data

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/utils"
)

func NormalizeDataSub(info *orm.SeriesInfo, sub *strat.DataSub) (*strat.DataSub, error) {
	if info == nil {
		return nil, fmt.Errorf("series info is required")
	}
	if sub == nil {
		return nil, fmt.Errorf("data sub is required")
	}
	if sub.ExSymbol == nil {
		return nil, fmt.Errorf("exsymbol is required")
	}
	if strings.TrimSpace(sub.ExSymbol.Symbol) == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if sub.Source != "" && sub.Source != info.Name {
		return nil, fmt.Errorf("unsupported source %q", sub.Source)
	}
	if sub.TimeFrame != "" && sub.TimeFrame != info.TimeFrame {
		return nil, fmt.Errorf("unsupported timeframe %q", sub.TimeFrame)
	}
	cp := *sub
	cp.Source = info.Name
	cp.TimeFrame = info.TimeFrame
	if err := normalizeDataSubFields(info, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

func normalizeDataSubFields(info *orm.SeriesInfo, sub *strat.DataSub) error {
	if info == nil || sub == nil {
		return nil
	}
	available := make(map[string]bool, len(info.Binding.Fields))
	defaults := make([]string, 0, len(info.Binding.Fields))
	defaultSeries := make([]string, 0, len(info.Binding.Fields))
	for _, field := range info.Binding.Fields {
		available[field.Name] = true
		defaults = append(defaults, field.Name)
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(field.Type)), "float") {
			defaultSeries = append(defaultSeries, field.Name)
		}
	}
	fields := orm.NormalizeSeriesFields(info.Name, sub.Fields)
	if len(fields) == 0 {
		fields = defaults
	}
	for _, field := range fields {
		if !available[field] {
			return fmt.Errorf("unsupported field %q", field)
		}
	}
	sub.Fields = fields
	seriesFields := orm.MergeSeriesFields(sub.SeriesFields)
	if len(sub.SeriesFields) == 0 {
		seriesFields = defaultSeries
	}
	for _, field := range seriesFields {
		if !available[field] {
			return fmt.Errorf("unsupported series field %q", field)
		}
	}
	sub.SeriesFields = seriesFields
	sub.Fields = orm.MergeSeriesFields(sub.Fields, seriesFields)
	return nil
}

func ParseJSONFloat(raw []byte, field string) (float64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("missing field %s", field)
	}
	var str string
	if err := utils.Unmarshal(raw, &str, utils.JsonNumDefault); err == nil {
		if strings.TrimSpace(str) == "" {
			return 0, fmt.Errorf("missing field %s", field)
		}
		val, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return 0, fmt.Errorf("field %s is not numeric: %q", field, str)
		}
		return val, nil
	}
	var num float64
	if err := utils.Unmarshal(raw, &num, utils.JsonNumDefault); err == nil {
		return num, nil
	}
	return 0, fmt.Errorf("field %s is not numeric", field)
}

func ParseJSONInt(raw []byte, field string) (int64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("missing field %s", field)
	}
	var num int64
	if err := utils.Unmarshal(raw, &num, utils.JsonNumDefault); err == nil {
		return num, nil
	}
	var str string
	if err := utils.Unmarshal(raw, &str, utils.JsonNumDefault); err == nil {
		if strings.TrimSpace(str) == "" {
			return 0, fmt.Errorf("missing field %s", field)
		}
		val, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("field %s is not an integer: %q", field, str)
		}
		return val, nil
	}
	return 0, fmt.Errorf("field %s is not an integer", field)
}
