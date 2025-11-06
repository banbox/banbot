package goods

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banexg/errs"
	"github.com/go-viper/mapstructure/v2"
)

type IFilter interface {
	GetName() string
	IsDisable() bool
	Filter(pairs []string, timeMS int64) ([]string, *errs.Error)
}

type IProducer interface {
	IFilter
	GenSymbols(timeMS int64) ([]string, *errs.Error)
}

type BaseFilter struct {
	Name       string `yaml:"name" mapstructure:"name"`
	Disable    bool   `yaml:"disable" mapstructure:"disable,omitempty"`
	AllowEmpty bool
}

// VolumePairFilter Used to represent a configuration that sorts all trading pairs in reverse order by volume value 用于表示按成交量价值倒序排序所有交易对的配置
type VolumePairFilter struct {
	BaseFilter
	Limit      int     `yaml:"limit" mapstructure:"limit,omitempty"` // The number of returned results is limited to the first 100 返回结果的数量限制，取前100个
	LimitRate  float64 `yaml:"limit_rate" mapstructure:"limit_rate"`
	MinValue   float64 `yaml:"min_value" mapstructure:"min_value,omitempty"`     // Minimum volume value 最低成交量价值
	CacheSecs  int     `yaml:"cache_secs" mapstructure:"cache_secs,omitempty"`   // Cache time, in seconds 缓存时间，以秒为单位
	BackPeriod string  `yaml:"back_period" mapstructure:"back_period,omitempty"` // The time period for calculating the volume 计算成交量的时间周期
}

/*
PriceFilter The price filter configuration 价格过滤器配置结构体
Precision: 0.001，Filter trading pairs by price precision, and the minimum unit of price change is 0.1% by default 按价格精度过滤交易对，默认要求价格变动最小单位是0.1%
Min: Lowest price 最低价格
Max: Highest price 最高价格
MaxUnitValue: The value of the maximum allowable unit price change (for the pricing currency, it is generally USDT). 最大允许的单位价格变动对应的价值(针对定价货币，一般是USDT)。
*/
type PriceFilter struct {
	BaseFilter
	MaxUnitValue float64 `yaml:"max_unit_value" mapstructure:"max_unit_value,omitempty"`
	Precision    float64 `yaml:"precision" mapstructure:"precision,omitempty"`
	Min          float64 `yaml:"min" mapstructure:"min,omitempty"`
	Max          float64 `yaml:"max" mapstructure:"max,omitempty"`
}

// RateOfChangeFilter 一段时间内(high-low)/low比值
type RateOfChangeFilter struct {
	BaseFilter
	BackDays  int     `yaml:"back_days" mapstructure:"back_days,omitempty"`   // 回顾的K线天数
	Min       float64 `yaml:"min" mapstructure:"min,omitempty"`               // 最小价格变动比率
	Max       float64 `yaml:"max" mapstructure:"max,omitempty"`               // 最大价格变动比率
	CacheSecs int     `yaml:"cache_secs" mapstructure:"cache_secs,omitempty"` // 缓存时间，秒
}

// 流动性过滤器。
type SpreadFilter struct {
	BaseFilter
	MaxRatio float32 `yaml:"max_ratio" mapstructure:"max_ratio,omitempty"` // 公式：1-bid/ask，买卖价差占价格的最大比率
}

type CorrelationFilter struct {
	BaseFilter
	Min       float64 `yaml:"min" mapstructure:"min,omitempty"`
	Max       float64 `yaml:"max" mapstructure:"max,omitempty"`
	Timeframe string  `yaml:"timeframe" mapstructure:"timeframe,omitempty"`
	BackNum   int     `yaml:"back_num" mapstructure:"back_num,omitempty"`
	TopN      int     `yaml:"top_n" mapstructure:"top_n"`
	TopRate   float64 `yaml:"top_rate" mapstructure:"top_rate"`
	Sort      string  `yaml:"sort" mapstructure:"sort"`
}

// VolatilityFilter StdDev(ln(close / prev_close)) * sqrt(num)
type VolatilityFilter struct {
	BaseFilter
	BackDays int     `yaml:"back_days" mapstructure:"back_days,omitempty"` // 回顾的K线天数
	Max      float64 `yaml:"max" mapstructure:"max,omitempty"`             // 波动分数最大值
	Min      float64 `yaml:"min" mapstructure:"min,omitempty"`             // 波动分数最小值
}

type BlockFilter struct {
	BaseFilter
	Pairs   []string `yaml:"pairs" mapstructure:"pairs,omitempty"`
	pairMap map[string]bool
}

type AgeFilter struct {
	BaseFilter
	Min int `yaml:"min" mapstructure:"min,omitempty"` // 最小上市天数
	Max int `yaml:"max" mapstructure:"max,omitempty"` // 最大上市天数
}

type OffsetFilter struct {
	BaseFilter
	Reverse bool    `yaml:"reverse" mapstructure:"reverse,omitempty"`
	Offset  int     `yaml:"offset" mapstructure:"offset,omitempty"`
	Limit   int     `yaml:"limit" mapstructure:"limit,omitempty"`
	Rate    float64 `yaml:"rate" mapstructure:"rate,omitempty"`
}

type ShuffleFilter struct {
	BaseFilter
	Seed int `yaml:"seed" mapstructure:"seed,omitempty"`
}

// FilterFactory is a function that creates a new filter instance
type FilterFactory func(base BaseFilter) IFilter

// FilterRegistry holds all registered filter factories
var filterRegistry = make(map[string]FilterFactory)

// RegisterFilter registers a custom filter factory with the given name
// This allows users to create and register their own filters
// Example:
//
//	type MyCustomFilter struct {
//	    goods.BaseFilter
//	    MyParam string `yaml:"my_param" mapstructure:"my_param"`
//	}
//
//	func (f *MyCustomFilter) Filter(pairs []string, timeMS int64) ([]string, *errs.Error) {
//	    // Your custom filter logic here
//	    return pairs, nil
//	}
//
//	func init() {
//	    goods.RegisterFilter("MyCustomFilter", func(base goods.BaseFilter) goods.IFilter {
//	        return &MyCustomFilter{BaseFilter: base}
//	    })
//	}
func RegisterFilter(name string, factory FilterFactory) {
	filterRegistry[name] = factory
}

// GetFilterFactory returns the factory function for a given filter name
func GetFilterFactory(name string) (FilterFactory, bool) {
	factory, ok := filterRegistry[name]
	return factory, ok
}

// CreateFilter creates a filter instance from config using the registry
func CreateFilter(cfg *config.CommonPairFilter, allowEmpty bool) (IFilter, *errs.Error) {
	base := BaseFilter{Name: cfg.Name, AllowEmpty: allowEmpty}

	// Try to get from registry first
	if factory, ok := filterRegistry[cfg.Name]; ok {
		filter := factory(base)
		err := mapstructure.Decode(cfg.Items, &filter)
		if err != nil {
			return nil, errs.New(errs.CodeUnmarshalFail, err)
		}
		return filter, nil
	}

	return nil, errs.NewMsg(errs.CodeParamInvalid, "unknown symbol filter: %s", cfg.Name)
}

// init registers all built-in filters
func init() {
	// Register all built-in filters
	RegisterFilter("AgeFilter", func(base BaseFilter) IFilter {
		return &AgeFilter{BaseFilter: base}
	})
	RegisterFilter("VolumePairList", func(base BaseFilter) IFilter {
		return &VolumePairFilter{BaseFilter: base}
	})
	RegisterFilter("PriceFilter", func(base BaseFilter) IFilter {
		return &PriceFilter{BaseFilter: base}
	})
	RegisterFilter("RateOfChangeFilter", func(base BaseFilter) IFilter {
		return &RateOfChangeFilter{BaseFilter: base}
	})
	RegisterFilter("VolatilityFilter", func(base BaseFilter) IFilter {
		return &VolatilityFilter{BaseFilter: base}
	})
	RegisterFilter("SpreadFilter", func(base BaseFilter) IFilter {
		return &SpreadFilter{BaseFilter: base}
	})
	RegisterFilter("OffsetFilter", func(base BaseFilter) IFilter {
		return &OffsetFilter{BaseFilter: base}
	})
	RegisterFilter("ShuffleFilter", func(base BaseFilter) IFilter {
		return &ShuffleFilter{BaseFilter: base}
	})
	RegisterFilter("CorrelationFilter", func(base BaseFilter) IFilter {
		return &CorrelationFilter{BaseFilter: base}
	})
	RegisterFilter("BlockFilter", func(base BaseFilter) IFilter {
		return &BlockFilter{BaseFilter: base}
	})
}
