// 此文件仅用作简单在线策略编写过程中可能用到的自动补全。用不到的不要维护在这里，危险方法也禁用。

import { m } from '$lib/paraglide/messages.js';

// 定义自动提示项的类型
type CompletionItemType = 'struct' | 'variable' | 'function' | 'method';

// 定义自动提示项
interface CompletionItem {
  label: string;        // 显示名称
  type: CompletionItemType;  // 类型（结构体、变量、函数或方法）
  detail?: string;      // 详细描述
  info?: string;        // 更多信息（如参数、返回值等）
}

// 为每个Go包定义自动提示数据
interface PackageCompletions {
  [packageName: string]: CompletionItem[];
}

const taCompletions: CompletionItem[] = [
  {
    label: "Series",
    type: "struct",
    detail: m.completion_series(),
  },
  {label: "Cross", type: "function", detail: "(se *Series, obj2 interface{}) int",
    info: m.completion_cross()
  },
  {label: "AvgPrice", type: "function", detail: "(e *BarEnv) *Series", info: m.ta_avg_price()},
  {label: "HL2", type: "function", detail: "(h, l *Series) *Series", info: m.ta_hl2()},
  {label: "HLC3", type: "function", detail: "(h, l, c *Series) *Series", info: m.ta_hlc3()},
  {label: "Sum", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_sum()},
  {label: "SMA", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_sma()},
  {label: "VWMA", type: "function", detail: "(price *Series, vol *Series, period int) *Series", info: m.ta_vwma()},
  {label: "EMA", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_ema()},
  {label: "EMABy", type: "function", detail: "(obj *Series, period int, initType int) *Series", info: m.ta_ema_by()},
  {label: "RMA", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_rma()},
  {label: "RMABy", type: "function", detail: "(obj *Series, period int, initType int, initVal float64) *Series", info: m.ta_rma_by()},
  {label: "WMA", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_wma()},
  {label: "HMA", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_hma()},
  {label: "TR", type: "function", detail: "(high *Series, low *Series, close *Series) *Series", info: m.ta_tr()},
  {label: "ATR", type: "function", detail: "(high *Series, low *Series, close *Series, period int) *Series", info: m.ta_atr()},
  {label: "MACD", type: "function", detail: "(obj *Series, fast int, slow int, smooth int) (*Series, *Series)", info: m.ta_macd()},
  {label: "MACDBy", type: "function", detail: "(obj *Series, fast int, slow int, smooth int, initType int) (*Series, *Series)", info: m.ta_macd_by()},
  {label: "RSI", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_rsi()},
  {label: "RSI50", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_rsi50()},
  {label: "CRSI", type: "function", detail: "(obj *Series, period, upDn, roc int) *Series", info: m.ta_crsi()},
  {label: "CRSIBy", type: "function", detail: "(obj *Series, period, upDn, roc, vtype int) *Series", info: m.ta_crsi_by()},
  {label: "UpDown", type: "function", detail: "(obj *Series, vtype int) *Series", info: m.ta_updown()},
  {label: "PercentRank", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_percent_rank()},
  {label: "Highest", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_highest()},
  {label: "HighestBar", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_highest_bar()},
  {label: "Lowest", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_lowest()},
  {label: "LowestBar", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_lowest_bar()},
  {label: "KDJ", type: "function", detail: "(high *Series, low *Series, close *Series, period int, sm1 int, sm2 int) (*Series, *Series, *Series)", info: m.ta_kdj()},
  {label: "KDJBy", type: "function", detail: "(high *Series, low *Series, close *Series, period int, sm1 int, sm2 int, maBy string) (*Series, *Series, *Series)", info: m.ta_kdj_by()},
  {label: "Stoch", type: "function", detail: "(high, low, close *Series, period int) *Series", info: m.ta_stoch()},
  {label: "Aroon", type: "function", detail: "(high *Series, low *Series, period int) (*Series, *Series, *Series)", info: m.ta_aroon()},
  {label: "StdDev", type: "function", detail: "(obj *Series, period int) (*Series, *Series)", info: m.ta_stddev()},
  {label: "StdDevBy", type: "function", detail: "(obj *Series, period int, ddof int) (*Series, *Series)", info: m.ta_stddev_by()},
  {label: "BBANDS", type: "function", detail: "(obj *Series, period int, stdUp, stdDn float64) (*Series, *Series, *Series)", info: m.ta_bbands()},
  {label: "TD", type: "function", detail: "(obj *Series) *Series", info: m.ta_td()},
  {label: "ADX", type: "function", detail: "(high *Series, low *Series, close *Series, period int) *Series", info: m.ta_adx()},
  {label: "ADXBy", type: "function", detail: "(high *Series, low *Series, close *Series, period int, method int) *Series", info: m.ta_adx_by()},
  {label: "PluMinDI", type: "function", detail: "(high *Series, low *Series, close *Series, period int) (*Series, *Series)", info: m.ta_plu_min_di()},
  {label: "PluMinDM", type: "function", detail: "(high *Series, low *Series, close *Series, period int) (*Series, *Series)", info: m.ta_plu_min_dm()},
  {label: "ROC", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_roc()},
  {label: "HeikinAshi", type: "function", detail: "(e *BarEnv) (*Series, *Series, *Series, *Series)", info: m.ta_heikin_ashi()},
  {label: "ER", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_er()},
  {label: "AvgDev", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_avg_dev()},
  {label: "CCI", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_cci()},
  {label: "CMF", type: "function", detail: "(env *BarEnv, period int) *Series", info: m.ta_cmf()},
  {label: "ADL", type: "function", detail: "(env *BarEnv) *Series", info: m.ta_adl()},
  {label: "ChaikinOsc", type: "function", detail: "(env *BarEnv, short int, long int) *Series", info: m.ta_chaikin_osc()},
  {label: "KAMA", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_kama()},
  {label: "KAMABy", type: "function", detail: "(obj *Series, period int, fast, slow int) *Series", info: m.ta_kama_by()},
  {label: "WillR", type: "function", detail: "(e *BarEnv, period int) *Series", info: m.ta_willr()},
  {label: "StochRSI", type: "function", detail: "(obj *Series, rsiLen int, stochLen int, maK int, maD int) (*Series, *Series)", info: m.ta_stoch_rsi()},
  {label: "MFI", type: "function", detail: "(e *BarEnv, period int) *Series", info: m.ta_mfi()},
  {label: "RMI", type: "function", detail: "(obj *Series, period int, montLen int) *Series", info: m.ta_rmi()},
  {label: "LinReg", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_lin_reg()},
  {label: "LinRegAdv", type: "function", detail: "(obj *Series, period int, angle, intercept, degrees, r, slope, tsf bool) *Series", info: m.ta_lin_reg_adv()},
  {label: "CTI", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_cti()},
  {label: "CMO", type: "function", detail: "(obj *Series, period int) *Series", info: m.ta_cmo()},
  {label: "CMOBy", type: "function", detail: "(obj *Series, period int, maType int) *Series", info: m.ta_cmo_by()},
  {label: "CHOP", type: "function", detail: "(e *BarEnv, period int) *Series", info: m.ta_chop()},
  {label: "ALMA", type: "function", detail: "(obj *Series, period int, sigma, distOff float64) *Series", info: m.ta_alma()},
  {label: "Stiffness", type: "function", detail: "(obj *Series, maLen, stiffLen, stiffMa int) *Series", info: m.ta_stiffness()},
  {label: "DV", type: "function", detail: "(h, l, c *Series, period, maLen int) *Series", info: m.ta_dv()},
  {label: "UTBot", type: "function", detail: "(c, atr *Series, rate float64) *Series", info: m.ta_ut_bot()},
  {label: "STC", type: "function", detail: "(obj *Series, period, fast, slow int, alpha float64) *Series", info: m.ta_stc()},
]

// BanBot Go包的CodeMirror自动提示数据源
const banCompletions: PackageCompletions = {
  // utils包
  "utils": [
    {
      label: "DecPow",
      type: "function",
      detail: m.utils_dec_pow(),
      info: `Parameters:\n- \`x decimal.Decimal\` - base number\n- \`y decimal.Decimal\` - exponent\nReturns:\n- \`decimal.Decimal\` - calculation result`
    },
    {
      label: "DecArithMean",
      type: "function",
      detail: m.utils_dec_arith_mean(),
      info: `Parameters:\n- \`values []decimal.Decimal\` - value array\nReturns:\n- \`decimal.Decimal\` - mean value\n- \`error\` - calculation error info`
    },
    {
      label: "DecStdDev",
      type: "function",
      detail: m.utils_dec_std_dev(),
      info: `Parameters:\n- \`values []decimal.Decimal\` - value array\nReturns:\n- \`decimal.Decimal\` - standard deviation\n- \`error\` - calculation error info`
    },
    {
      label: "SharpeRatio",
      type: "function",
      detail: m.utils_sharpe_ratio(),
      info: `Parameters:\n- \`moReturns []float64\` - returns array\n- \`riskFree float64\` - risk-free rate\nReturns:\n- \`float64\` - Sharpe ratio\n- \`error\` - calculation error info`
    },
    {
      label: "SortinoRatio",
      type: "function",
      detail: m.utils_sortino_ratio(),
      info: `Parameters:\n- \`moReturns []float64\` - returns array\n- \`riskFree float64\` - risk-free rate\nReturns:\n- \`float64\` - Sortino ratio\n- \`error\` - calculation error info`
    },
    {
      label: "SnakeToCamel",
      type: "function",
      detail: m.utils_snake_to_camel(),
      info: `Parameters:\n- \`input string\` - snake case string\nReturns:\n- \`string\` - camel case string`
    },
    {
      label: "PadCenter",
      type: "function",
      detail: m.utils_pad_center(),
      info: `Parameters:\n- \`s string\` - original string\n- \`width int\` - target width\n- \`padText string\` - padding characters\nReturns:\n- \`string\` - padded string`
    },
    {
      label: "RandomStr",
      type: "function",
      detail: m.utils_random_str(),
      info: `Parameters:\n- \`length int\` - string length\nReturns:\n- \`string\` - random string`
    },
    {
      label: "FormatWithMap",
      type: "function",
      detail: m.utils_format_with_map(),
      info: `Parameters:\n- \`text string\` - template string with placeholders\n- \`args map[string]interface{}\` - replacement map\nReturns:\n- \`string\` - formatted string`
    },
    {
      label: "GroupByPairQuotes",
      type: "function",
      detail: m.utils_group_by_pair_quotes(),
      info: `Parameters:\n- \`items map[string][]string\` - trading pair mapping\nReturns:\n- \`string\` - formatted grouping string`
    },
    {
      label: "SplitSymbol",
      type: "function",
      detail: m.utils_split_symbol(),
      info: `Parameters: pair string - trading pair name\nReturns: string - base currency\nstring - quote currency\nstring - base currency code\nstring - quote currency code`
    },
    {
      label: "CountDigit",
      type: "function",
      detail: m.count_digit(),
      info: `Parameters:\n- \`text string\` - input string\nReturns:\n- \`int\` - number of digit characters`
    },
    {
      label: "SplitSolid",
      type: "function",
      detail: m.split_solid(),
      info: `Parameters:\n- \`text string\` - string to split\n- \`sep string\` - separator\nReturns:\n- \`[]string\` - array of non-empty strings`
    },
    {
      label: "KeysOfMap",
      type: "function",
      detail: m.keys_of_map(),
      info: `Parameters:\n- \`m M\` - input map (generic type)\nReturns:\n- \`[]K\` - array of keys`
    },
    {
      label: "ValsOfMap",
      type: "function",
      detail: m.vals_of_map(),
      info: `Parameters:\n- \`m M\` - input map (generic type)\nReturns:\n- \`[]V\` - array of values`
    },
    {
      label: "CutMap",
      type: "function",
      detail: m.cut_map(),
      info: `Parameters:\n- \`m M\` - input map (generic type)\n- \`keys ...K\` - keys to extract\nReturns:\n- \`M\` - new map with specified keys`
    },
    {
      label: "UnionArr",
      type: "function",
      detail: m.union_arr(),
      info: `Parameters:\n- \`arrs ...[]T\` - arrays to merge (generic type)\nReturns:\n- \`[]T\` - merged array with duplicates removed`
    },
    {
      label: "ReverseArr",
      type: "function",
      detail: m.reverse_arr(),
      info: `Parameters:\n- \`s []T\` - array to reverse (generic type)`
    },
    {
      label: "ConvertArr",
      type: "function",
      detail: m.convert_arr(),
      info: `Parameters:\n- \`arr []T1\` - source array\n- \`doMap func(T1) T2\` - conversion function\nReturns:\n- \`[]T2\` - converted array`
    },
    {
      label: "ArrToMap",
      type: "function",
      detail: m.arr_to_map(),
      info: `Parameters:\n- \`arr []T2\` - source array\n- \`doMap func(T2) T1\` - key mapping function\nReturns:\n- \`map[T1][]T2\` - converted map with array values`
    },
    {
      label: "RemoveFromArr",
      type: "function",
      detail: m.remove_from_arr(),
      info: `Parameters:\n- \`arr []T\` - source array\n- \`it T\` - element to remove\n- \`num int\` - number to remove, negative for all\nReturns:\n- \`[]T\` - new array with elements removed`
    },
    {
      label: "UniqueItems",
      type: "function",
      detail: m.unique_items(),
      info: `Parameters:\n- \`arr []T\` - input array (generic type)\nReturns:\n- \`[]T\` - unique elements array\n- \`[]T\` - duplicate elements array`
    },
    {
      label: "DeepCopyMap",
      type: "function",
      detail: m.deep_copy_map(),
      info: `Parameters:\n- \`dst map[string]interface{}\` - destination map\n- \`src map[string]interface{}\` - source map`
    },
    {
      label: "MapToStr",
      type: "function",
      detail: m.map_to_str(),
      info: `Parameters:\n- \`m map[string]float64\` - map to convert\nReturns:\n- \`string\` - converted string\n- \`int\` - total length of numeric part`
    },
    {
      label: "MD5",
      type: "function",
      detail: m.md5(),
      info: `Parameters:\n- \`data []byte\` - input data\nReturns:\n- \`string\` - MD5 hash in hex format`
    },
    {
      label: "GetSystemLanguage",
      type: "function",
      detail: m.get_system_language(),
      info: `Returns:\n- \`string\` - system language code`
    },
    {
      label: "CalcCorrMat",
      type: "function",
      detail: m.calc_corr_mat(),
      info: `Parameters:\n- \`arrLen int\` - data length\n- \`dataArr [][]float64\` - 2D data array\n- \`useChgRate bool\` - whether to use change rate\nReturns:\n- \`*mat.SymDense\` - correlation matrix\n- \`[]float64\` - average correlation for each series\n- \`error\` - error info`
    },
    {
      label: "CalcEnvsCorr",
      type: "function",
      detail: m.calc_envs_corr(),
      info: `Parameters:\n- \`envs []*ta.BarEnv\` - bar environment list\n- \`hisNum int\` - history data count\nReturns:\n- \`*mat.SymDense\` - correlation matrix\n- \`[]float64\` - average correlation for each environment\n- \`error\` - error info`
    },
    {
      label: "CalcExpectancy",
      type: "function",
      detail: m.calc_expectancy(),
      info: `Parameters:\n- \`profits []float64\` - profit array\nReturns:\n- \`float64\` - expected return\n- \`float64\` - risk-reward ratio`
    },
    {
      label: "CalcMaxDrawDown",
      type: "function",
      detail: m.calc_max_draw_down(),
      info: `Parameters:\n- \`profits []float64\` - profit array\n- \`initBalance float64\` - initial balance\nReturns:\n- \`float64\` - maximum drawdown amount\n- \`float64\` - maximum drawdown percentage\n- \`int\` - drawdown start position\n- \`int\` - drawdown end position\n- \`float64\` - balance at drawdown start\n- \`float64\` - balance at drawdown end`
    },
    {
      label: "AutoCorrPenalty",
      type: "function",
      detail: m.auto_corr_penalty(),
      info: `Parameters:\n- \`returns []float64\` - returns array\nReturns:\n- \`float64\` - penalty factor`
    },
    {
      label: "KMeansVals",
      type: "function",
      detail: m.kmeans_vals(),
      info: `Parameters:\n- \`vals []float64\` - value array\n- \`num int\` - cluster count\nReturns:\n- \`*ClusterRes\` - clustering result`
    },
    {
      label: "StdDevVolatility",
      type: "function",
      detail: m.std_dev_volatility(),
      info: `Parameters:\n- \`data []float64\` - data array\n- \`rate float64\` - decay rate\nReturns:\n- \`float64\` - volatility`
    },
    {
      label: "IsTextContent",
      type: "function",
      detail: m.is_text_content(),
      info: `Parameters:\n- \`data []byte\` - data to check\nReturns:\n- \`bool\` - true if text content`
    },
    {
      label: "IsDocker",
      type: "function",
      detail: m.is_docker(),
      info: `Returns:\n- \`bool\` - whether running in Docker container`
    },
  ],

  // core包
  "core": [
    {
      label: "NoEnterUntil",
      type: "variable",
      detail: m.no_enter_until(),
      info: `Prohibits opening positions before given deadline timestamp`
    },
    {
      label: "Param",
      type: "struct",
      detail: m.param_struct(),
      info: `For defining and managing system parameters, especially for hyperparameter search scenarios.\nMain fields: Name(parameter name), VType(parameter value type), Min(minimum value), Max(maximum value), Mean(mean value), IsInt(is integer), Rate(normal distribution ratio)`
    },
    {
      label: "GetPrice",
      type: "function",
      detail: m.get_price(),
      info: `Parameters: symbol string - trading pair symbol\nReturns: float64 - latest price`
    },
    {
      label: "GetPriceSafe",
      type: "function",
      detail: m.get_price_safe(),
      info: `Parameters: symbol string - trading pair symbol\nReturns: float64 - processed price\nIncludes fiat currency handling logic`
    },
    {
      label: "IsMaker",
      type: "function",
      detail: m.is_maker(),
      info: `Parameters: pair string - trading pair name\nside string - trade direction\nprice float64 - price\nReturns: bool - whether it's a maker price`
    },
    {
      label: "IsFiat",
      type: "function",
      detail: m.is_fiat(),
      info: `Parameters: code string - currency code\nReturns: bool - whether it's a fiat currency`
    },
    {
      label: "SplitSymbol",
      type: "function",
      detail: m.split_symbol_core(),
      info: `Parameters: pair string - trading pair name\nReturns: string - base currency\nstring - quote currency\nstring - base currency code\nstring - quote currency code`
    },
    {
      label: "PNorm",
      type: "function",
      detail: m.pnorm(),
      info: `Parameters: min float64 - minimum value\nmax float64 - maximum value\nReturns: *Param - parameter object`
    },
    {
      label: "PNormF",
      type: "function",
      detail: m.pnorm_f(),
      info: `Parameters: min float64 - minimum value\nmax float64 - maximum value\nmean float64 - mean value\nrate float64 - ratio\nReturns: *Param - parameter object`
    },
    {
      label: "PUniform",
      type: "function",
      detail: m.puniform(),
      info: `Parameters: min float64 - minimum value\nmax float64 - maximum value\nReturns: *Param - parameter object`
    },
    {
      label: "IsLimitOrder",
      type: "function",
      detail: m.is_limit_order(),
      info: `Parameters: t int - order type\nReturns: bool - whether it's a limit order`
    },
    {
      label: "IsPriceEmpty",
      type: "function",
      detail: m.is_price_empty(),
      info: `Returns: bool - whether the price cache is empty`
    }
  ],

  // btime包
  "btime": [
    {
      label: "TimeMS",
      type: "function",
      detail: m.btime_time_ms(),
      info: `Returns: int64 - current timestamp in milliseconds`
    },
    {
      label: "UTCTime",
      type: "function",
      detail: m.btime_utc_time(),
      info: `Returns: float64 - 10-digit floating point seconds timestamp`
    },
    {
      label: "UTCStamp",
      type: "function",
      detail: m.btime_utc_stamp(),
      info: `Returns: int64 - 13-digit millisecond timestamp`
    },
    {
      label: "Time",
      type: "function",
      detail: m.btime_time(),
      info: `Returns: float64 - 10-digit seconds timestamp, real-time mode returns current time, backtest mode returns backtest time`
    },
    {
      label: "MSToTime",
      type: "function",
      detail: m.btime_ms_to_time(),
      info: `Parameters: timeMSecs int64 - 13-digit millisecond timestamp\nReturns: *time.Time - time object pointer`
    },
    {
      label: "Now",
      type: "function",
      detail: m.btime_now(),
      info: `Returns: *time.Time - time object pointer, real-time mode returns current time, backtest mode returns backtest time`
    },
    {
      label: "ParseTimeMS",
      type: "function",
      detail: m.btime_parse_time_ms(),
      info: `Parameters: timeStr string - time string\nReturns: int64 - 13-digit millisecond timestamp\nSupported formats: year(2006), date(20060102), 10-digit seconds timestamp, 13-digit millisecond timestamp, datetime(2006-01-02 15:04), datetime with seconds(2006-01-02 15:04:05)`
    },
    {
      label: "ParseTimeMSBy",
      type: "function",
      detail: m.btime_parse_time_ms_by(),
      info: `Parameters: layout string - time format template\ntimeStr string - time string\nReturns: int64 - 13-digit millisecond timestamp`
    },
    {
      label: "ToDateStr",
      type: "function",
      detail: m.btime_to_date_str(),
      info: `Parameters: timestamp int64 - timestamp (supports 10-digit seconds or 13-digit milliseconds)\nformat string - time format template (default: 2006-01-02 15:04:05)\nReturns: string - formatted time string`
    },
    {
      label: "ToDateStrLoc",
      type: "function",
      detail: m.btime_to_date_str_loc(),
      info: `Parameters: timestamp int64 - timestamp (supports 10-digit seconds or 13-digit milliseconds)\nformat string - time format template (default: 2006-01-02 15:04:05)\nReturns: string - formatted time string`
    },
    {
      label: "ToTime",
      type: "function",
      detail: m.btime_to_time(),
      info: `Parameters: timestamp int64 - timestamp (supports 10-digit seconds or 13-digit milliseconds)\nReturns: time.Time - time object`
    },
    {
      label: "CountDigit",
      type: "function",
      detail: "Count digit characters in a string",
      info: `Parameters: text string - input string\nReturns: int - count of digit characters`
    }
  ],

  // config包
  "config": [
    {
      label: "Config",
      type: "struct",
      detail: m.config_struct(),
      info: `Structure containing system global configuration`
    },
    {
      label: "CmdArgs",
      type: "struct",
      detail: m.cmd_args(),
      info: `Structure containing command line arguments`
    },
    {
      label: "RunPolicyConfig",
      type: "struct",
      detail: m.run_policy_config(),
      info: `Configuration structure for running multiple strategies simultaneously`
    },
    {
      label: "TimeTuple",
      type: "struct",
      detail: m.time_tuple(),
      info: `Structure representing start and end times of a time range`
    },
    {
      label: "GetDataDir",
      type: "function",
      detail: m.config_get_data_dir(),
      info: `Returns: string - absolute path of data directory. Returns empty string if BanDataDir environment variable is not set.`
    },
    {
      label: "GetStratDir",
      type: "function",
      detail: m.config_get_strat_dir(),
      info: `Returns: string - absolute path of strategy directory. Returns empty string if BanStratDir environment variable is not set.`
    },
    {
      label: "GetTakeOverTF",
      type: "function",
      detail: m.config_get_take_over_tf(),
      info: `Parameters: pair: string - trading pair name\ndefTF: string - default timeframe\nReturns: string - timeframe`
    },
    {
      label: "GetAccLeverage",
      type: "function",
      detail: m.config_get_acc_leverage(),
      info: `Parameters: account: string - account name\nReturns: float64 - leverage multiplier`
    },
    {
      label: "GetStakeAmount",
      type: "function",
      detail: m.config_get_stake_amount(),
      info: `Parameters: accName: string - account name\nReturns: float64 - stake amount`
    },
    {
      label: "ParseTimeRange",
      type: "function",
      detail: m.config_parse_time_range(),
      info: `Parameters: timeRange: string - time range string, format YYYYMMDD-YYYYMMDD\nReturns: int64 - start timestamp (milliseconds)\nint64 - end timestamp (milliseconds)\nerror - error message`
    },
    {
      label: "GetExportConfig",
      type: "function",
      detail: m.config_get_export_config(),
      info: `Parameters: path: string - config file path\nReturns: *ExportConfig - export config object\n*errs.Error - error message`
    }
  ],

  // biz包
  "biz": [
    {
      label: "AccOdMgrs",
      type: "variable",
      detail: m.biz_acc_od_mgrs(),
      info: `Order book object, guaranteed non-nil`
    },
    {
      label: "AccLiveOdMgrs",
      type: "variable",
      detail: m.biz_acc_live_od_mgrs(),
      info: `Live order book object, non-nil in live trading mode`
    },
    {
      label: "AccWallets",
      type: "variable",
      detail: m.biz_acc_wallets(),
      info: `Account wallets`
    },
    {
      label: "LiveOrderMgr",
      type: "struct",
      detail: m.biz_live_order_mgr(),
      info: `Used for managing orders in live trading, extends OrderMgr`
    },
    {
      label: "OrderMgr",
      type: "struct",
      detail: m.biz_order_mgr(),
      info: `Provides basic order management functions, main fields: Account(account name), BarMS(current bar timestamp)`
    },
    {
      label: "IOrderMgr",
      type: "struct",
      detail: m.iorder_mgr(),
      info: `Defines basic order management methods: ProcessOrders, EnterOrder, ExitOpenOrders, ExitOrder, UpdateByDataSeries, OnEnvEnd, CleanUp`
    },
    {
      label: "IOrderMgrLive",
      type: "struct",
      detail: m.iorder_mgr_live(),
      info: `Extends IOrderMgr, additional methods: SyncExgOrders, WatchMyTrades, TrialUnMatchesForever, ConsumeOrderQueue`
    },
    {
      label: "ItemWallet",
      type: "struct",
      detail: m.biz_item_wallet(),
      info: `Single currency wallet, main fields: Coin(currency code), Available(available balance), Pendings(locked amount), Frozens(long-term frozen amount), UnrealizedPOL(unrealized profit/loss), UsedUPol(used unrealized profit/loss), Withdraw(withdraw from balance)`
    },
    {
      label: "BanWallets",
      type: "struct",
      detail: m.biz_ban_wallets(),
      info: `Account wallet manager, main fields: Items(currency to wallet mapping), Account(account name), IsWatch(whether monitoring balance changes)`
    },
    {
      label: "GetOdMgr",
      type: "function",
      detail: "Get order manager",
      info: `Parameters: account: string - account name\nReturns: IOrderMgr - order manager interface`
    },
    {
      label: "GetAllOdMgr",
      type: "function",
      detail: "Get all order managers",
      info: `Returns: map[string]IOrderMgr - account name to order manager mapping`
    },
    {
      label: "GetLiveOdMgr",
      type: "function",
      detail: "Get live order manager",
      info: `Parameters: account: string - account name\nReturns: *LiveOrderMgr - live order manager`
    },
    {
      label: "GetWallets",
      type: "function",
      detail: "Get wallets",
      info: `Parameters: account: string - account name\nReturns: *BanWallets - wallet object`
    }
  ],

  // orm包
  "orm": [
    {
      label: "HistODs",
      type: "variable",
      detail: m.hist_ods(),
      info: `All closed positions, backtest use only`
    },
    {
      label: "AccOpenODs",
      type: "variable",
      detail: m.acc_open_ods(),
      info: `Not submitted, submitted but not entered, partially entered, fully entered, partially exited`
    },
    {
      label: "ExSymbol",
      type: "struct",
      detail: m.ex_symbol(),
      info: `Contains exchange and trading pair basic info.\nMain fields: ID(trading pair ID), Exchange(exchange name), ExgReal(actual exchange identifier), Market(market type), Symbol(trading pair symbol), Combined(whether combined trading pair), ListMs(listing timestamp), DelistMs(delisting timestamp)`
    },
    {
      label: "GetExSymbols",
      type: "function",
      detail: m.get_ex_symbols(),
      info: `Parameters: exgName string - exchange name, market string - market name\nReturns: map[int32]*ExSymbol - trading pair ID to trading pair info mapping`
    },
    {
      label: "GetExSymbolMap",
      type: "function",
      detail: m.get_ex_symbol_map(),
      info: `Parameters: exgName string - exchange name, market string - market name\nReturns: map[string]*ExSymbol - trading pair name to trading pair info mapping`
    },
    {
      label: "GetSymbolByID",
      type: "function",
      detail: m.get_symbol_by_id(),
      info: `Parameters: id int32 - trading pair ID\nReturns: *ExSymbol - trading pair info`
    },
    {
      label: "GetExSymbolCur",
      type: "function",
      detail: m.get_ex_symbol_cur(),
      info: `Parameters: symbol string - trading pair name\nReturns: *ExSymbol - trading pair info, *errs.Error - error info`
    },
    {
      label: "GetExSymbol",
      type: "function",
      detail: m.get_ex_symbol(),
      info: `Parameters: exchange banexg.BanExchange - exchange interface, symbol string - trading pair name\nReturns: *ExSymbol - trading pair info, *errs.Error - error info`
    },
    {
      label: "GetAllExSymbols",
      type: "function",
      detail: m.get_all_ex_symbols(),
      info: `Returns: map[int32]*ExSymbol - trading pair ID to trading pair info mapping`
    },
    {
      label: "ParseShort",
      type: "function",
      detail: m.parse_short(),
      info: `Parameters: exgName string - exchange name, short string - short format trading pair name\nReturns: *ExSymbol - trading pair info, *errs.Error - error info`
    },
    {
      label: "MapExSymbols",
      type: "function",
      detail: m.map_ex_symbols(),
      info: `Parameters: exchange banexg.BanExchange - exchange interface, symbols []string - trading pair name list\nReturns: map[int32]*ExSymbol - trading pair ID to trading pair info mapping, *errs.Error - error info`
    },
    {
      label: "AutoFetchOHLCV",
      type: "function",
      detail: m.auto_fetch_ohlcv(),
      info: `Parameters: exchange banexg.BanExchange - exchange interface, exs *ExSymbol - trading pair info, timeFrame string - timeframe, startMS int64 - start time (milliseconds), endMS int64 - end time (milliseconds), limit int - limit count, withUnFinish bool - whether to include unfinished candles, pBar *utils.PrgBar - progress bar\nReturns: []*AdjInfo - price adjustment info, []*banexg.Kline - candle data, *errs.Error - error info`
    },
    {
      label: "GetOHLCV",
      type: "function",
      detail: m.ohlcv_get_ohlcv(),
      info: `Parameters: exs *ExSymbol - trading pair info, timeFrame string - timeframe, startMS int64 - start time (milliseconds), endMS int64 - end time (milliseconds), limit int - limit count, withUnFinish bool - whether to include unfinished candles\nReturns: []*AdjInfo - price adjustment info, []*banexg.Kline - candle data, *errs.Error - error info`
    },
    {
      label: "BulkDownOHLCV",
      type: "function",
      detail: m.ohlcv_bulk_down_ohlcv(),
      info: `Parameters: exchange banexg.BanExchange - exchange interface, exsList map[int32]*ExSymbol - trading pair list, timeFrame string - timeframe, startMS int64 - start time (milliseconds), endMS int64 - end time (milliseconds), limit int - limit count, prg utils.PrgCB - progress callback\nReturns: *errs.Error - error info`
    },
    {
      label: "FetchApiOHLCV",
      type: "function",
      detail: m.ohlcv_fetch_api_ohlcv(),
      info: `Parameters: ctx context.Context - context object, exchange banexg.BanExchange - exchange interface, pair string - trading pair name, timeFrame string - timeframe, startMS int64 - start time (milliseconds), endMS int64 - end time (milliseconds), out chan []*banexg.Kline - candle data output channel\nReturns: *errs.Error - error info`
    },
    {
      label: "ApplyAdj",
      type: "function",
      detail: m.ohlcv_apply_adj(),
      info: `Parameters: adjs []*AdjInfo - price adjustment info, klines []*banexg.Kline - candle data, adj int - adjustment type, cutEnd int64 - cutoff time, limit int - limit count\nReturns: []*banexg.Kline - adjusted candle data`
    },
    {
      label: "FastBulkOHLCV",
      type: "function",
      detail: m.ohlcv_fast_bulk_ohlcv(),
      info: `Parameters: exchange banexg.BanExchange - exchange interface, symbols []string - trading pair name list, timeFrame string - timeframe, startMS int64 - start time (milliseconds), endMS int64 - end time (milliseconds), limit int - limit count, handler func(string, string, []*banexg.Kline, []*AdjInfo) - data processing callback function\nReturns: *errs.Error - error info`
    },
    {
      label: "GetAlignOff",
      type: "function",
      detail: m.ohlcv_get_align_off(),
      info: `Parameters: sid int32 - trading pair ID, toTfMSecs int64 - target timeframe (milliseconds)\nReturns: int64 - time offset (milliseconds)`
    },
    {
      label: "GetDownTF",
      type: "function",
      detail: m.ohlcv_get_down_tf(),
      info: `Parameters: timeFrame string - timeframe\nReturns: string - next level timeframe, *errs.Error - error info`
    },
  ],

  // exg包
  "exg": [
    {
      label: "Default",
      type: "variable",
      detail: m.exg_default(),
      info: `Default exchange instance`
    },
    {
      label: "GetWith",
      type: "function",
      detail: m.exg_get_with(),
      info: `Parameters:\n- \`name string\` - exchange name\n- \`market string\` - market type\n- \`contractType string\` - contract type\nReturns:\n- \`banexg.BanExchange\` - exchange instance\n- \`*errs.Error\` - error info if error occurs during acquisition, otherwise nil`
    },
    {
      label: "PrecCost",
      type: "function",
      detail: m.exg_prec_cost(),
      info: `Parameters:\n- \`exchange banexg.BanExchange\` - exchange instance\n- \`symbol string\` - trading pair symbol\n- \`cost float64\` - original cost amount\nReturns:\n- \`float64\` - cost amount processed according to exchange precision\n- \`*errs.Error\` - error info if error occurs during processing, otherwise nil`
    },
    {
      label: "PrecPrice",
      type: "function",
      detail: m.exg_prec_price(),
      info: `Parameters:\n- \`exchange banexg.BanExchange\` - exchange instance\n- \`symbol string\` - trading pair symbol\n- \`price float64\` - original price\nReturns:\n- \`float64\` - price processed according to exchange precision\n- \`*errs.Error\` - error info if error occurs during processing, otherwise nil`
    },
    {
      label: "PrecAmount",
      type: "function",
      detail: m.exg_prec_amount(),
      info: `Parameters:\n- \`exchange banexg.BanExchange\` - exchange instance\n- \`symbol string\` - trading pair symbol\n- \`amount float64\` - original amount\nReturns:\n- \`float64\` - amount processed according to exchange precision\n- \`*errs.Error\` - error info if error occurs during processing, otherwise nil`
    },
    {
      label: "GetLeverage",
      type: "function",
      detail: m.exg_get_leverage(),
      info: `Parameters:\n- \`symbol string\` - trading pair symbol\n- \`notional float64\` - notional value\n- \`account string\` - account identifier\nReturns:\n- \`float64, float64\` - two float values related to leverage ratio`
    },
    {
      label: "GetOdBook",
      type: "function",
      detail: m.exg_get_od_book(),
      info: `Parameters:\n- \`pair string\` - trading pair symbol\nReturns:\n- \`*banexg.OrderBook\` - order book data\n- \`*errs.Error\` - error info if error occurs during acquisition, otherwise nil`
    },
    {
      label: "GetTickers",
      type: "function",
      detail: m.exg_get_tickers(),
      info: `Returns:\n- \`map[string]*banexg.Ticker\` - market data mapping with trading pairs as keys\n- \`*errs.Error\` - error info if error occurs during acquisition, otherwise nil`
    },
    {
      label: "GetAlignOff",
      type: "function",
      detail: m.exg_get_align_off(),
      info: `Parameters:\n- \`exgName string\` - exchange name\n- \`tfSecs int\` - timeframe (in seconds)\nReturns:\n- \`int\` - alignment offset`
    }
  ],

  // strat包
  "strat": [
    {
      label: "StagyMap",
      type: "variable",
      detail: m.strat_stagy_map(),
      info: `Mapping of all registered strategies`
    },
    {
      label: "Versions",
      type: "variable",
      detail: m.strat_versions(),
      info: `Version information for all strategies`
    },
    {
      label: "TradeStrat",
      type: "struct",
      detail: m.strat_trade_strat(),
      info: `Defines basic properties and behaviors of strategies.\nMain fields: Name(strategy name), Version(version number), WarmupNum(warmup candles required), MinTfScore(minimum timeframe quality), WatchBook(whether to monitor order book), DrawDownExit(whether to enable drawdown exit), BatchInOut(whether to batch execute entry/exit), BatchInfo(whether to batch process after OnInfoBar), StakeRate(relative base amount multiplier), StopEnterBars(limit entry order timeout candles), EachMaxLong(maximum long orders per trading pair), EachMaxShort(maximum short orders per trading pair), AllowTFs(allowed timeframes), Outputs(strategy output text file content), Policy(strategy run configuration)`
    },
    {
      label: "StratJob",
      type: "struct",
      detail: m.strat_strat_job(),
      info: `Responsible for executing specific trading operations.\nMain fields: Strat(parent strategy), Env(candle environment), Entrys(entry request list), Exits(exit request list), LongOrders(long order list), ShortOrders(short order list), Symbol(current running symbol), TimeFrame(current running timeframe), Account(current task account), TPMaxs(maximum profit price for orders), OrderNum(all unfinished order count), EnteredNum(fully/partially entered order count), CheckMS(last signal processing timestamp), MaxOpenLong(maximum open long count), MaxOpenShort(maximum open short count), CloseLong(whether to allow close long), CloseShort(whether to allow close short), ExgStopLoss(whether to allow exchange stop loss), LongSLPrice(default long stop loss price at open), ShortSLPrice(default short stop loss price at open), ExgTakeProfit(whether to allow exchange take profit), LongTPPrice(default long take profit price at open), ShortTPPrice(default short take profit price at open), IsWarmUp(whether currently in warmup state), More(strategy custom additional information)`
    },
    {
      label: "BatchTask",
      type: "struct",
      detail: m.strat_batch_task(),
      info: `Main fields: Job(strategy task instance), Type(task type)`
    },
    {
      label: "BatchMap",
      type: "struct",
      detail: m.strat_batch_map(),
      info: `Batch execution task pool for all symbols under current exchange-market-timeframe.\nMain fields: Map(task mapping), TFMSecs(timeframe milliseconds), ExecMS(batch task execution timestamp)`
    },
    {
      label: "PairSub",
      type: "struct",
      detail: m.strat_pair_sub(),
      info: `Main fields: Pair(trading pair name), TimeFrame(timeframe), WarmupNum(warmup count)`
    },
    {
      label: "EnterReq",
      type: "struct",
      detail: m.strat_enter_req(),
      info: `Main fields: Tag(entry signal), StgyName(strategy name), Short(whether short), OrderType(order type), Limit(limit order entry price), CostRate(open position multiplier), LegalCost(fiat currency amount spent), Leverage(leverage multiplier), Amount(entry target amount), StopLossVal(distance from entry price to stop loss price), StopLoss(stop loss trigger price), StopLossLimit(stop loss limit price), StopLossRate(stop loss exit ratio), StopLossTag(stop loss reason), TakeProfitVal(distance from entry price to take profit price), TakeProfit(take profit trigger price), TakeProfitLimit(take profit limit price), TakeProfitRate(take profit exit ratio), TakeProfitTag(take profit reason), StopBars(entry limit order timeout candles)`
    },
    {
      label: "ExitReq",
      type: "struct",
      detail: m.strat_exit_req(),
      info: `Main fields: Tag(exit signal), StgyName(strategy name), EnterTag(only exit orders with entry signal EnterTag), Dirt(direction), OrderType(order type), Limit(limit order exit price), ExitRate(exit ratio), Amount(target amount to exit), OrderID(only exit specified order), UnFillOnly(only exit unfilled part), FilledOnly(only exit filled orders), Force(whether force exit)`
    },
    {
      label: "Get",
      type: "function",
      detail: m.strat_get(),
      info: `Parameters: pair string - trading pair name\nstratID string - strategy ID\nReturns: *TradeStrat - trading strategy instance, nil if not exists`
    },
    {
      label: "GetStratPerf",
      type: "function",
      detail: m.strat_get_strat_perf(),
      info: `Parameters: pair string - trading pair name\nstrat string - strategy name\nReturns: *config.StratPerfConfig - strategy performance configuration`
    },
    {
      label: "AddStratGroup",
      type: "function",
      detail: m.strat_add_strat_group(),
      info: `Parameters: group string - strategy group name\nitems map[string]FuncMakeStrat - strategy creation function mapping`
    },
    {
      label: "GetJobs",
      type: "function",
      detail: m.strat_get_jobs(),
      info: `Parameters: account string - account name\nReturns: map[string]map[string]*StratJob - task mapping grouped by trading pair and strategy`
    },
    {
      label: "GetInfoJobs",
      type: "function",
      detail: m.strat_get_info_jobs(),
      info: `Parameters: account string - account name\nReturns: map[string]map[string]*StratJob - info task mapping grouped by trading pair and strategy`
    },
  ],

  // rpc包
  "rpc": [
    {
      label: "SendMsg",
      type: "function",
      detail: m.rpc_send_msg(),
      info: `Send message to configured notification channels.\nParameters: msg map[string]interface{} - message content, including fields: type - message type, account - account identifier (optional), status - message status content, other fields for webhook template rendering`
    },
    {
      label: "TrySendExc",
      type: "function",
      detail: m.rpc_try_send_exc(),
      info: `Try to send exception information, will merge identical exceptions.\nParameters: cacheKey string - exception cache key, usually the call location, content string - exception content`
    },
  ],
  "ta": taCompletions,
  "banta": taCompletions,
};


// 创建自动补全扩展
export function createBanBotCompletionSource(context: any) {
  const packageMatch = context.matchBefore(/\w+\.(\w*)$/);
  if (packageMatch) {
    const packageName = packageMatch.text.split('.')[0];
    if (banCompletions[packageName]) {
      const afterDot = (packageMatch.text.split('.')[1] || '').toLowerCase();
      return {
        from: packageMatch.from + packageName.length + 1,
        options: banCompletions[packageName]
          .filter(item => !afterDot || item.label.toLowerCase().startsWith(afterDot))
          .map(item => ({
            label: item.label,
            type: item.type,
            detail: item.detail || "",
            info: item.info || "",
            boost: item.type === 'method' ? 2 : 1  // 方法优先显示
          }))
      };
    }
  }
  return null;
}