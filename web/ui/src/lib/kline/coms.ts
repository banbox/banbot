import * as kc from 'klinecharts';
import type {CandleStyle} from 'klinecharts';
import type {BarArr, Period, Timespan} from "./types";
import {TFToSecs, formatDate, dateTimeFormat} from "../dateutil";
import * as m from '$lib/paraglide/messages.js'

export const formatPrecision = kc.utils.formatPrecision
export const formatThousands = kc.utils.formatThousands
export const formatBigNumber = kc.utils.formatBigNumber

const _periods: Record<string, Period> = {}
// 有些周期需要对齐到指定的日期，下面应该按tfsecs从大到小排序
const tfsecs_origins = [
  {tfsecs: 604800, origin: 345600, date: '1970-01-05'},  // 周级别，从1970-01-05星期一开始
]


export function GetNumberDotOffset(value: number){
  value = Math.abs(value)
  if(value >= 1)return 0
  let count = 0;
  while (value < 1){
    value = value * 10;
    count += 1;
  }
  return count;
}

export function makePeriod(timeframe: string): Period {
  if (_periods[timeframe]) return _periods[timeframe]
  const sep_id = timeframe.length - 1
  const unit = timeframe.substring(sep_id);
  const num = timeframe.substring(0, sep_id);
  const num_val = parseInt(num);
  let timespan: Timespan = 'minute';
  if (unit == 'w') {
    timespan = 'week'
  } else if (unit == 'd') {
    timespan = 'day'
  } else if (unit == 'h') {
    timespan = 'hour'
  } else if (unit == 'm') {
    timespan = 'minute'
  } else if (unit == 's') {
    timespan = 'second'
  } else {
    throw new Error(`unsupport period: ${timeframe}`)
  }
  const secs = TFToSecs(timeframe)
  _periods[timeframe] = {multiplier: num_val, timespan, timeframe, secs}
  return _periods[timeframe]
}

export const AllPeriods: Period[] = [
  makePeriod('1m'),
  makePeriod('5m'),
  makePeriod('15m'),
  makePeriod('30m'),
  makePeriod('1h'),
  makePeriod('2h'),
  makePeriod('4h'),
  makePeriod('8h'),
  makePeriod('12h'),
  makePeriod('1d'),
  makePeriod('3d'),
  makePeriod('1w'),
]


function getIconTool(id: string, icon: string, color: string, ){
  return{
    id,
    position: 'middle',
    marginLeft: 8,
    marginTop: 2,
    marginRight: 0,
    marginBottom: 0,
    paddingLeft: 0,
    paddingTop: 0,
    paddingRight: 0,
    paddingBottom: 0,
    size: 14,
    color: color,
    activeColor: color,
    backgroundColor: 'transparent',
    activeBackgroundColor: 'rgba(22, 119, 255, 0.15)',
    type: 'icon_font',
    iconFont: {
      content: icon,
      family: 'icomoon'
    }
  }
}

  
export function getThemeStyles(theme: string): Record<string, any> {
  const color = theme === 'dark' ? '#929AA5' : '#76808F'
  const lineColor = theme === 'dark' ? '#555555' : '#dddddd'
  // 注意，modalSetting中设置的样式path，必须在这里指定，否则无法重置样式
  return {
    candle: {
      type: kc.CandleType.CandleSolid,
      priceMark: {
        last: {show: true},
        high: {show: true},
        low: {show: true},
      },
      tooltip: {
        custom: function (data: any, styles: CandleStyle) {
          const defVal = styles.tooltip.defaultValue
          const current = data.current
          const prevClose = data.prev?.close ?? current.close
          const changeValue = current.close - prevClose
          const thousandsSeparator = ','
          const clow = data.current.low
          const minProce = Math.min(clow, data.prev?.low ?? clow, data.next?.low ?? clow)
          const pricePrecision = GetNumberDotOffset(minProce) + 2
          const volumePrecision = 3
      
          const volPrecision = formatPrecision(current.volume ?? defVal, volumePrecision)
          const volume = formatThousands(formatBigNumber(volPrecision), thousandsSeparator)
          const change = prevClose === 0 ? defVal : `${formatPrecision(changeValue / prevClose * 100)}%`
          return [
            {title: m.time_(), value: formatDate(dateTimeFormat!, current.timestamp, 'YYYY-MM-DD HH:mm')},
            {title: m.open_(), value: formatThousands(formatPrecision(current.open, pricePrecision), thousandsSeparator)},
            {title: m.high_(), value: formatThousands(formatPrecision(current.high, pricePrecision), thousandsSeparator)},
            {title: m.low_(), value: formatThousands(formatPrecision(current.low, pricePrecision), thousandsSeparator)},
            {title: m.close_(), value: formatThousands(formatPrecision(current.close, pricePrecision), thousandsSeparator)},
            {title: m.volume_(), value: volume},
            {title: m.change_(), value: change}
          ]
        }
      }
    },
    indicator: {
      lastValueMark: {
        show: true,
      },
      tooltip: {
        features: [
          getIconTool('visible', '\ue903', color),
          getIconTool('invisible', '\ue901', color),
          getIconTool('setting', '\ue902', color),
          getIconTool('close', '\ue900', color),
        ]
      }
    },
    yAxis: {
      type: 'normal',
      reverse: false,
    },
    grid: {
      show: true,
      horizontal:{
        color: lineColor,
      },
      vertical:{
        color: lineColor,
      }
    }
  }
}

export type IndFieldType = 'number' | 'text' | 'select' | 'switch'
export type IndFieldOption = {
  label: string
  value: string | number | boolean
}
export type IndField = {
  title: string
  type?: IndFieldType
  default?: string | number | boolean
  options?: Array<IndFieldOption | string | number | boolean>
  min?: number
  max?: number
  step?: number
  precision?: number
  placeholder?: string
  styleKey?: string
  valueType?: 'number' | 'string' | 'boolean'
}

export function normalizeIndFieldOptions(options?: IndField['options']): IndFieldOption[] {
  if (!options || options.length === 0) return []
  return options.map((opt) => {
    if (typeof opt === 'object' && opt !== null && 'value' in opt) {
      return opt as IndFieldOption
    }
    return { label: String(opt), value: opt as string | number | boolean }
  })
}

export function inferIndFieldType(field: IndField, value?: unknown): IndFieldType {
  if (field.type) return field.type
  if (field.options && field.options.length > 0) return 'select'
  if (typeof value === 'boolean') return 'switch'
  if (typeof value === 'string') return 'text'
  return 'number'
}

export function getIndFieldDefaultValue(field: IndField): string | number | boolean {
  if (field.default !== undefined) return field.default
  const options = normalizeIndFieldOptions(field.options)
  if (options.length > 0) return options[0].value
  const fieldType = inferIndFieldType(field)
  if (fieldType === 'switch') return false
  if (fieldType === 'text') return ''
  return 0
}

export function getIndCalcParams(fields: IndField[]): Array<string | number | boolean> {
  return fields.map((field) => getIndFieldDefaultValue(field))
}

const param = m.param();
  
export const IndFieldsMap: Record<string, IndField[]> = {
  AO: [
    { title: param + '1', precision: 0, min: 1, default: 5 },
    { title: param + '2', precision: 0, min: 1, default: 34 }
  ],
  BIAS: [
    { title: 'BIAS1', precision: 0, min: 1, styleKey: 'lines[0].color', default: 6 },
    { title: 'BIAS2', precision: 0, min: 1, styleKey: 'lines[1].color', default: 12 },
    { title: 'BIAS3', precision: 0, min: 1, styleKey: 'lines[2].color', default: 24 },
    { title: 'BIAS4', precision: 0, min: 1, styleKey: 'lines[3].color', default: 48 },
    { title: 'BIAS5', precision: 0, min: 1, styleKey: 'lines[4].color', default: 96 }
  ],
  BOLL: [
    { title: m.period(), precision: 0, min: 1, default: 20 },
    { title: m.standard_deviation(), precision: 2, min: 1, default: 2 }
  ],
  BRAR: [
    { title: m.period(), precision: 0, min: 1, default: 26 }
  ],
  BBI: [
    { title: param + '1', precision: 0, min: 1, default: 3 },
    { title: param + '2', precision: 0, min: 1, default: 6 },
    { title: param + '3', precision: 0, min: 1, default: 12 },
    { title: param + '4', precision: 0, min: 1, default: 24 }
  ],
  CCI: [
    { title: param + '1', precision: 0, min: 1, default: 20 }
  ],
  CR: [
    { title: param + '1', precision: 0, min: 1, default: 26 },
    { title: param + '2', precision: 0, min: 1, default: 10 },
    { title: param + '3', precision: 0, min: 1, default: 20 },
    { title: param + '4', precision: 0, min: 1, default: 40 },
    { title: param + '5', precision: 0, min: 1, default: 60 }
  ],
  DMA: [
    { title: param + '1', precision: 0, min: 1, default: 10 },
    { title: param + '2', precision: 0, min: 1, default: 50 },
    { title: param + '3', precision: 0, min: 1, default: 10 }
  ],
  DMI: [
    { title: param + '1', precision: 0, min: 1, default: 14 },
    { title: param + '2', precision: 0, min: 1, default: 6 }
  ],
  EMV: [
    { title: param + '1', precision: 0, min: 1, default: 14 },
    { title: param + '2', precision: 0, min: 1, default: 9 }
  ],
  EMA: [
    { title: 'EMA1', precision: 0, min: 1, styleKey: 'lines[0].color', default: 5 },
    { title: 'EMA2', precision: 0, min: 1, styleKey: 'lines[1].color', default: 10 },
    { title: 'EMA3', precision: 0, min: 1, styleKey: 'lines[2].color', default: 30 },
  ],
  MTM: [
    { title: param + '1', precision: 0, min: 1, default: 12 },
    { title: param + '2', precision: 0, min: 1, default: 6 }
  ],
  MA: [
    { title: 'MA1', precision: 0, min: 1, styleKey: 'lines[0].color', default: 5 },
    { title: 'MA2', precision: 0, min: 1, styleKey: 'lines[1].color', default: 10 },
    { title: 'MA3', precision: 0, min: 1, styleKey: 'lines[2].color', default: 30 },
  ],
  MACD: [
    { title: param + '1', precision: 0, min: 1, default: 12 },
    { title: param + '2', precision: 0, min: 1, default: 26 },
    { title: param + '3', precision: 0, min: 1, default: 9 }
  ],
  OBV: [
    { title: param + '1', precision: 0, min: 1, default: 30 }
  ],
  PVT: [],
  PSY: [
    { title: param + '1', precision: 0, min: 1, default: 12 },
    { title: param + '2', precision: 0, min: 1, default: 6 }
  ],
  ROC: [
    { title: param + '1', precision: 0, min: 1, default: 12 },
    { title: param + '2', precision: 0, min: 1, default: 6 }
  ],
  RSI: [
    { title: 'RSI1', precision: 0, min: 1, styleKey: 'lines[0].color', default: 6 },
    { title: 'RSI2', precision: 0, min: 1, styleKey: 'lines[1].color', default: 14 },
    { title: 'RSI3', precision: 0, min: 1, styleKey: 'lines[2].color', default: 24 },
    { title: 'RSI4', precision: 0, min: 1, styleKey: 'lines[3].color', default: 48 },
    { title: 'RSI5', precision: 0, min: 1, styleKey: 'lines[4].color', default: 96 }
  ],
  SMA: [
    { title: param + '1', precision: 0, min: 1, default: 12 },
    { title: param + '2', precision: 0, min: 1, default: 2 }
  ],
  KDJ: [
    { title: param + '1', precision: 0, min: 1, default: 9 },
    { title: param + '2', precision: 0, min: 1, default: 3 },
    { title: param + '3', precision: 0, min: 1, default: 3 }
  ],
  SAR: [
    { title: param + '1', precision: 0, min: 1, default: 2 },
    { title: param + '2', precision: 0, min: 1, default: 2 },
    { title: param + '3', precision: 0, min: 1, default: 20 }
  ],
  TRIX: [
    { title: param + '1', precision: 0, min: 1, default: 12 },
    { title: param + '2', precision: 0, min: 1, default: 9 }
  ],
  VOL: [
    { title: param + '1', precision: 0, min: 1, default: 5 },
    { title: param + '2', precision: 0, min: 1, default: 10 },
    { title: param + '3', precision: 0, min: 1, default: 20 }
  ],
  VR: [
    { title: param + '1', precision: 0, min: 1, default: 26 },
    { title: param + '2', precision: 0, min: 1, default: 6 }
  ],
  WR: [
    { title: 'WR1', precision: 0, min: 1, styleKey: 'lines[0].color', default: 5 },
    { title: 'WR2', precision: 0, min: 1, styleKey: 'lines[1].color', default: 10 },
    { title: 'WR3', precision: 0, min: 1, styleKey: 'lines[2].color', default: 20 },
    { title: 'WR4', precision: 0, min: 1, styleKey: 'lines[3].color', default: 30 },
    { title: 'WR5', precision: 0, min: 1, styleKey: 'lines[4].color', default: 60 },
  ]
}

export function isNumber (value: any): value is number {
  return typeof value === 'number' && !isNaN(value)
}

export function readableNumber (value: string | number, keepLen=2): string {
  const v = +value
  if (isNumber(v)) {
    if (v > 1000000000) {
      return `${+((v / 1000000000).toFixed(keepLen))}B`
    }
    if (v > 1000000) {
      return `${+((v / 1000000).toFixed(keepLen))}M`
    }
    if (v > 1000) {
      return `${+((v / 1000).toFixed(keepLen))}K`
    }
  }
  return `${value}`
}


export function align_tfsecs(time_secs: number, tf_secs: number){
  if(time_secs > 1000000000000){
    throw Error('10 digit timestamp is require for align_tfsecs')
  }
  let origin_off = 0
  for(const item of tfsecs_origins){
    if(tf_secs < item.tfsecs)break
    if(tf_secs % item.tfsecs == 0){
      origin_off = item.origin
      break
    }
  }
  if(!origin_off){
    return Math.floor(time_secs / tf_secs) * tf_secs
  }
  return Math.floor((time_secs - origin_off) / tf_secs) * tf_secs + origin_off
}


export function align_tfmsecs(time_msecs: number, tf_msecs: number){
  if(time_msecs < 1000000000000){
    throw Error('13 digit timestamp is require for align_tfmsecs')
  }
  if(tf_msecs < 1000){
    throw Error('milliseconds tf_msecs is require for align_tfmsecs')
  }
  const time_secs = Math.floor(time_msecs / 1000)
  const tf_secs = Math.floor(tf_msecs / 1000)
  return align_tfsecs(time_secs, tf_secs) * 1000
}

export function build_ohlcvs(details: BarArr[], in_msecs: number, tf_msecs: number, last_bar: BarArr | null = null): BarArr[] {
  if(last_bar){
    last_bar[0] = align_tfmsecs(last_bar[0], tf_msecs)
  }
  if(in_msecs == tf_msecs){
    if(last_bar && details[0][0] > last_bar[0]){
      details.splice(0, 0, last_bar)
    }
    return details
  }
  const result: BarArr[] = last_bar ? [last_bar] : []
  let lastIdx = result.length - 1
  details.forEach((row: BarArr, index: number) => {
    row[0] = align_tfmsecs(row[0], tf_msecs)
    if(lastIdx < 0 || row[0] > result[lastIdx][0]){
      result.push(row)
      lastIdx += 1
    }
    else{
      const prow = result[lastIdx]
      prow[2] = Math.max(prow[2], row[2])
      prow[3] = Math.min(prow[3], row[3])
      prow[4] = row[4]
      prow[5] += row[5]
    }
  })
  return result
}
