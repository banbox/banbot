import type {IndicatorTemplate, KLineData} from 'klinecharts';
import {postApi} from "../../netio";
import {drawCloudInd} from "./common";
import {IndFieldsMap} from "../coms"
import * as m from "$lib/paraglide/messages";

const param = m.param();

/**
 * 为CSV文件创建指标模板
 * CSV文件的每一列（除time外）都作为一个figure显示
 */
export const makeCsvIndicator = (csvName: string): IndicatorTemplate => {
  // 缓存CSV数据和列信息
  let cachedColumns: string[] = [];
  let cachedData: Map<number, Record<string, number>> = new Map();
  let lastFetchKey = '';

  return {
    name: csvName,
    shortName: csvName.replace('.csv', ''),
    figures: [], // Will be dynamically set after first data fetch
    calc: async (dataList: KLineData[], ind) => {
      if (dataList.length === 0) return [];

      const startMs = dataList[0].timestamp;
      const endMs = dataList[dataList.length - 1].timestamp;
      const tfSecs = dataList.length > 1 
        ? Math.round((dataList[1].timestamp - dataList[0].timestamp) / 1000)
        : 60;
      
      const fetchKey = `${csvName}_${startMs}_${endMs}_${tfSecs}`;
      
      // Only fetch if params changed
      if (fetchKey !== lastFetchKey) {
        const rsp = await postApi('/kline/csv/data', {
          name: csvName,
          start_ms: startMs,
          end_ms: endMs,
          tf_secs: tfSecs
        });

        if (rsp.code !== 200 || !rsp.data) {
          console.error('fetch csv data fail:', rsp);
          return dataList.map(() => ({}));
        }

        cachedColumns = rsp.columns || [];
        cachedData.clear();
        
        // Build time-indexed map from response data
        const csvData = rsp.data as Record<string, number>[];
        // CSV data is aligned to kline timestamps
        for (let i = 0; i < csvData.length && i < dataList.length; i++) {
          cachedData.set(dataList[i].timestamp, csvData[i]);
        }
        
        lastFetchKey = fetchKey;

        // Dynamically update figures based on columns
        if (cachedColumns.length > 0 && ind.figures.length === 0) {
          ind.figures = cachedColumns.map((col) => ({
            key: col,
            title: `${col}: `,
            type: 'line'
          }));
        }
      }

      // Map cached data to dataList
      return dataList.map(kline => {
        const csvRow = cachedData.get(kline.timestamp);
        if (!csvRow) return {};
        
        const result: Record<string, number> = {};
        cachedColumns.forEach(col => {
          if (csvRow[col] !== undefined && !isNaN(csvRow[col])) {
            result[col] = csvRow[col];
          }
        });
        return result;
      });
    }
  };
};
/**
 * 按传入的参数生成云端指标。
 * 支持自定义图形：
 * tag: 买卖点显示，做多时值为正数的价格，做空时值为负数的价格。
 * @param params
 */
export const makeCloudInds = (params: Record<string, any>[]): IndicatorTemplate[] => {
  return params.map((args): IndicatorTemplate => {
    const name = args['name']
    const figures = args['figures'] ?? []
    const calcParams = args['calcParams'] ?? [];
    const figureTpl = args['figure_tpl'];
    if (calcParams.length > 0){
      let fields: Array<any> = []
      calcParams.forEach((v: any, i: number) => {
        if (figureTpl){
          let key = figureTpl.replace(/\{i\}/, i+1)
          let plot_type = args['figure_type'];
          if (!plot_type){
            plot_type = 'line'
          }
          figures.push({key, title: `${key.toUpperCase()}: `, type: plot_type})
        }
        fields.push({ title: param + (i+1), precision: 0, min: 1, styleKey: `lines[${i}].color`, default: v })
      })
      IndFieldsMap[name] = fields
    }
    return {
      ...args, name, figures,
      calc: async (dataList, ind) => {
        const name = ind.name;
        const params = ind.calcParams;
        const kwargs = ind.extendData;
        const kline = dataList.map(d => [d.timestamp, d.open, d.high, d.low, d.close, d.volume]);
        if (kline.length == 0){return []}
        const rsp = await postApi('/kline/calc_ind', {name, params, kline, kwargs})
        if (rsp.code != 200 || !rsp.data) {
          console.error('calc ind fail:', rsp)
          return dataList.map(d => {
            return {}
          })
        }
        return rsp.data ?? []
      },
      draw: drawCloudInd
    }
  })
}

export default makeCloudInds
