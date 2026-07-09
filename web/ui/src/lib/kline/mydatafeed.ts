import type {Datafeed, SymbolInfo, BarArr, DatafeedWatchCallback, KData, GetKlineArgs} from './types';
import {getApi} from "../netio";
import {site} from "$lib/stores/site";
import {get} from "svelte/store";

function seriesToBar(row: any): BarArr {
  const values = row.Values ?? row.values ?? {}
  return [
    row.TimeMS ?? row.timeMS ?? row.time_ms,
    values.open,
    values.high,
    values.low,
    values.close,
    values.volume ?? 0
  ]
}

export default class MyDatafeed implements Datafeed {

  private _prevSymbol?: SymbolInfo
  private _ws?: WebSocket
  private listens: Record<string, any> = {}

  async getHistoryKLineData({symbol, period, from, to, strategy}: GetKlineArgs): Promise<KData> {
    const query = {
      symbol: symbol.ticker,
      timeframe: period.timeframe,
      exchange: symbol.exchange,
      from, to, strategy
    }
    const rsp = await getApi('/kline/hist', query)
    const kline_data = (rsp.data || []).map((data: any) => ({
      timestamp: data[0],
      open: data[1],
      high: data[2],
      low: data[3],
      close: data[4],
      volume: data[5]
    }))
    return await {data: kline_data, lays: rsp.overlays ?? []}
  }

  async getSymbols(): Promise<SymbolInfo[]> {
    const rsp = await getApi(`/kline/symbols`)
    return await (rsp.data || []).map((data: any) => ({
      ticker: data.symbol,
      name: data.symbol,
      shortName: data.short_name,
      market: data.market ?? 'spot',
      exchange: data.exchange,
      priceCurrency: 'USDT',
      type: data.type,
      logo: ''
    }))
  }

  watch(key: string, callback: DatafeedWatchCallback) {
    if (!this._ws) return
    this._ws?.send(JSON.stringify({
      action: 'watch', key: key
    }))
    this.listens[key] = callback
  }

  subscribe(symbol: SymbolInfo, callback: DatafeedWatchCallback): void {
    if (this._prevSymbol && this._prevSymbol.ticker === symbol.ticker) return
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
    let host = get(site).apiHost;
    if(!host){
      host = location.host;
    }else{
      host = host.replace(/^https?:\/\//, '');
    }
    let ws_url = `${protocol}://${host}/api`
    this._prevSymbol = symbol
    this._ws?.close()
    this._ws = new WebSocket(`${ws_url}/ws/ohlcv`)
    this._ws.onopen = () => {
      this._ws?.send(JSON.stringify({
        action: 'subscribe',
        exchange: symbol.exchange,
        symbol: symbol.ticker
      }))
    }
    let last_bar: BarArr | null = null;
    this._ws.onmessage = event => {
      const result = JSON.parse(event.data)
      const action = result.a as string
      if (action == 'subscribe' && result.series && result.series.length) {
        result.bars = result.series.map(seriesToBar)
        const first = result.bars[0] as BarArr
        if (last_bar && first[0] == last_bar[0]) {
          // 如果和上一个推送的bar时间戳相同，则认为是其更新，减去上一个的volume，避免调用方错误累加
          first[5] -= last_bar[5]
        }
        last_bar = result.bars[result.bars.length - 1]
        callback(result)
      } else {
        const cb = this.listens[action];
        if (cb) {
          cb(result)
        } else {
          console.error('unknown msg:', event)
        }
      }
    }
  }

  unsubscribe(): void {
    if(!this._ws || !this._prevSymbol)return
    let symbol = this._prevSymbol!
    this._ws?.send(JSON.stringify({
      action: 'unsubscribe',
      exchange: symbol.exchange,
      symbol: symbol.ticker
    }))
  }

}
