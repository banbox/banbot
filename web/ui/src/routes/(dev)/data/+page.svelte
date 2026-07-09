<script lang="ts">
  import { onMount } from 'svelte';
  import {getApi} from '$lib/netio'
  import type { ExSymbol } from '$lib/dev/common';
  import * as m from '$lib/paraglide/messages.js';
  import DrawerDataTools from '$lib/dev/DrawerDataTools.svelte';
  import { exchanges, getMarkets } from '$lib/common';
  import {curTZ, fmtDateStr} from '$lib/dateutil';
  import { pagination } from '$lib/Snippets.svelte';
  import {localizeHref} from "$lib/paraglide/runtime";

  // 状态变量
  let symbols: ExSymbol[] = $state([]);
  let dataSources: DataSourceStatus[] = $state([]);
  let totalCount = $state(0);
  let currentPage = $state(1);
  let pageSize = $state(20);

  let marketOptions = getMarkets();

  // 工具抽屉状态
  let isDrawerOpen = $state(false);

  // 筛选条件
  let selectedExchange = $state('');
  let selectedMarket = $state('');
  let symbolFilter = $state('');
  let lastId = $state(0);

  type OpStatus = {
    state: string;
    error?: string;
    at_ms?: number;
    sid?: number;
    timeframe?: string;
    start_ms?: number;
    end_ms?: number;
    rows?: number;
    subs?: number;
  }

  type DataSourceStatus = {
    name: string;
    health: string;
    registered_at_ms: number;
    register_source: string;
    version?: string;
    type: string;
    timeframe: string;
    table: string;
    duplicate_registrations?: number;
    last_error?: string;
    last_backfill: OpStatus;
    subscription: OpStatus;
  }

  // 获取数据
  async function fetchSymbols() {
    const rsp = await getApi(`/dev/symbols`, {
      exchange: selectedExchange,
      market: selectedMarket,
      symbol: symbolFilter,
      limit: pageSize,
      after_id: lastId,
    });
    symbols = rsp.data;
    totalCount = rsp.total;
    selectedExchange = rsp.exchange || "";
    selectedMarket = rsp.market || "";
  }

  async function fetchDataSources() {
    const rsp = await getApi('/kline/data_sources');
    dataSources = rsp.data ?? [];
  }

  function badgeClass(state: string) {
    if (state === 'ok' || state === 'subscribed') return 'badge-success';
    if (state === 'error') return 'badge-error';
    if (state === 'starting' || state === 'subscribing') return 'badge-warning';
    return 'badge-neutral';
  }

  function opText(op: OpStatus) {
    if (!op || !op.state || op.state === 'idle') return 'idle';
    const bits = [op.state];
    if (op.sid) bits.push(`sid ${op.sid}`);
    if (op.timeframe) bits.push(op.timeframe);
    if (op.rows) bits.push(`${op.rows} rows`);
    if (op.subs) bits.push(`${op.subs} subs`);
    if (op.error) bits.push(op.error);
    return bits.join(' · ');
  }

  // 应用筛选
  function applyFilters() {
      currentPage = 1;
      lastId = 0;
      console.log('applyFilters');
      fetchSymbols();
  }

  // 监听页面变化
  $effect(() => {
    if (currentPage > 1) {
      setTimeout(() => {
        lastId = symbols[symbols.length - 1]?.id || 0;
        fetchSymbols();
      }, 10);
    }
  });

  function toggleDrawer() {
    isDrawerOpen = !isDrawerOpen;
  }
  onMount(() => {
    fetchDataSources()
    fetchSymbols()
  });
</script>

<div class="container mx-auto p-4">
    <div class="mb-6">
        <div class="flex items-center justify-between mb-2">
            <h2 class="text-base font-semibold">Data Sources</h2>
            <button class="btn btn-xs btn-outline" onclick={fetchDataSources}>Refresh</button>
        </div>
        <div class="overflow-x-auto w-full">
            <table class="table table-sm w-full">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Health</th>
                        <th>Version</th>
                        <th>TF</th>
                        <th>Table</th>
                        <th>Last Backfill</th>
                        <th>Subscription</th>
                        <th>Registered</th>
                    </tr>
                </thead>
                <tbody>
                    {#if dataSources.length === 0}
                        <tr><td colspan="8" class="text-base-content/60">No custom data sources registered.</td></tr>
                    {:else}
                        {#each dataSources as src}
                            <tr class="hover:bg-base-300">
                                <td>
                                    <div class="font-medium">{src.name}</div>
                                    <div class="text-xs text-base-content/60">{src.type}</div>
                                    {#if src.duplicate_registrations}
                                        <div class="text-xs text-warning">duplicate: {src.duplicate_registrations}</div>
                                    {/if}
                                </td>
                                <td><span class="badge badge-sm {badgeClass(src.health)}">{src.health}</span></td>
                                <td>{src.version || '-'}</td>
                                <td>{src.timeframe}</td>
                                <td>{src.table}</td>
                                <td title={src.last_backfill?.error || ''}>
                                    <div>{opText(src.last_backfill)}</div>
                                    <div class="text-xs text-base-content/60">{fmtDateStr(src.last_backfill?.at_ms ?? 0)}</div>
                                </td>
                                <td title={src.subscription?.error || ''}>
                                    <div>{opText(src.subscription)}</div>
                                    <div class="text-xs text-base-content/60">{fmtDateStr(src.subscription?.at_ms ?? 0)}</div>
                                </td>
                                <td title={src.register_source}>
                                    {fmtDateStr(src.registered_at_ms)}
                                </td>
                            </tr>
                        {/each}
                    {/if}
                </tbody>
            </table>
        </div>
    </div>

    <div class="flex justify-between items-center mb-4">
        <div class="flex gap-4">
            <select
                class="select w-full"
                bind:value={selectedExchange}
                onchange={applyFilters}
            >
                <option value="">{m.all_exchanges()}</option>
                {#each exchanges as exchange}
                    <option value={exchange}>{exchange}</option>
                {/each}
            </select>

            <select
                class="select w-full"
                bind:value={selectedMarket}
                onchange={applyFilters}
            >
                <option value="">{m.all_markets()}</option>
                {#each marketOptions as market}
                    <option value={market.value}>{market.title}</option>
                {/each}
            </select>

            <input
                type="text"
                placeholder={m.search_symbol()}
                class="input w-full max-w-lg"
                bind:value={symbolFilter}
                oninput={applyFilters}
            />
        </div>

        <div class="flex items-center gap-2">
            <a href={localizeHref("/kline")} target="_blank" class="btn btn-primary btn-outline m-1">{m.kline()}</a>
            <button class="btn btn-primary m-1" onclick={toggleDrawer}>{m.tools()}</button>
        </div>
    </div>

    <div class="overflow-x-auto w-full" style="min-height: 24rem;">
        <table class="table table-zebra w-full">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>{m.exchange()}</th>
                    <th>{m.exg_real()}</th>
                    <th>{m.market()}</th>
                    <th>{m.symbol()}</th>
                    <th>{m.combined()}</th>
                    <th>{m.list_time()}({curTZ()})</th>
                    <th>{m.delist_time()}({curTZ()})</th>
                    <th>-</th>
                </tr>
            </thead>
            <tbody>
                {#each symbols as symbol}
                    <tr class="hover:bg-base-300">
                        <td>{symbol.id}</td>
                        <td>{symbol.exchange}</td>
                        <td>{symbol.exg_real}</td>
                        <td>{symbol.market}</td>
                        <td>{symbol.symbol}</td>
                        <td>{symbol.combined ? m.yes() : ''}</td>
                        <td>{fmtDateStr(symbol.list_ms)}</td>
                        <td>{fmtDateStr(symbol.delist_ms)}</td>
                        <td>
                            <a href={localizeHref(`/data/item?id=${symbol.id}`)} class="btn btn-xs btn-info btn-outline">{m.details()}</a>
                        </td>
                    </tr>
                {/each}
            </tbody>
        </table>
    </div>

    {@render pagination(totalCount, pageSize, currentPage, i => {currentPage = i}, i => {pageSize = i})}
</div>

<DrawerDataTools bind:show={isDrawerOpen} />
