<script lang="ts">
  import { page } from '$app/state';
  import { onMount } from 'svelte';
  import { getApi } from '$lib/netio';
  import type { ExSymbol } from '$lib/dev/common';
  import * as m from '$lib/paraglide/messages.js';
  import { alerts } from "$lib/stores/alerts";
  import { fmtDurationDays, fmtDateStr, TFToSecs, toUTCStamp, curTZ } from '$lib/dateutil';
  import { makePeriod } from '$lib/kline/coms';
  import Icon from '$lib/Icon.svelte';
  import Icon2 from '$lib/kline/Icon.svelte';
  import DrawerDataTools from '$lib/dev/DrawerDataTools.svelte';
  import { pagination } from '$lib/Snippets.svelte';

  let symbol = $state<ExSymbol | null>(null);
  let sranges = $state<any[]>([]);
  let adjFactors = $state<any[]>([]);
  let activeTab = $state('sranges');
  let id = $state('');

  // 过滤条件
  let timeframe = $state('1d');
  let startDate = $state('');
  let endDate = $state('');
  let currentPage = $state(1);
  let pageSize = $state(20);

  let isDrawerOpen = $state(false);

  // 数据列表
  let dataList = $state<any[]>([]);
  let total = $state(0);

  onMount(() => {
    id = page.url.searchParams.get('id') || '';
    loadSymbolInfo();
  });

  async function loadSymbolInfo() {
    const rsp = await getApi('/dev/symbol_info', { id });
    if(rsp.code != 200) {
      alerts.error(rsp.msg || 'load symbol info failed');
      return;
    }
    symbol = rsp.symbol;
    sranges = rsp.sranges || [];
    adjFactors = rsp.adjFactors;
  }

  async function loadGaps() {
    if(!timeframe) {
      alerts.error(m.timeframe() + " is required");
      return;
    }
    const rsp = await getApi('/dev/symbol_gaps', {
      id,
      tf: timeframe,
      start: startDate ? toUTCStamp(startDate) : 0,
      end: endDate ? toUTCStamp(endDate) : 0,
      offset: (currentPage - 1) * pageSize,
      limit: pageSize,
    });
    if(rsp.code != 200) {
      alerts.error(rsp.msg || 'load gaps failed');
      return;
    }
    dataList = rsp.data;
    total = rsp.total;
  }

  async function loadData(start: number = 0, end: number = 0) {
    if(!timeframe) {
      alerts.error(m.timeframe() + " is required");
      return;
    }
    if(!start && startDate) {
      start = toUTCStamp(startDate);
    }
    if(!end && endDate) {
      end = toUTCStamp(endDate);
    }
    const rsp = await getApi('/dev/symbol_data', {
      id, tf: timeframe, start, end, limit: pageSize,
    });
    if(rsp.code != 200) {
      alerts.error(rsp.msg || 'load data failed');
      return;
    }
    dataList = rsp.data;
  }

  $effect(() => {
    if(activeTab === 'gaps') {
      setTimeout(loadGaps, 0);
    } else if(activeTab === 'data') {
      setTimeout(loadData, 0);
    }
  });

  function handlePageChange(newPage: number) {
    const chg = newPage - currentPage;
    currentPage = newPage;
    if(activeTab === 'gaps') {
      loadGaps();
    } else if(activeTab === 'data') {
      let start = 0;
      let end = 0;
      if(dataList.length > 0) {
        if(chg > 0) {
          start = dataList[dataList.length - 1][0] + TFToSecs(timeframe) * 1000;
        } else if(chg < 0) {
          end = dataList[0][0];
        }
      }
      loadData(start, end);
    }
  }

  function handlePageSizeChange(newSize: number) {
    pageSize = newSize;
    currentPage = 1;
    if(activeTab === 'gaps') {
      loadGaps();
    } else if(activeTab === 'data') {
      loadData();
    }
  }

  function setMenuItem(item: string) {
    if((item === 'data' || item === 'gaps') && !timeframe) {
      timeframe = getTimeframes()[0] || '1d';
    }
    currentPage = 1;
    total = 0;
    activeTab = item;
  }

  // 获取所有可用的时间周期
  function getTimeframes() {
    const tfs = new Set<string>();
    sranges
      .filter(r => r?.table?.startsWith('kline_') && r?.timeframe)
      .forEach(r => tfs.add(r.timeframe));
    return Array.from(tfs).sort((a, b) => {
      return makePeriod(a).secs - makePeriod(b).secs;
    });
  }

  // 过滤复权因子
  function filterAdjFactors() {
    let filtered = [...adjFactors];
    if(startDate) {
      const start = toUTCStamp(startDate);
      filtered = filtered.filter(f => f.time >= start);
    }
    if(endDate) {
      const end = toUTCStamp(endDate);
      filtered = filtered.filter(f => f.time <= end);
    }
    return filtered;
  }
</script>

<div class="container mx-auto max-w-[1500px] px-4 py-6">
  <!-- 顶部品种信息 -->
  {#if symbol}
    <div class="flex justify-between items-center mb-6">
      <h1 class="text-2xl font-bold">{symbol.symbol} - {symbol.market} - {symbol.exchange}: {id}</h1>
      <button class="btn btn-primary m-1" onclick={() => isDrawerOpen = true}>{m.tools()}</button>
    </div>
  {/if}

  <!-- 主体内容 -->
  <div class="flex gap-6">
    <!-- 左侧菜单 -->
    <div class="w-[15%]">
      <ul class="menu w-full bg-base-200 rounded-box">
        <li>
          <button class:menu-active={activeTab === 'sranges'} onclick={() => setMenuItem('sranges')}>
            <Icon name="info" />
            {m.kline_range()}
          </button>
        </li>
        {#if symbol?.combined}
          <li>
            <button class:menu-active={activeTab === 'adjs'} onclick={() => setMenuItem('adjs')}>
              <Icon2 name="indicator" />
              {m.adj_factor()}
            </button>
          </li>
        {/if}
        <li>
          <button class:menu-active={activeTab === 'gaps'} onclick={() => setMenuItem('gaps')}>
            <Icon name="link-slash" />
            {m.gaps()}
          </button>
        </li>
        <li>
          <button class:menu-active={activeTab === 'data'} onclick={() => setMenuItem('data')}>
            <Icon name="chart-bar" />
            {m.data()}
          </button>
        </li>
      </ul>
    </div>

    <!-- 右侧内容 -->
    <div class="flex-1">
      {#if activeTab === 'sranges'}
        <div class="overflow-x-auto">
          <table class="table">
            <thead>
              <tr>
                <th>{m.timeframe()}</th>
                <th>{m.start_time()}({curTZ()})</th>
                <th>{m.end_time()}({curTZ()})</th>
                <th>{m.duration()}</th>
              </tr>
            </thead>
            <tbody>
              {#each sranges.filter(r => r?.table?.startsWith('kline_') && r?.has_data) as info}
                <tr>
                  <td>{info.timeframe}</td>
                  <td>{fmtDateStr(info.start_ms)}</td>
                  <td>{fmtDateStr(info.stop_ms)}</td>
                  <td>{fmtDurationDays((info.stop_ms - info.start_ms)/1000)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>

      {:else if activeTab === 'adjs'}
        <!-- 复权因子过滤器 -->
        <div class="flex gap-4 mb-4">
          <input type="date" class="input" bind:value={startDate} />
          <input type="date" class="input" bind:value={endDate} />
        </div>

        <!-- 复权因子列表 -->
        <div class="overflow-x-auto">
          <table class="table">
            <thead>
              <tr>
                <th>{m.time()}({curTZ()})</th>
                <th>{m.adj_factor()}</th>
              </tr>
            </thead>
            <tbody>
              {#each filterAdjFactors() as adj}
                <tr>
                  <td>{fmtDateStr(adj.time)}</td>
                  <td>{adj.factor}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>

      {:else if activeTab === 'gaps' || activeTab === 'data'}
        <!-- 过滤器 -->
        <div class="flex gap-4 mb-4">
          <select class="select" bind:value={timeframe}>
            <option value="">{m.timeframe()}</option>
            {#each getTimeframes() as tf}
              <option value={tf}>{tf}</option>
            {/each}
          </select>
          <input type="date" class="input" bind:value={startDate} />
          <input type="date" class="input" bind:value={endDate} />
          <button class="btn btn-primary" onclick={() => {
            if(activeTab === 'gaps') loadGaps();
            else loadData();
          }}>
            {m.query()}
          </button>
        </div>

        <!-- 数据列表 -->
        <div class="overflow-x-auto">
          <table class="table">
            <thead>
              <tr>
                {#if activeTab === 'gaps'}
                  <th>{m.timeframe()}</th>
                  <th>{m.start_time()}({curTZ()})</th>
                  <th>{m.end_time()}({curTZ()})</th>
                  <th>Number</th>
                  <th>{m.indeed_empty()}</th>
                {:else}
                  <th>{m.time()}({curTZ()})</th>
                  <th>{m.time()}</th>
                  <th>{m.kopen()}</th>
                  <th>{m.khigh()}</th>
                  <th>{m.klow()}</th>
                  <th>{m.kclose()}</th>
                  <th>{m.kvolume()}</th>
                {/if}
              </tr>
            </thead>
            <tbody>
              {#each dataList as item}
                <tr>
                  {#if activeTab === 'gaps'}
                    <td>{item.timeframe}</td>
                    <td>{fmtDateStr(item.start_ms)}</td>
                    <td>{fmtDateStr(item.stop_ms)}</td>
                    <td>{Math.round((item.stop_ms - item.start_ms)/1000/TFToSecs(item.timeframe))}</td>
                    <td>{!item.has_data}</td>
                  {:else}
                    <td>{fmtDateStr(item[0])}</td>
                    <td>{item[0]}</td>
                    <td>{item[1]}</td>
                    <td>{item[2]}</td>
                    <td>{item[3]}</td>
                    <td>{item[4]}</td>
                    <td>{item[5]}</td>
                  {/if}
                </tr>
              {/each}
            </tbody>
          </table>
        </div>

        <!-- 分页器 -->
        {@render pagination(total, pageSize, currentPage, handlePageChange, handlePageSizeChange)}
      {/if}
    </div>
  </div>
</div>

<DrawerDataTools bind:show={isDrawerOpen} />
