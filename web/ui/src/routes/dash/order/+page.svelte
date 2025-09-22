<script lang="ts">
  import { onMount } from 'svelte';
  import * as m from '$lib/paraglide/messages';
  import { getAccApi, postAccApi } from '$lib/netio';
  import { alerts } from '$lib/stores/alerts';
  import {modals} from '$lib/stores/modals';
  import Modal from '$lib/kline/Modal.svelte';
  import { fmtDateStr, toUTCStamp, curTZ } from '$lib/dateutil';
  import { InOutOrderDetail, ExgOrderDetail, ExgPositionDetail } from '$lib/order';
  import type { InOutOrder, BanExgOrder, Position } from '$lib/order';
  import {getFirstValid} from "$lib/common";
  import { pagination } from '$lib/Snippets.svelte';
  import { acc } from '$lib/dash/store';

  let tabName = $state('bot'); // bot/exchange/position
  let banodList = $state<InOutOrder[]>([]);
  let exgodList = $state<BanExgOrder[]>([]);
  let exgposList = $state<Position[]>([]);
  let pageSize = $state(15);
  let limitSize = $state(30);
  let currentPage = $state(1);
  let showOdDetail = $state(false);
  let showExOdDetail = $state(false);
  let showExgPosDetail = $state(false);
  let showOpenOrder = $state(false);
  let showCloseExg = $state(false);
  let openingOd = $state(false);
  let closingExgPos = $state(false);
  let exFilter = $state('');
  let selectedOrder = $state<InOutOrder|null>(null);
  let selectedExgOrder = $state<BanExgOrder|null>(null);
  let selectedExgPos = $state<Position|null>(null);
  let pageFirst = $state(0);
  let pageLast = $state(0);

  let search = $state({
    status: '',
    symbols: '',
    dirt: '',
    strategy: '',
    startMs: '',
    stopMs: ''
  });

  let openOd = $state({
    pair: '',
    side: 'long',
    orderType: 'market',
    price: undefined,
    stopLossPrice: undefined,
    enterCost: undefined,
    enterTag: undefined,
    leverage: undefined,
    strategy: undefined
  });

  let closeExgPos = $state({
    symbol: '',
    side: '',
    initAmount: 0,
    amount: 0,
    percent: 100,
    orderType: 'market',
    price: undefined
  });

  function getQuoteCode(symbol: string) {
    const arr = symbol.split('/');
    if (arr.length !== 2) return '';
    let quote = arr[1];
    quote = quote.split('.')[0];
    return quote.split(':')[0];
  }

  async function loadData(page: number) {
    let afterDirt = -1;
    if (currentPage > page){
      afterDirt = 1;
    }
    currentPage = page;
    const data: Record<string, any> = {...search};
    data.startMs = toUTCStamp(data.startMs);
    data.stopMs = toUTCStamp(data.stopMs);
    data.source = tabName;
    
    if (tabName === 'bot') {
      data.limit = pageSize;
      if (page > 1) {
        if(afterDirt > 0){
          data.afterId = pageFirst * afterDirt;
        }else{
          data.afterId = pageLast * afterDirt;
        }
      }
    } else if (tabName === 'exchange') {
      // exchange和position必须传symbols
      if (!data.symbols) {
        alerts.error(m.symbol_required());
        return;
      }
      data.limit = limitSize;
    }

    const rsp = await getAccApi('/orders', data);
    let res = rsp.data ?? [];
    exFilter = '';

    if (tabName === 'bot') {
      banodList = res;
      if (res.length > 0) {
        pageLast = res[res.length - 1].id;
        pageFirst = res[0].id;
      }
    } else if (tabName === 'exchange') {
      if (search.status) {
        const oldNum = res.length;
        res = res.filter((od: any) => od.filled);
        exFilter = `已筛选：${res.length}/${oldNum}`;
      }
      exgodList = res;
    } else {
      exgposList = res;
    }
  }

  async function closeOrder(orderId: string) {
    if (!await modals.confirm(m.confirm_close_position())) return;
    
    const rsp = await postAccApi('/exit_order', {orderId});
    if(rsp.code !== 200){
      console.error('close order failed', rsp);
      alerts.error(rsp.msg ?? m.close_order_failed());
      return;
    }
    
    const closeNum = rsp.closeNum ?? 0;
    const failNum = rsp.failNum ?? 0;
    
    let message = m.closed_positions({close: closeNum, fail: failNum});
    if (rsp.errMsg) {
      message += `\n${rsp.errMsg}`;
    }
    alerts.addAlert(failNum > 0 ? 'warning' : 'success', message);
    await loadData(1);
  }

  function onPosAmountChg(percent: number) {
    closeExgPos.amount = parseFloat((closeExgPos.initAmount * percent / 100).toFixed(5));
  }

  function showOrder(idx: number) {
    if (tabName === 'bot') {
      selectedOrder = banodList[idx];
      showOdDetail = true;
    } else if (tabName === 'exchange') {
      selectedExgOrder = exgodList[idx];
      showExOdDetail = true;
    } else if (tabName === 'position') {
      selectedExgPos = exgposList[idx];
      showExgPosDetail = true;
    }
  }

  function clickCloseExgPos(index: number) {
    const item = exgposList[index];
    closeExgPos = {
      symbol: item.symbol,
      amount: item.contracts,
      side: item.side,
      initAmount: item.contracts,
      percent: 100,
      orderType: 'market',
      price: undefined
    };
    showCloseExg = true;
  }

  async function closeExgPosition(all: boolean = false) {
    let data = {...closeExgPos};
    if (all) {
      if (!await modals.confirm(m.confirm_close_all_positions())) return;
      data.symbol = 'all';
    }

    const rsp = await postAccApi('/close_exg_pos', data);

    const closeNum = rsp.close_num ?? 0;
    const doneNum = rsp.done_num ?? 0;
    let message = m.closed_orders({num: closeNum});
    if (doneNum) {
      message += m.filled_orders({num: doneNum});
    }
    showCloseExg = false;
    alerts.success(message);
    await loadData(1);
  }

  async function doOpenOrder() {
    openingOd = true;
    const rsp = await postAccApi('/forceenter', openOd);
    openingOd = false;
    
    if (rsp.code !== 200) {
      alerts.error(rsp.msg ?? m.open_order_failed());
    } else {
      alerts.success(m.open_order_success());
      showOpenOrder = false;
    }
  }

  async function clickCalcProfits() {
    const rsp = await postAccApi('/calc_profits', {});
    
    if (rsp.code !== 200) {
      alerts.error(m.update_failed({msg: rsp.msg ?? ''}));
    } else {
      alerts.success(m.updated_orders({num: rsp.num ?? 0}));
      await loadData(currentPage);
    }
  }

  onMount(() => {
    loadData(1);
  });
</script>

<!-- 主体内容 -->
<div class="bg-base-100 p-4 min-h-[600px] border border-base-200">
  <div class="flex flex-col gap-4">
    <!-- Tab Menu -->
    <div class="flex justify-between items-center">
      <div class="tabs tabs-boxed bg-base-200/50 p-1 rounded-md">
        <button
          class="tab tab-sm transition-all duration-200 px-4 {tabName === 'bot' ? 'tab-active bg-primary/90 text-primary-content' : 'hover:bg-base-300'}"
          onclick={() => tabName = 'bot'}
        >
          {m.bot_orders()}
        </button>
        {#if $acc.env !== 'dry_run'}
          <button
            class="tab tab-sm transition-all duration-200 px-4 {tabName === 'exchange' ? 'tab-active bg-primary/90 text-primary-content' : 'hover:bg-base-300'}"
            onclick={() => tabName = 'exchange'}
          >
            {m.exchange_orders()}
          </button>
          <button
            class="tab tab-sm transition-all duration-200 px-4 {tabName === 'position' ? 'tab-active bg-primary/90 text-primary-content' : 'hover:bg-base-300'}"
            onclick={() => {
              tabName = 'position';
              loadData(1);
            }}
          >
            {m.exchange_positions()}
          </button>
        {/if}
      </div>

      <div class="flex gap-2 items-center">
        <!-- 添加每页大小控制 -->
        <select 
          class="select select-sm focus:outline-none w-36 text-sm"
          bind:value={pageSize}
          onchange={() => loadData(1)}
        >
          <option value={10}>10 / {m.page()}</option>
          <option value={15}>15 / {m.page()}</option>
          <option value={20}>20 / {m.page()}</option>
          <option value={30}>30 / {m.page()}</option>
          <option value={50}>50 / {m.page()}</option>
        </select>

        <!-- 现有的操作按钮 -->
        {#if tabName === 'bot'}
          <button class="btn btn-sm bg-primary/90 hover:bg-primary text-primary-content border-none" onclick={clickCalcProfits}>
            {m.update_profits()}
          </button>
          <button class="btn btn-sm bg-primary/90 hover:bg-primary text-primary-content border-none" onclick={() => showOpenOrder = true}>
            {m.open_order()}
          </button>
          <button class="btn btn-sm bg-error/90 hover:bg-error text-error-content border-none" onclick={() => closeOrder('all')}>
            {m.close_all()}
          </button>
        {:else if tabName === 'position'}
          <button class="btn btn-sm bg-primary/90 hover:bg-primary text-primary-content border-none" onclick={() => loadData(1)}>
            {m.refresh()}
          </button>
          <button class="btn btn-sm bg-error/90 hover:bg-error text-error-content border-none" onclick={() => closeExgPosition(true)}>
            {m.close_all()}
          </button>
        {/if}
      </div>
    </div>

    <!-- Search Form -->
    {#if tabName !== 'position'}
      <div class="flex flex-wrap gap-4">
        {#if tabName === 'bot'}
          <select class="select select-sm focus:outline-none text-sm w-24" bind:value={search.status}>
            <option value="">{m.any_status()}</option>
            <option value="wait">{m.pos_empty()}</option>
            <option value="open">{m.pos_opened()}</option>
            <option value="his">{m.closed()}</option>
          </select>
        {/if}
        <select class="select select-sm focus:outline-none text-sm w-24" bind:value={search.dirt}>
          <option value="">{m.any_dirt()}</option>
          <option value="long">{m.long()}</option>
          <option value="short">{m.short()}</option>
        </select>

        {#if tabName === 'bot'}
        <input type="text" class="input input-sm focus:outline-none text-sm font-mono w-36" placeholder={m.strategy()} bind:value={search.strategy}/>
        {/if}

        <input type="text" class="input input-sm focus:outline-none text-sm font-mono w-36" placeholder={m.symbol()} bind:value={search.symbols}
          required={tabName !== 'bot'} />

        <input type="text" class="input input-sm focus:outline-none text-sm font-mono w-48" placeholder="20231012" bind:value={search.startMs} />

        {#if tabName === 'bot'}
          <input type="text" class="input input-sm focus:outline-none text-sm font-mono w-48" placeholder="20231012" bind:value={search.stopMs} />
        {:else}
          <input type="number" class="input input-sm focus:outline-none text-sm font-mono w-24" placeholder="limit" bind:value={limitSize} />
        {/if}

        <button 
          class="btn btn-sm bg-primary/90 hover:bg-primary text-primary-content border-none" 
          onclick={() => {
            if (tabName !== 'bot' && !search.symbols) {
              alerts.error(m.symbol_required());
              return;
            }
            loadData(1);
          }}
        >
          {m.search()}
        </button>

        {#if exFilter}
          <span class="text-info">{exFilter}</span>
        {/if}
      </div>
    {/if}

    <!-- Data Tables -->
    <div class="overflow-x-auto">
      {#if tabName === 'bot'}
        <table class="table table-sm">
          <thead>
            <tr>
              <th class="bg-base-200/30 font-medium text-sm">ID</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.pair()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.basic()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.enter_time()}({curTZ()})</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.enter_tag()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.enter_price()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.amount()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.exit_time()}({curTZ()})</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.exit_tag()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.profit()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.actions()}</th>
            </tr>
          </thead>
          <tbody>
            {#each banodList as order, i}
              <tr class="hover:bg-base-200/50 transition-colors">
                <td class="text-sm font-mono">{order.id}</td>
                <td class="text-sm font-mono">{order.symbol}</td>
                <td>{order.timeframe}/{order.short ? m.short() : m.long()}/{order.leverage}</td>
                <td class="text-sm">{fmtDateStr(getFirstValid([order.enter?.update_at, order.enter?.create_at, order.enter_at]))}</td>
                <td>{order.strategy}:{order.enter_tag}</td>
                <td>{getFirstValid([order.enter?.average, order.enter?.price, order.init_price]).toFixed(7)}</td>
                <td>{getFirstValid([order.enter?.amount, 0]).toFixed(7)}</td>
                <td class="text-sm">{fmtDateStr(getFirstValid([order.exit?.update_at, order.exit?.create_at, order.exit_at]))}</td>
                <td>{order.exit_tag}</td>
                <td>{order.profit.toFixed(5)}</td>
                <td>
                  <div class="flex gap-2">
                    <button class="btn btn-xs bg-primary/90 hover:bg-primary text-primary-content border-none" onclick={() => showOrder(i)}>
                      {m.view()}
                    </button>
                    {#if order.status !== 4}
                      <button class="btn btn-xs bg-error/90 hover:bg-error text-error-content border-none" onclick={() => closeOrder(order.id.toString())}>
                        {m.close_order()}
                      </button>
                    {/if}
                  </div>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      {:else if tabName === 'exchange' && $acc.env !== 'dry_run'}
        <table class="table table-sm">
          <thead>
            <tr>
              <th class="bg-base-200/30 font-medium text-sm">{m.order_id()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.client_id()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.pair()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.time()}({curTZ()})</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.side()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.position_type()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.price()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.amount()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.status()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.actions()}</th>
            </tr>
          </thead>
          <tbody>
            {#each exgodList as order, i}
              <tr class="hover:bg-base-200/50 transition-colors">
                <td>{order.id}</td>
                <td>{order.clientOrderId}</td>
                <td>{order.symbol}</td>
                <td>{fmtDateStr(order.timestamp)}</td>
                <td>{order.positionSide.toLowerCase() === 'long' ? m.long() : m.short()}</td>
                <td>{order.reduceOnly ? m.close_position() : m.open_position()}</td>
                <td>{order.average || order.price || '-'}</td>
                <td>{order.amount}</td>
                <td>{order.status}</td>
                <td>
                  <button class="btn btn-xs bg-primary/90 hover:bg-primary text-primary-content border-none" onclick={() => showOrder(i)}>
                    {m.view()}
                  </button>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      {:else if tabName === 'position' && $acc.env !== 'dry_run'}
        <table class="table table-sm">
          <thead>
            <tr>
              <th class="bg-base-200/30 font-medium text-sm">{m.pair()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.time()}({curTZ()})</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.side()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.entry_price()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.amount()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.notional()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.leverage()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.unrealized_pnl()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.margin_mode()}</th>
              <th class="bg-base-200/30 font-medium text-sm">{m.actions()}</th>
            </tr>
          </thead>
          <tbody>
            {#each exgposList as pos, i}
              <tr class="hover:bg-base-200/50 transition-colors">
                <td>{pos.symbol}</td>
                <td>{fmtDateStr(pos.timestamp)}</td>
                <td>{pos.side === 'long' ? m.long() : m.short()}</td>
                <td>{pos.entryPrice?.toFixed(4)}</td>
                <td>{pos.contracts?.toFixed(4)}</td>
                <td>{pos.notional?.toFixed(4)}</td>
                <td>{pos.leverage}x</td>
                <td class={pos.unrealizedPnl >= 0 ? 'text-success' : 'text-error'}>
                  {pos.unrealizedPnl?.toFixed(4)}
                </td>
                <td>{pos.marginMode}</td>
                <td>
                  <div class="flex gap-2">
                    <button class="btn btn-xs bg-primary/90 hover:bg-primary text-primary-content border-none" onclick={() => showOrder(i)}>
                      {m.view()}
                    </button>
                    <button class="btn btn-xs bg-error/90 hover:bg-error text-error-content border-none" onclick={() => clickCloseExgPos(i)}>
                      {m.close_position()}
                    </button>
                  </div>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </div>

    <!-- 修改分页控制 -->
    {#if tabName === 'bot'}
      {@render pagination(0, pageSize, currentPage, loadData, pSize => {pageSize = pSize })}
    {/if}
  </div>
</div>

<!-- Modals -->
<InOutOrderDetail bind:show={showOdDetail} order={selectedOrder} editable={(selectedOrder?.status ?? 0) < 4 } />
{#if $acc.env !== 'dry_run'}
  <ExgOrderDetail bind:show={showExOdDetail} order={selectedExgOrder} />
  <ExgPositionDetail bind:show={showExgPosDetail} pos={selectedExgPos} />
{/if}

<Modal title={m.open_order()} bind:show={showOpenOrder} width={800} buttons={[]}>
  <div class="p-4">
    <div class="grid grid-cols-2 gap-4">
      <fieldset class="fieldset">
        <label class="label" for="pair">{m.pair()}</label>
        <input id="pair" type="text" class="input input-sm focus:outline-none text-sm font-mono" placeholder={m.enter_symbol_placeholder()} bind:value={openOd.pair} />
      </fieldset>

      <fieldset class="fieldset">
        <label class="label">{m.side()}</label>
        <div class="flex gap-4 flex-1">
          <label class="label cursor-pointer">
            <input type="radio" class="radio" bind:group={openOd.side} value="long" />
            <span class="ml-2">{m.long()}</span>
          </label>
          <label class="label cursor-pointer">
            <input type="radio" class="radio" bind:group={openOd.side} value="short" />
            <span class="ml-2">{m.short()}</span>
          </label>
        </div>
      </fieldset>

      <fieldset class="fieldset col-span-2">
        <label class="label">{m.order_type()}</label>
        <div class="flex gap-4 flex-1">
          <label class="label cursor-pointer">
            <input type="radio" class="radio" bind:group={openOd.orderType} value="market" />
            <span class="ml-2">{m.market_order()}</span>
          </label>
          <label class="label cursor-pointer">
            <input type="radio" class="radio" bind:group={openOd.orderType} value="limit" />
            <span class="ml-2">{m.limit_order()}</span>
          </label>
        </div>
      </fieldset>

      {#if openOd.orderType === 'limit'}
        <fieldset class="fieldset">
          <label class="label" for="price">{m.price()}</label>
          <input id="price" type="number" class="input input-sm focus:outline-none text-sm font-mono" bind:value={openOd.price} step="0.00000001" />
        </fieldset>

        <fieldset class="fieldset">
          <label class="label" for="stopLossPrice">{m.stop_loss_price()}</label>
          <input id="stopLossPrice" type="number" class="input input-sm focus:outline-none text-sm font-mono" bind:value={openOd.stopLossPrice} step="0.00000001" />
        </fieldset>
      {/if}

      <fieldset class="fieldset">
        <label class="label" for="enterCost">{m.enter_cost()}</label>
        <div class="input-group">
          <input id="enterCost" type="number" class="input input-sm focus:outline-none text-sm font-mono" bind:value={openOd.enterCost} step="0.00000001" />
          <span class="input-group-addon">{getQuoteCode(openOd.pair)}</span>
        </div>
      </fieldset>

      <fieldset class="fieldset">
        <label class="label" for="enterTag">{m.enter_tag()}</label>
        <input id="enterTag" type="text" class="input input-sm focus:outline-none text-sm font-mono" bind:value={openOd.enterTag} />
      </fieldset>

      <fieldset class="fieldset">
        <label class="label" for="leverage">{m.leverage()}</label>
        <input id="leverage" type="number" class="input input-sm focus:outline-none text-sm font-mono" bind:value={openOd.leverage} min="1" max="200" step="1" />
      </fieldset>

      <fieldset class="fieldset">
        <label class="label" for="strategy">{m.strategy()}</label>
        <input id="strategy" type="text" class="input input-sm focus:outline-none text-sm font-mono" placeholder={m.enter_strategy_placeholder()} bind:value={openOd.strategy} />
      </fieldset>
    </div>

    <div class="flex justify-end mt-4">
      <button class="btn btn-sm btn-primary border-none" disabled={!openOd.pair || !openOd.enterCost} onclick={doOpenOrder}>
        {#if openingOd}
          <span class="loading loading-spinner"></span>
        {/if}
        {m.save()}
      </button>
    </div>
  </div>
</Modal>

{#if $acc.env !== 'dry_run'}
  <Modal title={m.close_position()} bind:show={showCloseExg} width={800} buttons={[]}>
    <div class="space-y-4 p-4">
      <fieldset class="fieldset">
        <label class="label" for="symbol">{m.pair()}</label>
        <input id="symbol" type="text" class="input input-sm focus:outline-none text-sm font-mono" bind:value={closeExgPos.symbol} disabled />
      </fieldset>

      <fieldset class="fieldset">
        <label class="label" for="amount">{m.close_amount()}</label>
        <input id="amount" type="number" class="input input-sm focus:outline-none text-sm font-mono" bind:value={closeExgPos.amount} step="0.00000001" />
      </fieldset>

      <fieldset class="fieldset">
        <label class="label" for="percent">{closeExgPos.percent}%</label>
        <input id="percent" type="range" min="0" max="100" class="range range-primary range-sm" bind:value={closeExgPos.percent}
          onchange={() => onPosAmountChg(closeExgPos.percent)} />
      </fieldset>

      <fieldset class="fieldset">
        <label class="label">{m.order_type()}</label>
        <div class="flex gap-4 flex-1">
          <label class="label cursor-pointer">
            <input type="radio" class="radio" bind:group={closeExgPos.orderType} value="market" />
            <span class="ml-2">{m.market_order()}</span>
          </label>
          <label class="label cursor-pointer">
            <input type="radio" class="radio" bind:group={closeExgPos.orderType} value="limit" />
            <span class="ml-2">{m.limit_order()}</span>
          </label>
        </div>
      </fieldset>

      {#if closeExgPos.orderType === 'limit'}
        <fieldset class="fieldset">
          <label class="label" for="limitPrice">{m.price()}</label>
          <input id="limitPrice" type="number" class="input input-sm focus:outline-none text-sm font-mono" bind:value={closeExgPos.price} step="0.00000001" />
        </fieldset>
      {/if}

      <div class="flex justify-end mt-4">
        <button
          class="btn btn-sm bg-primary/90 hover:bg-primary text-primary-content border-none"
          disabled={!closeExgPos.symbol || !closeExgPos.amount}
          onclick={() => closeExgPosition(false)}
        >
          {#if closingExgPos}
            <span class="loading loading-spinner"></span>
          {/if}
          {m.save()}
        </button>
      </div>
    </div>
  </Modal>
{/if}
