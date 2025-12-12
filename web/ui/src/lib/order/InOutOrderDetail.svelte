<script lang="ts">
  import { fmtDateStrTZ } from '$lib/dateutil';
  import * as m from '$lib/paraglide/messages';
  import type { InOutOrder } from '$lib/order/types';
  import Modal from '../kline/Modal.svelte';

  let { 
    show=$bindable(false),
    order=undefined,
    editable=false,
  } = $props();
  
  // 将 order 设置为可响应的状态
  let currentOrder = $state<InOutOrder | null>(null);
  let isEditable = $state(editable);
  
  // 监听 order 变化
  $effect(() => {
    currentOrder = order;
    isEditable = editable;
  });

  // 订单状态数组
  const InOutStatus = [m.no_enter(), m.part_enter(), m.full_enter(), m.part_exit(), m.full_exit(), m.deleted()];

  function getExStatusText(status: number, filled: number): string {
    if(status === 0){
      return m.no_filled();
    }else if (status === 1){
      return m.part_ok();
    }else if (status === 2){
      if (filled && filled > 0){
        return m.filled()
      }
      return m.closed();
    }else{
      return "unknown"
    }
  }

  // 获取状态对应的颜色类
  function getStatusColor(status: number): string {
    switch(status) {
      case 2: return "bg-success/20 text-success border-success/30"; // 完全入场
      case 3: return "bg-success/10 text-success border-success/20"; // 部分出场
      case 4: return "bg-info/20 text-info border-info/30"; // 完全出场
      case 5: return "bg-error/20 text-error border-error/30"; // 删除
      default: return "bg-base-200 text-base-content border-base-300"; // 默认
    }
  }
  
  // 获取盈亏对应的颜色类
  function getProfitColor(value: number | undefined): string {
    if (value === undefined) return "";
    return value > 0 ? "text-success font-medium" : value < 0 ? "text-error font-medium" : "";
  }

  // 复制ID到剪贴板
  function copyToClipboard(text: string) {
    navigator.clipboard.writeText(text);
    // 可以添加一个toast提示，这里略过
  }

  // 格式化百分比
  function formatPercent(value: number | undefined): string {
    if (value === undefined) return "-";
    return (value * 100).toFixed(2) + "%";
  }
</script>

<Modal title={m.order_details()} bind:show={show} width={1200}>
  {#if currentOrder}
  <div class="flex flex-col gap-2 p-1 pr-2">
    <!-- 订单概览卡片 -->
    <div class="bg-base-100 rounded-lg shadow-sm border border-base-200">
      <!-- 卡片头部 -->
      <div class="flex items-center justify-between px-3 py-2 bg-base-200/50 border-b border-base-300">
        <div class="flex items-center gap-2">
          <div class="bg-primary/20 p-1.5 rounded-full">
            <span class="i-lucide-briefcase text-primary text-sm"></span>
          </div>
          <div class="flex flex-row gap-0.5">
            <span class="text-md font-bold">{m.order_summary()}</span>
            <span class="ml-2 flex items-center gap-1 text-xs text-base-content/70">
              <span class="font-mono">{currentOrder.symbol}</span>
              <span>•</span>
              <span>{currentOrder.timeframe}</span>
              <span>•</span>
              <span class={currentOrder.short ? "text-error" : "text-success"}>
                {currentOrder.short ? m.short() : m.long()}
              </span>
            </span>
          </div>
        </div>
        
        <div class="flex items-center gap-2">
          <div class="text-right">
            <span class="text-xs text-base-content/70">{m.leverage()}</span>
            <span class="text-sm font-bold">{currentOrder.leverage}x</span>
          </div>
          
          <div class={`px-2 py-0.5 text-xs rounded-full border ${getStatusColor(currentOrder.status)}`}>
            {InOutStatus[currentOrder.status]}
          </div>
        </div>
      </div>
      
      <!-- 卡片内容 -->
      <div class="p-3 grid grid-cols-1 md:grid-cols-3 gap-3">
        <!-- 基本信息 -->
        <div class="space-y-2">
          <h3 class="text-sm font-semibold flex items-center gap-1 mb-1">
            <span class="i-lucide-info text-primary text-xs"></span>
            {m.basic_info()}
          </h3>
          
          <div class="grid grid-cols-2 gap-x-3 gap-y-1.5 text-xs">
            <div class="space-y-0.5">
              <div class="text-xs text-base-content/70">ID</div>
              <div class="font-mono flex items-center gap-1">
                <span class="truncate">{currentOrder?.id}</span>
                <button onclick={() => currentOrder?.id && copyToClipboard(currentOrder.id.toString())} 
                        class="btn btn-xs btn-ghost btn-square p-0 h-4 min-h-0 w-4" 
                        title={m.copy_id()}>
                  <span class="i-lucide-copy text-base-content/50 text-xs"></span>
                </button>
              </div>
            </div>
            
            <div class="space-y-0.5">
              <div class="text-xs text-base-content/70">{m.task_id()}</div>
              <div class="font-mono truncate">{currentOrder.task_id ?? '-'}</div>
            </div>
            
            <div class="space-y-0.5">
              <div class="text-xs text-base-content/70">{m.strategy()}</div>
              <div class="font-mono truncate text-xs">{currentOrder.strategy}:{currentOrder.stg_ver}</div>
            </div>
            
            <div class="space-y-0.5">
              <div class="text-xs text-base-content/70">{m.init_price()}</div>
              <div class="font-mono">{currentOrder.init_price?.toFixed(7) ?? '-'}</div>
            </div>
          </div>
        </div>
        
        <!-- 价格走势 -->
        <div class="space-y-2">
          <h3 class="text-sm font-semibold flex items-center gap-1 mb-1">
            <span class="i-lucide-trending-up text-primary text-xs"></span>
            {m.price_movement()}
          </h3>
          
          <div class="grid grid-cols-2 gap-2">
            <!-- 入场价格卡片 -->
            <div class="bg-base-200/30 rounded p-1.5 border border-base-300 flex flex-col">
              <span class="text-xs text-base-content/70">{m.enter_price()}</span>
              <span class="text-sm font-medium font-mono">
                {currentOrder.enter?.price?.toFixed(7) ?? '-'}
              </span>
              <span class="text-xs text-base-content/60 text-[10px]">
                {currentOrder.enter_at ? fmtDateStrTZ(currentOrder.enter_at) : '-'}
              </span>
            </div>
            
            <!-- 出场价格卡片 -->
            <div class="bg-base-200/30 rounded p-1.5 border border-base-300 flex flex-col">
              <span class="text-xs text-base-content/70">{m.exit_price()}</span>
              <span class="text-sm font-medium font-mono">
                {currentOrder.exit?.price?.toFixed(7) ?? '-'}
              </span>
              <span class="text-xs text-base-content/60 text-[10px]">
                {currentOrder.exit_at ? fmtDateStrTZ(currentOrder.exit_at) : '-'}
              </span>
            </div>
            
            {#if currentOrder.status < 4 && currentOrder.curPrice}
            <div class="col-span-2 bg-base-200/30 rounded p-1.5 border border-base-300 flex flex-col">
              <span class="text-xs text-base-content/70">{m.cur_price()}</span>
              <span class="text-sm font-bold font-mono">{currentOrder.curPrice?.toFixed(7) ?? '-'}</span>
            </div>
            {/if}
          </div>
        </div>
        
        <!-- 收益指标 -->
        <div class="space-y-2">
          <h3 class="text-sm font-semibold flex items-center gap-1 mb-1">
            <span class="i-lucide-bar-chart-3 text-primary text-xs"></span>
            {m.performance()}
          </h3>
          
          <div class="grid grid-cols-2 gap-2">
            <!-- 盈亏金额 -->
            <div class="bg-base-200/30 rounded p-1.5 border border-base-300 flex flex-col">
              <span class="text-xs text-base-content/70">{m.profit()}</span>
              <span class={`text-sm font-medium font-mono ${getProfitColor(currentOrder.profit)}`}>
                {currentOrder.profit?.toFixed(5) ?? '-'}
              </span>
            </div>
            
            <!-- 盈亏比例 -->
            <div class="bg-base-200/30 rounded p-1.5 border border-base-300 flex flex-col">
              <span class="text-xs text-base-content/70">{m.profit_rate()}</span>
              <span class={`text-sm font-medium font-mono ${getProfitColor(currentOrder.profit_rate)}`}>
                {formatPercent(currentOrder.profit_rate)}
              </span>
            </div>
            
            <!-- 最大盈利率 -->
            <div class="bg-base-200/30 rounded p-1.5 border border-base-300 flex flex-col">
              <span class="text-xs text-base-content/70">{m.max_pft_rate()}</span>
              <span class="text-sm text-success font-medium font-mono">
                {formatPercent(currentOrder.max_pft_rate)}
              </span>
            </div>
            
            <!-- 最大回撤 -->
            <div class="bg-base-200/30 rounded p-1.5 border border-base-300 flex flex-col">
              <span class="text-xs text-base-content/70">{m.max_draw_down()}</span>
              <span class="text-sm text-error font-medium font-mono">
                {currentOrder.max_draw_down?.toFixed(2) ?? '-'}%
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  
    <!-- 入场订单详情 -->
    {#if currentOrder.enter}
    <div class="bg-base-100 rounded-lg shadow-sm border border-base-200">
      <div class="flex items-center justify-between px-3 py-2 bg-base-200/50 border-b border-base-300">
        <div class="flex items-center gap-2">
          <div class="bg-success/20 p-1.5 rounded-full">
            <span class="i-lucide-log-in text-success text-sm"></span>
          </div>
          <div>
            <h2 class="text-sm font-bold">{m.enter_order()}</h2>
            <div class="text-xs text-base-content/70">
              {fmtDateStrTZ(currentOrder.enter_at)}
            </div>
          </div>
        </div>
        
        <div class="badge badge-sm">
          {getExStatusText(currentOrder.enter.status, currentOrder.enter.filled)}
        </div>
      </div>
      
      <div class="p-3 grid grid-cols-2 md:grid-cols-6 gap-x-3 gap-y-2 text-xs">
        <!-- 入场原因 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.enter_tag()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" bind:value={currentOrder.enter_tag}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs">{currentOrder.enter_tag}</div>
          {/if}
        </div>
        
        <!-- 订单类型 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.order_type()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" bind:value={currentOrder.enter.order_type}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs">{currentOrder.enter.order_type}</div>
          {/if}
        </div>
        
        <!-- 方向 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.side()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" bind:value={currentOrder.enter.side}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs 
                  {currentOrder.enter.side === 'sell' ? 'text-error' : 'text-success'}">
              {currentOrder.enter.side}
            </div>
          {/if}
        </div>

        <!-- 价格 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.trigger_price()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" value={currentOrder.stop?.toFixed(7) ?? ''}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
              {currentOrder.stop?.toFixed(7) ?? '-'}
            </div>
          {/if}
        </div>

        <!-- 价格 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.enter_price()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" value={currentOrder.enter.price?.toFixed(7) ?? ''}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
              {currentOrder.enter.price?.toFixed(7) ?? '-'}
            </div>
          {/if}
        </div>
        
        <!-- 数量 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.amount()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" value={currentOrder.enter.quantity?.toFixed(5) ?? ''}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
              {currentOrder.enter.quantity?.toFixed(5) ?? '-'}
            </div>
          {/if}
        </div>
        
        <!-- 订单ID -->
        <div class="md:col-span-3 space-y-0.5">
          <div class="text-xs text-base-content/70">{m.order_id()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono flex items-center justify-between">
            <span class="truncate pr-1">{currentOrder?.enter?.order_id ?? '-'}</span>
            {#if currentOrder?.enter?.order_id}
            <button onclick={() => currentOrder?.enter?.order_id && copyToClipboard(currentOrder.enter.order_id)} 
                    class="btn btn-xs btn-ghost btn-square p-0 h-4 min-h-0 w-4" 
                    title={m.copy_id()}>
              <span class="i-lucide-copy text-base-content/50 text-xs"></span>
            </button>
            {/if}
          </div>
        </div>
        
        <!-- 更新时间 -->
        <div class="md:col-span-3 space-y-0.5">
          <div class="text-xs text-base-content/70">{m.update_time()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
            {fmtDateStrTZ(currentOrder.enter?.update_at)}
          </div>
        </div>
        
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.cost()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" value={currentOrder.quote_cost?.toFixed(5) ?? '-'}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
              {currentOrder.quote_cost?.toFixed(5) ?? '-'}
            </div>
          {/if}
        </div>
        
        {#if currentOrder.enter.filled > 0}
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.average_price()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
            {currentOrder.enter.average?.toFixed(7) ?? '-'}
          </div>
        </div>
        
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.filled_amount()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
            {currentOrder.enter.filled?.toFixed(5) ?? '-'}
          </div>
        </div>
        
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.fee()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
            {currentOrder.enter.fee?.toFixed(5) ?? '-'} {currentOrder.enter.fee_type ?? ''}
          </div>
        </div>
        {/if}
      </div>
    </div>
    {/if}
  
    <!-- 出场订单详情 -->
    {#if currentOrder.exit_tag && currentOrder.exit}
    <div class="bg-base-100 rounded-lg shadow-sm border border-base-200">
      <div class="flex items-center justify-between px-3 py-2 bg-base-200/50 border-b border-base-300">
        <div class="flex items-center gap-2">
          <div class="bg-error/20 p-1.5 rounded-full">
            <span class="i-lucide-log-out text-error text-sm"></span>
          </div>
          <div>
            <h2 class="text-sm font-bold">{m.exit_order()}</h2>
            <div class="text-xs text-base-content/70">
              {fmtDateStrTZ(currentOrder.exit_at)}
            </div>
          </div>
        </div>
        
        <div class="badge badge-sm">
          {getExStatusText(currentOrder.exit.status, currentOrder.exit.filled)}
        </div>
      </div>
      
      <div class="p-3 grid grid-cols-2 md:grid-cols-6 gap-x-3 gap-y-2 text-xs">
        <!-- 出场原因 -->
        <div class="md:col-span-2 space-y-0.5">
          <div class="text-xs text-base-content/70">{m.exit_tag()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" bind:value={currentOrder.exit_tag}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs">{currentOrder.exit_tag}</div>
          {/if}
        </div>
        
        <!-- 订单类型 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.order_type()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" bind:value={currentOrder.exit.order_type}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs">{currentOrder.exit.order_type}</div>
          {/if}
        </div>
        
        <!-- 方向 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.side()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" bind:value={currentOrder.exit.side}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs 
                  {currentOrder.exit.side === 'sell' ? 'text-error' : 'text-success'}">
              {currentOrder.exit.side}
            </div>
          {/if}
        </div>
        
        <!-- 价格 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.exit_price()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" value={currentOrder.exit.price?.toFixed(7) ?? ''}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
              {currentOrder.exit.price?.toFixed(7) ?? '-'}
            </div>
          {/if}
        </div>
        
        <!-- 数量 -->
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.amount()}</div>
          {#if isEditable}
            <input type="text" class="input input-xs w-full h-6" value={currentOrder.exit.quantity?.toFixed(5) ?? ''}/>
          {:else}
            <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
              {currentOrder.exit.quantity?.toFixed(5) ?? '-'}
            </div>
          {/if}
        </div>
        
        <!-- 订单ID -->
        <div class="md:col-span-3 space-y-0.5">
          <div class="text-xs text-base-content/70">{m.order_id()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono flex items-center justify-between">
            <span class="truncate pr-1">{currentOrder?.exit?.order_id ?? '-'}</span>
            {#if currentOrder?.exit?.order_id}
            <button onclick={() => currentOrder?.exit?.order_id && copyToClipboard(currentOrder.exit.order_id)} 
                    class="btn btn-xs btn-ghost btn-square p-0 h-4 min-h-0 w-4" 
                    title={m.copy_id()}>
              <span class="i-lucide-copy text-base-content/50 text-xs"></span>
            </button>
            {/if}
          </div>
        </div>
        
        <!-- 更新时间 -->
        <div class="md:col-span-3 space-y-0.5">
          <div class="text-xs text-base-content/70">{m.update_time()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
            {fmtDateStrTZ(currentOrder.exit?.update_at)}
          </div>
        </div>
        
        {#if currentOrder.exit.filled > 0}
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.average_price()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
            {currentOrder.exit.average?.toFixed(7) ?? '-'}
          </div>
        </div>
        
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.filled_amount()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
            {currentOrder.exit.filled?.toFixed(5) ?? '-'}
          </div>
        </div>
        
        <div class="space-y-0.5">
          <div class="text-xs text-base-content/70">{m.fee()}</div>
          <div class="bg-base-200/30 p-1.5 rounded border border-base-300 text-xs font-mono">
            {currentOrder.exit.fee?.toFixed(5) ?? '-'} {currentOrder.exit.fee_type ?? ''}
          </div>
        </div>
        {/if}
      </div>
    </div>
    {/if}
  
    <!-- 额外信息 -->
    {#if currentOrder.info}
    <div class="bg-base-100 rounded-lg shadow-sm border border-base-200">
      <div class="flex items-center justify-between px-3 py-2 bg-base-200/50 border-b border-base-300">
        <div class="flex items-center gap-2">
          <div class="bg-primary/20 p-1.5 rounded-full">
            <span class="i-lucide-file-text text-primary text-sm"></span>
          </div>
          <h2 class="text-sm font-bold">{m.additional_info()}</h2>
        </div>
      </div>
      
      <div class="p-2">
        <pre class="whitespace-pre-wrap break-all bg-base-200 p-2 rounded text-xs font-mono overflow-x-auto border border-base-300">{JSON.stringify(currentOrder.info, null, 2)}</pre>
      </div>
    </div>
    {/if}
  </div> 
  {/if}
</Modal>

<style>
  /* 自定义滚动条样式 */
  .custom-scrollbar::-webkit-scrollbar {
    width: 5px;
    height: 5px;
  }
  
  .custom-scrollbar::-webkit-scrollbar-thumb {
    background-color: rgba(0, 0, 0, 0.2);
    border-radius: 3px;
  }
  
  .custom-scrollbar::-webkit-scrollbar-track {
    background-color: transparent;
  }
  
  .custom-scrollbar:hover::-webkit-scrollbar-thumb {
    background-color: rgba(0, 0, 0, 0.3);
  }
</style>