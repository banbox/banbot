<script lang="ts">
  import Modal from "./Modal.svelte"
  import { getContext } from "svelte";
  import * as m from '$lib/paraglide/messages.js'
  import { ChartSave, ChartCtx } from "./chart";
  import type { Writable } from "svelte/store";
  import type { Chart, Nullable, PaneOptions } from 'klinecharts';
  import { derived } from "svelte/store";
  import {ActionType} from 'klinecharts';
  import { IndFieldsMap, getIndCalcParams } from './coms';
  import KlineIcon from './Icon.svelte';
  import { postApi, getApi } from '$lib/netio';
  import { alerts } from '$lib/stores/alerts';
  import { makeCsvIndicator } from './indicators/cloudInds';
  import { registerIndicator } from 'klinecharts';
  let { show = $bindable() } = $props();
  
  let fileInput: HTMLInputElement;
  let uploading = $state(false);
  
  const ctx = getContext('ctx') as Writable<ChartCtx>;
  const save = getContext('save') as Writable<ChartSave>;
  const chart = getContext('chart') as Writable<Nullable<Chart>>;
  
  let keyword = $state('');
  let checked = $state<Record<string, boolean>>({})

  let showInds = $derived.by(() => {
    if (keyword) {
      const keywordLow = keyword.toLowerCase();
      return $ctx.allInds.filter(i => 
        i.name.toLowerCase().includes(keywordLow) || 
        i.title.toLowerCase().includes(keywordLow)
      )
    }
    return $ctx.allInds;
  })

  let saveInds = derived(save, ($save) => $save.saveInds)
  saveInds.subscribe((new_val) => {
    checked = {}
    Object.keys(new_val).forEach(k => {
      const parts = k.split('_');
      checked[parts[parts.length - 1]] = true
    })
  })

  function toggleInd(isMain: boolean, name: string, checked: boolean) {
    const paneId = isMain ? 'candle_pane' : 'pane_'+name;
    if (checked) {
      createIndicator(name, undefined, isMain, {id: paneId})
    } else {
      delInd(paneId, name)
    }
  }

  function resolveIndicatorFeature(data: any) {
    const iconId = data?.iconId ?? data?.feature?.id ?? '';
    const indicatorName = data?.indicatorName ?? data?.indicator?.name ?? '';
    const paneId = data?.paneId ?? data?.indicator?.paneId ?? '';
    if (!iconId || !indicatorName || !paneId) return null;
    return { iconId, indicatorName, paneId }
  }

  function handleIndicatorFeatureClick(data: any) {
    const resolved = resolveIndicatorFeature(data)
    if (!resolved) return
    const { iconId, indicatorName, paneId } = resolved
    switch (iconId) {
      case 'visible': {
        $chart?.overrideIndicator({ name: indicatorName, visible: true, paneId })
        break
      }
      case 'invisible': {
        $chart?.overrideIndicator({ name: indicatorName, visible: false, paneId })
        break
      }
      case 'setting': {
        $ctx.editIndName = indicatorName
        $ctx.editPaneId = paneId
        $ctx.modalIndCfg = true
        break
      }
      case 'close': {
        delInd(paneId, indicatorName)
        break
      }
    }
  }
  
  export function createIndicator(name: string, params?: any[], isStack?: boolean, paneOptions?: PaneOptions): Nullable<any> {
    const chartObj = $chart;
    if (!chartObj) return null;
    // CSV indicators need to be registered before creating
    if (name.endsWith('.csv')) {
      registerIndicator(makeCsvIndicator(name));
    }
    if (name === 'VOL') {
      paneOptions = { axis: {gap: { bottom: 2 }}, ...paneOptions }
    }
    let calcParams = params;
    if (!calcParams || calcParams.length === 0) {
      const fields = IndFieldsMap[name] || [];
      if (fields.length > 0) {
        calcParams = getIndCalcParams(fields);
      }
    }
    const ind_id = chartObj.createIndicator({
      name, calcParams,
      // @ts-expect-error
      createTooltipDataSource: ({ indicator }) => {
        const icon_ids = [indicator.visible ? 1: 0, 2, 3];
        const styles = chartObj.getStyles().indicator.tooltip;
        const features = icon_ids.map(i => styles.features[i])
        return { features }
      },
      onClick: (evt: any) => {
        if (evt?.target !== 'feature' && !evt?.feature) return;
        handleIndicatorFeatureClick(evt);
      },
    }, isStack, paneOptions)
    if(!ind_id)return null
    const pane_id = paneOptions?.id ?? ''
    const ind = {name, pane_id, params: calcParams}
    save.update(s => {
      s.saveInds[`${pane_id}_${name}`] = ind;
      return s
    })
    return ind
  }
  
  export function delInd(paneId: string, name: string){
    $chart?.removeIndicator({paneId, name})
    save.update(s => {
      delete s.saveInds[`${paneId}_${name}`]
      return s
    })
  }

  const cloudIndLoaded = derived(ctx, ($ctx) => $ctx.cloudIndLoaded);
  cloudIndLoaded.subscribe((new_val) => {
    Object.values($save.saveInds).forEach(o => {
      createIndicator(o.name, o.params, o.pane_id === 'candle_pane', {id: o.pane_id})
    })
  })

  const initDone = derived(ctx, ($ctx) => $ctx.initDone);
  initDone.subscribe((new_val) => {
    $chart?.subscribeAction(ActionType.OnCandleTooltipFeatureClick, data => {
      handleIndicatorFeatureClick(data)
    })
  })

  async function handleFileSelect(event: Event) {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) return;
    
    if (!file.name.toLowerCase().endsWith('.csv')) {
      alerts.error('Only .csv files are allowed');
      return;
    }

    uploading = true;
    const formData = new FormData();
    formData.append('file', file);

    const rsp = await postApi('/kline/csv/upload', formData);
    uploading = false;
    
    if (rsp.code === 200) {
      alerts.success(m.upload_csv_success());
      await refreshCsvList();
    } else {
      alerts.error(m.upload_csv_failed() + ': ' + (rsp.msg || ''));
    }
    
    // Reset the input so the same file can be selected again
    input.value = '';
  }

  export async function refreshCsvList() {
    const rsp = await getApi('/kline/csv/list');
    if (rsp.code === 200 && rsp.data) {
      // Remove old CSV indicators and add new ones
      ctx.update(c => {
        c.allInds = c.allInds.filter(ind => !ind.name.endsWith('.csv'));
        for (const csvInd of rsp.data) {
          c.allInds.push({
            name: csvInd.name,
            title: csvInd.title,
            cloud: true,
            is_main: false
          });
          // Register CSV indicator with klinecharts
          registerIndicator(makeCsvIndicator(csvInd.name));
        }
        return c;
      });
    }
  }

</script>

<Modal title={m.indicator()} width={550} bind:show={show}>
  <div class="flex flex-col gap-4">
    <div class="flex gap-2">
      <input
        type="text"
        class="input flex-1"
        placeholder={m.search()}
        bind:value={keyword}
      />
      <input
        type="file"
        accept=".csv"
        class="hidden"
        bind:this={fileInput}
        onchange={handleFileSelect}
      />
      <button 
        class="btn btn-outline btn-sm"
        onclick={() => fileInput?.click()}
        disabled={uploading}
      >
        {#if uploading}
          <span class="loading loading-spinner loading-xs"></span>
        {/if}
        {m.import_csv()}
      </button>
      <button 
        class="btn btn-outline btn-sm btn-square"
        onclick={() => refreshCsvList()}
        title={m.refresh_cloud_inds()}
      >
        ↻
      </button>
    </div>
    
    <div class="flex h-[400px]">
      <div class="flex-1 overflow-y-auto">
        {#each showInds as ind}
          <div class="flex items-center h-10 px-5 hover:bg-base-200">
            <label class="label cursor-pointer flex justify-between flex-1">
              <div class="flex items-center flex-1">
                <span class="icon-overlay w-6 mr-3">
                  {#if ind.cloud}
                    <KlineIcon name="cloud" size={24} />
                  {/if}
                </span>
                <span class="label-text flex-1">{ind.title}</span>
              </div>
              <input type="checkbox" class="checkbox" checked={!!checked[ind.name]} 
              onchange={(e) => toggleInd(ind.is_main, ind.name, e.currentTarget.checked)} />
            </label>
          </div>
        {/each}
      </div>
    </div>
  </div>
</Modal> 
