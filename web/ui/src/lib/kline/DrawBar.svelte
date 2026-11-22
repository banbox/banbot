<script lang="ts">
import type {OverlayEvent, OverlayFilter} from 'klinecharts';
import {OverlayMode} from 'klinecharts';
import { getContext } from 'svelte';
import type { Chart } from 'klinecharts'
import {ChartSave} from './chart';
import type {Writable} from 'svelte/store';
import {persisted} from 'svelte-persisted-store';
import KlineIcon from './Icon.svelte';
import type {Nullable} from 'klinecharts';
import * as m from '$lib/paraglide/messages.js'
import {derived} from 'svelte/store';
import type {ChartCtx} from './chart';
import _ from 'lodash';
import { overlayMap } from './overlays'
import {getFirstValid} from "$lib/common";

let popoverKey = $state('');
let modeIcon = $state('weakMagnet')
let mode = $state('normal')
let lock = $state(false)
let visiable = $state(true)
let hisLays: string[] = $state([])  // 按创建顺序，记录所有overlay，方便删除
let selectDraw = $state('')

const GROUP_ID = 'drawing_tools'

const save = getContext('save') as Writable<ChartSave>
const ctx = getContext('ctx') as Writable<ChartCtx>
const chart = getContext('chart') as Writable<Nullable<Chart>>
const overlays = persisted<Record<string, Record<string, any>>>($save.key + '_overlays', {})

function getOverlayKey(overlayId: string): string {
  const sym = $save.symbol;
  const symbol = getFirstValid([sym.shortName, sym.name, sym.ticker]);
  return `${symbol}_${$save.period.timeframe}_${overlayId}`
}

const singleLineOpts = [
  { key: 'segment', text: 'segment' },
  { key: 'arrow', text: 'arrow' },
  { key: 'rayLine', text: 'ray_line' },
  { key: 'straightLine', text: 'straight_line' },
  { key: 'priceLine', text: 'price_line' },
  { key: 'horizontalStraightLine', text: 'horizontal_straight_line' },
  { key: 'horizontalRayLine', text: 'horizontal_ray_line' },
  { key: 'horizontalSegment', text: 'horizontal_segment' },
  { key: 'verticalStraightLine', text: 'vertical_straight_line' },
  { key: 'verticalRayLine', text: 'vertical_ray_line' },
  { key: 'verticalSegment', text: 'vertical_segment' },
]

const moreLineOpts = [
  { key: 'priceChannelLine', text: 'price_channel_line' },
  { key: 'parallelStraightLine', text: 'parallel_straight_line' }
]

const polygonOpts = [
  { key: 'circle', text: 'circle' },
  { key: 'rect', text: 'rect' },
  { key: 'parallelogram', text: 'parallelogram' },
  { key: 'triangle', text: 'triangle' }
]

const fibonacciOpts = [
  { key: 'fibonacciLine', text: 'fibonacci_line' },
  { key: 'fibonacciSegment', text: 'fibonacci_segment' },
  { key: 'fibonacciCircle', text: 'fibonacci_circle' },
  { key: 'fibonacciSpiral', text: 'fibonacci_spiral' },
  { key: 'fibonacciSpeedResistanceFan', text: 'fibonacci_speed_resistance_fan' },
  { key: 'fibonacciExtension', text: 'fibonacci_extension' },
  { key: 'gannBox', text: 'gann_box' }
]

const waveOpts = [
  { key: 'xabcd', text: 'xabcd' },
  { key: 'abcd', text: 'abcd' },
  { key: 'threeWaves', text: 'three_waves' },
  { key: 'fiveWaves', text: 'five_waves' },
  { key: 'eightWaves', text: 'eight_waves' },
  { key: 'anyWaves', text: 'any_waves' },
]

const subMenu = $state([
  { key: 'single-line', icon: 'segment', list: singleLineOpts },
  { key: 'more-line', icon: 'priceChannelLine', list: moreLineOpts },
  { key: 'polygon', icon: 'circle', list: polygonOpts },
  { key: 'fibonacci', icon: 'fibonacciLine', list: fibonacciOpts },
  { key: 'wave', icon: 'xabcd', list: waveOpts }
])

const modes = $state([
  { key: 'weakMagnet', text: 'weakMagnet' },
  { key: 'strongMagnet', text: 'strongMagnet' }
])

function clickPopoverKey(val: string){
  if (popoverKey == val){
    popoverKey = ""
  }else{
    popoverKey = val
  }
}

export function addOverlay(data: any){
  let moved = false;
  const overlayClass = overlayMap[data.name] ?? {}
  const defData = {
    groupId: GROUP_ID,
    onDrawEnd: (event: OverlayEvent<any>) => {
      if(overlayClass.onDrawEnd){
        overlayClass.onDrawEnd(event)
      }
      editOverlay(event.overlay)
      return true
    },
    onPressedMoving: (event: OverlayEvent<any>) => {
      if(overlayClass.onPressedMoving){
        overlayClass.onPressedMoving(event)
      }
      moved = true;
      return false
    },
    onPressedMoveEnd: (event: OverlayEvent<any>) => {
      if(overlayClass.onPressedMoveEnd){
        overlayClass.onPressedMoveEnd(event)
      }
      if(!moved)return true
      moved = false
      editOverlay(event.overlay)
      return true
    },
    onSelected: (event: OverlayEvent<any>) => {
      if(overlayClass.onSelected){
        overlayClass.onSelected(event)
      }
      selectDraw = event.overlay.id
      return true;
    },
    onDeselected: (event: OverlayEvent<any>) => {
      if(overlayClass.onDeselected){
        overlayClass.onDeselected(event)
      }
      selectDraw = ''
      return true;
    },
    onRemoved: (event: OverlayEvent<any>) => {
      if(overlayClass.onRemoved){
        overlayClass.onRemoved(event)
      }
      overlays.update(ol => {
        delete ol[getOverlayKey(event.overlay.id)]
        return ol
      })
      return true
    }
  }
  // 合并时保留原有的事件处理器
  const layId = $chart?.createOverlay(_.mergeWith({}, defData, data, (objValue, srcValue, key) => {
    if (key.startsWith('on') && objValue && srcValue) {
      return (event: OverlayEvent<any>) => {
        srcValue(event)
        return objValue(event)
      }
    }
  }))
  if(layId){
    if(Array.isArray(layId)){
      hisLays.push(...(layId as string[]))
    }
    else{
      hisLays.push(layId as string)
    }
  }
  return layId;
}

function startOverlay(val: string){
  addOverlay({
    name: val,
    visible: visiable,
    lock: lock,
    mode: mode as OverlayMode,
  })
}


function clickSubPopover(index: number, value: string){
  subMenu[index].icon = value;
  startOverlay(value)
  popoverKey = '';
}

function clickMode(){
  let cur_mode = modeIcon
  if (mode !== 'normal') {
    cur_mode = 'normal'
  }
  mode = cur_mode;
  $chart?.overrideOverlay({ mode: cur_mode as OverlayMode })
}

function clickSubMode(value: string){
  modeIcon = value;
  mode = value;
  popoverKey = '';
  $chart?.overrideOverlay({ mode: value as OverlayMode })
}

function toggleLock(){
  lock = !lock;
  $chart?.overrideOverlay({ lock: lock });
}

function toggleVisiable(){
  visiable = !visiable
  $chart?.overrideOverlay({ visible: visiable })
}

export function clickRemove(){
  let args: OverlayFilter = { groupId: GROUP_ID };
  if(selectDraw){
    args['id'] = selectDraw
  }
  else if(hisLays.length > 0){
    args['id'] = hisLays.pop()
  }
  $chart?.removeOverlay(args)
}

function editOverlay(overlay: any){
  if(overlay.groupId !== GROUP_ID)return
  const keys = ['extendData', 'groupId', 'id', 'lock', 'mode', 'name', 'paneId', 'points', 'styles',
    'totalStep', 'visible', 'zLevel']
  const oid = getOverlayKey(overlay['id'])
  overlays.update(ol => {
    ol[oid] = Object.fromEntries(keys.map(k => [k, overlay[k]]))
    return ol
  })
}

const clickChart = derived(ctx, ($ctx) => $ctx.clickChart);
clickChart.subscribe(() => {
  popoverKey = ''
})

const cloudIndLoaded = derived(ctx, ($ctx) => $ctx.cloudIndLoaded);
cloudIndLoaded.subscribe(() => {
  const currentPrefix = getOverlayKey('')
  Object.entries($overlays).forEach(([key, v]) => {
    if (key.startsWith(currentPrefix)) {
      addOverlay(v)
    }
  })
})
</script>

{#snippet DrawButton(onClick: () => void, icon: string, itemKey: string = '', subItems: {key: string, text: string}[] = [])}
  <div class="flex flex-row items-center justify-center relative w-full mt-2 cursor-pointer text-base-content/70">
    <span class="w-8 h-8 hover:text-primary" onclick={onClick}>
      <KlineIcon name={icon} active={itemKey === 'mode' && mode === modeIcon}/>
    </span>
    {#if subItems.length > 0}
      <div 
        class="flex flex-row items-center justify-center absolute top-0 right-0 h-8 w-[10px] opacity-0 hover:opacity-100 transition-all duration-200 rounded-l hover:bg-base-200 z-10"
        onclick={() => clickPopoverKey(itemKey)}
      >
        <svg class:rotate-180={popoverKey === icon} class="w-1 h-1.5 transition-all duration-200" viewBox="0 0 4 6">
          <path d="M1.07298,0.159458C0.827521,-0.0531526,0.429553,-0.0531526,0.184094,0.159458C-0.0613648,0.372068,-0.0613648,0.716778,0.184094,0.929388L2.61275,3.03303L0.260362,5.07061C0.0149035,5.28322,0.0149035,5.62793,0.260362,5.84054C0.505822,6.05315,0.903789,6.05315,1.14925,5.84054L3.81591,3.53075C4.01812,3.3556,4.05374,3.0908,3.92279,2.88406C3.93219,2.73496,3.87113,2.58315,3.73964,2.46925L1.07298,0.159458Z" stroke="none" stroke-opacity="0"/>
        </svg>
      </div>
      {#if itemKey === popoverKey}
        <ul class="absolute top-0 left-[calc(100%+1px)] whitespace-nowrap bg-base-100 z-50 shadow-xl min-h-0">
          {#each subItems as data}
            <li class="px-4 hover:bg-primary-content/40 flex flex-row items-center h-10" 
              onclick={() => itemKey === 'mode' ? clickSubMode(data.key) : clickSubPopover(subMenu.findIndex(i => i.key === itemKey), data.key)}>
              <KlineIcon name={data.key}/>
              <span class="pl-2">{m[data.text]()}</span>
            </li>
          {/each}
        </ul>
      {/if}
    {/if}
  </div>
{/snippet}

{#snippet Divider()}
  <div class="w-full h-px bg-base-content/20 mt-2"></div>
{/snippet}

<div class="w-[48px] h-full box-border border-r border-base-content/20" onclick={(e) => e.stopPropagation()}>
  {#each subMenu as item}
    {@render DrawButton(() => startOverlay(item.icon), item.icon, item.key, item.list)}
  {/each}
  
  {@render Divider()}
  
  {@render DrawButton(() => startOverlay('ruler'), 'ruler')}
  
  {@render Divider()}
  
  {@render DrawButton(clickMode, modeIcon, 'mode', modes)}
  
  {@render Divider()}
  
  {@render DrawButton(toggleLock, lock ? 'lock' : 'unlock')}
  
  {@render Divider()}
  
  {@render DrawButton(toggleVisiable, visiable ? 'visible' : 'invisible')}
  
  {@render Divider()}
  
  {@render DrawButton(clickRemove, 'remove')}
</div>