<script lang="ts">
  import * as m from '$lib/paraglide/messages.js'
  import CodeMirror from '$lib/dev/CodeMirror.svelte';
  import { oneDark } from '@codemirror/theme-one-dark';
  import type { Extension } from '@codemirror/state';
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import { getApi, postApi } from '$lib/netio';
  import {alerts} from "$lib/stores/alerts"
  import AllConfig from '$lib/dev/AllConfig.svelte';
  import Modal from '$lib/kline/Modal.svelte';
  import {localizeHref} from "$lib/paraglide/runtime";
  import { site } from '$lib/stores/site';
  import {clickCompile} from "$lib/dev/common";

  let separateStrat = $state(false);
  let theme: Extension | null = $state(oneDark);
  let editor: CodeMirror | null = $state(null);
  let configDrawer = $state(false);
  let configText = $state('');
  let showDuplicate = $state(false);
  let showCompile = $state(false);
  let disableMainBtn = $state(false);
  let dupMode = $state('');
  let activeTab = $state('');
  let tabs: Record<string, string> = $state({});
  let strats: string[] = $state([]);
  let searchQuery = $state('');
  type Inspection = {
    version: number;
    engines: string[];
    execution_mode: string;
    data_checked: boolean;
    strategies: {engine: string; name: string; id: string; account: string; timeframe?: string; resolved?: Record<string, unknown>}[];
    warnings: {code: string; message: string}[];
    origins: Record<string, {Source: string; Kind: string}>;
    effective_config: string;
  };
  let catalog = $state<{definitions: string[]; portfolio_builders: string[]} | null>(null);
  let inspection = $state<Inspection | null>(null);
  let checking = $state(false);
  let checkedDraft = $state('');
  let checkError = $state('');
  const currentDraft = $derived(JSON.stringify(tabs));

  let filteredStrats: string[] = $derived.by(() => {
    if (!searchQuery) return strats;
    return strats.filter(strat => 
      strat.toLowerCase().includes(searchQuery.toLowerCase())
    );
  });

  onMount(async () => {
    let arr = ['config.yml', 'config.local.yml'];
    let paths = arr.map(v => "@" + v);
    const [stratRsp, catalogRsp, rsp] = await Promise.all([
      getApi('/dev/available_strats'), getApi('/dev/strategy_catalog'), getApi('/dev/texts', {paths})
    ]);
    if (stratRsp.code === 200) strats = stratRsp.data;
    else alerts.error(stratRsp.msg || 'load strats failed');
    if (catalogRsp.code === 200 && catalogRsp.data?.version === 1) catalog = catalogRsp.data;
    if(rsp.code != 200) {
      alerts.error(rsp.msg || 'load config failed');
      return;
    }
    paths.forEach(p => {
      if(rsp[p]){
        activeTab = p.substring(1);
        tabs[activeTab] = rsp[p];
        configText = rsp[p];
      }
    })
    if (editor) {
      editor.setValue(activeTab, configText);
    }
  });

  $effect(() => {
    if(activeTab){
      setTimeout(function () {
        configText = tabs[activeTab];
        editor?.setValue(activeTab, configText);
      }, 100)
    }
  });

  async function onTextChange(value: string) {
    configText = value;
    tabs[activeTab] = value;
  }

  function editorConfigs() {
    const configs: Record<string, string> = {};
    const paths = Object.keys(tabs).map(key => `@${key}`);
    for (const key of Object.keys(tabs)) configs[`@${key}`] = tabs[key];
    return {configs, paths};
  }

  async function checkConfig() {
    const draft = currentDraft;
    checking = true;
    checkError = '';
    inspection = null;
    const rsp = await postApi('/dev/backtest_preflight', editorConfigs());
    checking = false;
    if (draft !== currentDraft) { checkError = m.preflight_stale(); return; }
    if (rsp.code !== 200) {
      checkError = rsp.code === 404 || rsp.code === 503 ? m.preflight_unavailable() : rsp.msg || 'Configuration check failed';
      return;
    }
    if (rsp.data?.version !== 1) { checkError = m.preflight_unavailable(); return; }
    inspection = rsp.data;
    checkedDraft = draft;
  }

  async function startBacktest() {
    if (!configText) {
      alerts.error("config is empty");
      return;
    }

    // 可以同时开始多个，后端会逐个启动执行
    const {configs, paths} = editorConfigs();
    const rsp = await postApi('/dev/run_backtest', {
      separate: separateStrat,
      configs: configs,
      paths: paths,
      dupMode: dupMode
    });
    if (rsp.code >= 400 && rsp.msg && rsp.msg.indexOf("already_exist") >= 0) {
      showDuplicate = true;
      return;
    }

    if (rsp.code === 200) {
      alerts.success(m.add_bt_ok());
      await goto(localizeHref('/backtest'));
    } else {
      console.error('run backtest fail', rsp);
      alerts.error(rsp.msg || "run backtest fail");
    }
  }

  async function clickDuplicate(type: string) {
    showDuplicate = false;
    if (type === 'backup_start') {
      dupMode = 'backup';
    }else if (type === 'overwrite_start') {
      dupMode = 'overwrite';
    }else{
      return;
    }
    startBacktest();
  }

  async function clickBacktest(){
    if($site.dirtyBin){
      showCompile = true
    }else{
      await startBacktest();
    }
  }

  async function clickCompileOrNot(choose: string){
    showCompile = false;
    if(choose == 'compile'){
      disableMainBtn = true;
      await clickCompile()
      disableMainBtn = false;
    }else if(choose == 'just_backtest'){
      $site.dirtyBin = false;
      await startBacktest();
    }
  }

  async function copyToClipboard(text: string) {
    await navigator.clipboard.writeText(text);
    alerts.success(m.copied());
  }

</script>

<Modal title={m.duplicate_backtest()} buttons={['backup_start', 'overwrite_start', 'cancel']} show={showDuplicate} 
click={clickDuplicate} center={true} width={600}>
  {m.backtest_duplicate_info()}
</Modal>

<Modal title={m.confirm()} buttons={['compile', 'just_backtest', 'cancel']} show={showCompile}
       click={clickCompileOrNot} center={true} width={400}>
  {m.build_for_backtest()}
</Modal>

<div class="drawer drawer-end">
  <input id="config-drawer" type="checkbox" class="drawer-toggle" bind:checked={configDrawer} />
  <div class="drawer-content">
    <div class="container mx-auto px-4 py-6">
      <div class="flex gap-6">
        <!-- 左侧策略列表 -->
        <div class="w-[15%] bg-base-200 rounded-lg p-3">
          <h2 class="text-lg font-bold mb-2 text-primary">{m.registered_strats()}</h2>
          <div class="mb-3">
            <input type="text" placeholder="Search ..." class="input input-sm" bind:value={searchQuery}/>
          </div>
          <div class="space-y-0.5">
            {#each filteredStrats as strat}
              <div 
                class="px-2 py-1 rounded cursor-pointer hover:bg-primary hover:bg-opacity-10 transition-all duration-200 text-sm"
                onclick={() => copyToClipboard(strat)}
              >
                {strat}
              </div>
            {/each}
          </div>
          {#if catalog}
            <h3 class="font-semibold mt-6 mb-2">{m.catalog_definitions()}</h3>
            {#each catalog.definitions as definition (definition)}
              <button class="btn btn-ghost btn-sm w-full justify-start" onclick={() => copyToClipboard(definition)}>{definition}</button>
            {/each}
            <h3 class="font-semibold mt-4 mb-2">{m.catalog_builders()}</h3>
            {#each catalog.portfolio_builders as builder (builder)}
              <button class="btn btn-ghost btn-sm w-full justify-start" onclick={() => copyToClipboard(builder)}>{builder}</button>
            {/each}
          {/if}
        </div>

        <!-- 右侧主内容区 -->
        <div class="flex-1">
          <div class="flex justify-between items-center mb-6">
            <h2 class="text-2xl font-bold">{m.run_backtest()}</h2>
            <a class="btn btn-outline" href={localizeHref("/backtest")}>{m.backtest_history()}</a>
          </div>

          <div class="mb-12">
            <div class="flex justify-between items-center mb-2">
              <div>
                <div class="tabs tabs-box tabs-sm">
                  {#each Object.keys(tabs) as tab}
                    <input type="radio" class="tab" aria-label={tab} checked={activeTab === tab}
                           onclick={() => activeTab = tab}/>
                  {/each}
                </div>
                <p class="mt-2 text-sm opacity-70">
                  {activeTab === 'config.local.yml'
                          ? m.local_config_desc()
                          : m.global_config_desc()}
                </p>
              </div>
              <label for="config-drawer" class="link link-primary cursor-pointer">{m.full_config()}</label>
            </div>
            <CodeMirror bind:this={editor} change={onTextChange} {theme} class="flex-1 h-full"/>
            <p class="text-sm opacity-70 mt-3">{m.catalog_factor_hint()}</p>
            <section class="mt-4 rounded-lg bg-base-200 p-4" aria-label={m.preflight_static()}>
              <div class="flex items-center justify-between gap-3">
                <h3 class="font-semibold">{m.preflight_static()}</h3>
                <button class="btn btn-outline btn-sm" disabled={checking} onclick={checkConfig}>{m.preflight_check()}</button>
              </div>
              {#if checkError}<p class="text-error mt-3" role="alert">{checkError}</p>{/if}
              {#if inspection}
                {#if checkedDraft !== currentDraft}
                  <p class="text-warning mt-3" role="status">{m.preflight_stale()}</p>
                {:else}
                  <p class="text-success mt-3" role="status">{m.preflight_passed()}</p>
                  <div class="flex gap-2 mt-2 flex-wrap">
                    {#each inspection.engines as engine (engine)}<span class="badge badge-outline">{engine}</span>{/each}
                    {#if inspection.execution_mode}<span class="badge">{inspection.execution_mode}</span>{/if}
                  </div>
                  <p class="text-sm mt-2">{m.preflight_data_unchecked()}</p>
                  <div class="overflow-auto mt-3"><table class="table table-sm">
                    <thead><tr><th>{m.result_engine()}</th><th>{m.result_strategy_id()}</th><th>{m.result_account_id()}</th><th>{m.timeframe()}</th></tr></thead>
                    <tbody>{#each inspection.strategies as strategy, strategyIndex (strategyIndex)}
                      <tr><td>{strategy.engine}</td><td>{strategy.id}</td><td>{strategy.account || '-'}</td><td>{strategy.timeframe || '-'}</td></tr>
                    {/each}</tbody>
                  </table></div>
                  <details class="mt-3"><summary class="cursor-pointer">{m.preflight_resolved()}</summary>
                    <pre class="text-xs overflow-auto max-h-80 mt-2">{JSON.stringify(inspection.strategies, null, 2)}</pre>
                  </details>
                  <details class="mt-3"><summary class="cursor-pointer">{m.preflight_origins()}</summary>
                    <div class="overflow-auto"><table class="table table-xs"><tbody>
                      {#each Object.entries(inspection.origins) as [field, origin] (field)}<tr><td>{field}</td><td>{origin.Kind}</td><td>{origin.Source}</td></tr>{/each}
                    </tbody></table></div>
                  </details>
                  <details class="mt-3"><summary class="cursor-pointer">{m.preflight_effective()}</summary>
                    <pre class="text-xs overflow-auto max-h-96 mt-2">{inspection.effective_config}</pre>
                  </details>
                {/if}
              {/if}
            </section>
          </div>

          <div class="flex gap-4 fixed bottom-0 left-0 right-0 p-2 w-[100%] bg-white flex justify-center">
            <button class="btn btn-primary w-[50%]" disabled={disableMainBtn} onclick={clickBacktest}>{m.start_backtest()}</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="drawer-side">
    <label for="config-drawer" aria-label="close sidebar" class="drawer-overlay"></label>
    <div class="bg-base-200 min-h-full w-2/3 p-4">
      <AllConfig />
    </div>
  </div>
</div>
