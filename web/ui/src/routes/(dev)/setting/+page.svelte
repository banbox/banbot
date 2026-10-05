<script lang="ts">
  import { onMount } from 'svelte';
  import { getApi, postApi } from '$lib/netio';
  import * as m from '$lib/paraglide/messages.js';
  import { alerts } from "$lib/stores/alerts";
  import Icon from '$lib/Icon.svelte';
  import CodeMirror from '$lib/dev/CodeMirror.svelte';
  import { oneDark } from '@codemirror/theme-one-dark';
  import { type Extension } from '@codemirror/state';
  import {site} from '$lib/stores/site';
  import { page } from '$app/state'
  import {modals} from '$lib/stores/modals';
  import { localizeHref, locales, getLocale } from "$lib/paraglide/runtime.js"
  import { goto } from '$app/navigation'
  import { getTimezoneSelectOptions, setTimezone, curTZ } from '$lib/dateutil';

  let activeTab = $state('config.local.yml');
  let theme: Extension | null = $state(oneDark);
  let editor: CodeMirror | null = $state(null);
  let configText = $state('');
  let configDigests: Record<string, string> = {};
  let savingConfig = false;
  let loadedConfigPath = '';
  let tabs: Record<string, string> = {'config.local.yml': '$/config.local.yml', 'config.yml': '$/config.yml'};

  // 构建相关状态
  let activePage = $state('config');
  let buildEnvs: string[] = $state([]);
  let osMap = $state(new Map<string, string[]>());
  let selectedOS = $state('');
  let selectedArch = $state('');
  let isBuilding = $state(false);
  let buildOk = $state(false);
  let selectedLang = $state<string>(getLocale());

  // 时区相关状态
  let timezoneOptions = getTimezoneSelectOptions();
  let selectedTimezone = $state(curTZ().toLowerCase());

  onMount(() => {
    const tab = page.url.searchParams.get('tab');
    if(tab){
      activePage = tab
    }
    loadConfig();
    loadBuildEnvs();
  });

  async function loadBuildEnvs() {
    const rsp = await getApi('/dev/build_envs');
    if(rsp.code != 200) {
      alerts.error(rsp.msg || 'load build envs failed');
      return;
    }
    buildEnvs = rsp.data ?? [];
    
    // 处理环境列表
    const tempMap = new Map<string, Set<string>>();
    buildEnvs.forEach(env => {
      const [os, arch] = env.split('/');
      if (!tempMap.has(os)) {
        tempMap.set(os, new Set());
      }
      tempMap.get(os)?.add(arch);
    });

    // 转换为最终的 Map
    osMap = new Map();
    tempMap.forEach((archSet, os) => {
      const archArray = Array.from(archSet);
      // 固定 amd64 和 arm64 在前面
      const sortedArch = [
        ...archArray.filter(arch => arch === 'amd64'),
        ...archArray.filter(arch => arch === 'arm64'),
        ...archArray.filter(arch => arch !== 'amd64' && arch !== 'arm64')
      ];
      osMap.set(os, sortedArch);
    });
  }

  let osList = $derived.by(() => {
    const keys = Array.from(osMap.keys());
    // 固定 linux, darwin, windows 在前面
    return [
      ...keys.filter(os => os === 'linux'),
      ...keys.filter(os => os === 'darwin'),
      ...keys.filter(os => os === 'windows'),
      ...keys.filter(os => !['linux', 'darwin', 'windows'].includes(os))
    ];
  })

  function getArchList(): string[] {
    if (!selectedOS) return [];
    return osMap.get(selectedOS) ?? [];
  }

  async function startBuild() {
    const path = downPath;
    if(!path)return ;
    const ok = await modals.confirm(m.confirm_build());
    if (!ok) return;
    
    isBuilding = true;
    try {
      const rsp = await postApi('/dev/build', {
        os: selectedOS,
        arch: selectedArch, path
      });
      
      if (rsp.code != 200) {
        alerts.error(rsp.msg || 'build failed');
        return;
      }
      buildOk = true;
      alerts.success(m.build_success());
    } finally {
      isBuilding = false;
    }
  }

  let downPath = $derived.by(() => {
    if (!selectedOS || !selectedArch) return;
    let path = `$/bot_${selectedOS}_${selectedArch}`;
    if(selectedOS === 'windows'){
      path = path +'.exe'
    }
    return path;
  })

  let downloadUrl = $derived.by(() => {
    const path = downPath;
    if(!path)return ;
    return `${$site.apiHost}/api/dev/download?path=${path}`;
  })

  async function loadConfig() {
    const tab = activeTab;
    const path = tabs[tab] ?? '';
    loadedConfigPath = '';
    const rsp = await getApi('/dev/text', { path });
    if(rsp.code != 200) {
      alerts.error(rsp.msg || 'load config failed');
      return;
    }
    if (activeTab !== tab) return;
    configDigests[path] = rsp.digest ?? '';
    configText = rsp.data ?? '';
    if (editor) {
      editor.setValue(tab, configText);
    }
    loadedConfigPath = path;
  }

  async function saveConfig() {
    if (savingConfig) return;
    const path = tabs[activeTab] ?? '';
    if (loadedConfigPath !== path) {
      alerts.error('Wait for the file to load before saving');
      return;
    }
    savingConfig = true;
    let rsp;
    try {
      rsp = await postApi('/dev/save_text', {
        path, content: editor?.getValue() ?? configText, digest: configDigests[path]
      });
    } finally {
      savingConfig = false;
    }
    if(rsp.code != 200) {
      alerts.error(rsp.msg || 'save config failed');
      return;
    }
    configDigests[path] = rsp.digest;
    alerts.success(m.save_success());
  }
  
  async function onTextChange(value: string, name?: string) {
    if (name !== activeTab) return;
    configText = value;
  }

  $effect(() => {
    if(activeTab){
      setTimeout(loadConfig, 100)
    }
  });

  function setActivePage(page: string) {
    activePage = page;
    if (page === 'config') {
      activeTab = 'config.local.yml';
      loadConfig();
    }
  }

  async function switchLanguage(lang: (typeof locales)[number]) {
    if (lang === selectedLang) return;
    selectedLang = lang;
    const path = page.url.pathname || '/';
    await goto(localizeHref(path, {locale: lang}));
  }

  async function handleTimezoneChange(timezone: string) {
    selectedTimezone = timezone;
    await setTimezone(timezone);
    alerts.success(m.save_success());
  }
</script>

<div class="container mx-auto max-w-[1500px] px-4 py-6">
  <!-- 顶部标题 -->
  <div class="flex justify-between items-center mb-6">
    <h1 class="text-2xl font-bold">{m.setting()}</h1>
  </div>

  <!-- 主体内容 -->
  <div class="flex gap-6">
    <!-- 左侧菜单 -->
    <div class="w-[15%]">
      <ul class="menu w-full bg-base-200 rounded-box">
        <li>
          <button class:menu-active={activePage === 'config'}
            onclick={() => setActivePage('config')}>
            <Icon name="config" />{m.global_config()}
          </button>
        </li>
        <li>
          <button class:menu-active={activePage === 'build'}
            onclick={() => setActivePage('build')}>
            <Icon name="play" />{m.build()}
          </button>
        </li>
        <li>
          <button class:menu-active={activePage === 'language'}
            onclick={() => setActivePage('language')}>
            <Icon name="language" />{m.language()}
          </button>
        </li>
        <li>
          <button class:menu-active={activePage === 'timezone'}
            onclick={() => setActivePage('timezone')}>
            <Icon name="clock" />{m.timezone()}
          </button>
        </li>
      </ul>
    </div>

    <!-- 右侧内容 -->
    <div class="flex-1">
      {#if activePage === 'config'}
        <!-- 配置页面内容 -->
        <div>
          <!-- 顶部控制区 -->
          <div class="flex justify-between items-start mb-4">
            <div>
              <!-- 配置文件选择tabs -->
              <div class="tabs tabs-box tabs-sm">
                {#each Object.keys(tabs) as tab}
                  <input type="radio" class="tab" aria-label={tab} checked={activeTab === tab}
                    onclick={() => activeTab = tab}/>
                {/each}
              </div>
              <!-- 说明文字 -->
              <p class="mt-2 text-sm opacity-70">
                {activeTab === 'config.local.yml' 
                  ? m.local_config_desc() 
                  : m.global_config_desc()}
              </p>
            </div>
            
            <!-- 保存按钮 -->
            <button class="btn btn-primary" onclick={saveConfig}>
              {m.save()}
            </button>
          </div>

          <!-- 配置编辑器 -->
          <div class="border rounded-lg overflow-hidden">
            <CodeMirror bind:this={editor} change={onTextChange} {theme} class="flex-1 h-full"/>
          </div>
        </div>
      {:else if activePage === 'build'}
        <!-- 构建页面内容 -->
        <div class="space-y-6">
          <div class="alert">
            {m.build_desc()}
          </div>
          <!-- 操作系统选择 -->
          <div class="flex">
            <div class="w-[10rem]">
              <label class="label">
                <span class="label-text">{m.select_os()}</span>
              </label>
            </div>
            <div class="flex-1">
              <div class="flex flex-wrap gap-2">
                {#each osList as os}
                  <label class="label cursor-pointer">
                    <input type="radio" name="os" class="radio radio-primary"
                      checked={selectedOS === os}
                      onchange={() => {
                        selectedOS = os;
                        selectedArch = '';
                      }}
                    />
                    <span class="label-text ml-2">{os}</span>
                  </label>
                {/each}
              </div>
            </div>
          </div>

          <!-- 架构选择 -->
          {#if selectedOS}
            <div class="flex">
              <div class="w-[10rem]">
                <label class="label">
                  <span class="label-text">{m.select_arch()}</span>
                </label>
              </div>
              <div class="flex-1">
                <div class="flex flex-wrap gap-2">
                  {#each getArchList() as arch}
                    <label class="label cursor-pointer">
                      <input type="radio" name="arch" class="radio radio-primary"
                        checked={selectedArch === arch}
                        onchange={() => selectedArch = arch}
                      />
                      <span class="label-text ml-2">{arch}</span>
                    </label>
                  {/each}
                </div>
              </div>
            </div>
          {/if}

          <!-- 编译按钮 -->
          {#if selectedOS && selectedArch}
            <div class="flex">
              <div class="w-[10rem]"></div>
              <div class="flex-1">
                <button class="btn btn-primary"  class:loading={isBuilding} onclick={startBuild}>
                  {isBuilding ? m.compiling() : m.compile()}
                </button>
                {#if buildOk}
                  <a href={downloadUrl} class="btn btn-primary btn-outline" target="_blank" data-no-translate>
                    {m.download()}
                  </a>
                {/if}
              </div>
            </div>
          {/if}
        </div>
      {:else if activePage === 'language'}
        <!-- 语言设置页面 -->
        <div class="space-y-6">
          <!-- 语言选择 -->
          <div class="flex">
            <div class="w-[10rem]">
              <label class="label">
                <span class="label-text">{m.language()}</span>
              </label>
            </div>
            <div class="flex-1">
              <div class="flex flex-wrap gap-2">
                {#each locales as lang}
                  <label class="label cursor-pointer">
                    <input type="radio" name="lang" class="radio radio-primary"
                      checked={selectedLang === lang}
                      onchange={() => switchLanguage(lang)}
                    />
                    <span class="label-text ml-2">{lang}</span>
                  </label>
                {/each}
              </div>
            </div>
          </div>
        </div>
      {:else}
        <div class="space-y-6">
          <!-- 时区设置 -->
          <div class="flex">
            <div class="w-[10rem]">
              <label class="label">
                <span class="label-text">{m.timezone()}</span>
              </label>
            </div>
            <div class="flex-1">
              <div class="grid grid-cols-4 gap-2">
                {#each timezoneOptions as option}
                  <label class="label cursor-pointer min-w-[200px] justify-start">
                    <input type="radio" name="timezone" class="radio radio-primary"
                      checked={selectedTimezone === option.key}
                      onchange={() => handleTimezoneChange(option.key)}
                    />
                    <span class="label-text ml-2">{m[option.text]()}</span>
                  </label>
                {/each}
              </div>
            </div>
          </div>
        </div>
      {/if}
    </div>
  </div>
</div>
