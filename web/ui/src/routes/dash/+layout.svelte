<script lang="ts">
  import { page } from '$app/state';
  import Icon from '$lib/Icon.svelte';
  import {ctx, save, acc, loadAccounts, activeAcc} from '$lib/dash/store';
  import type { BotAccount, BotTicket } from '$lib/dash/types';
  import * as m from '$lib/paraglide/messages.js';
  import { alerts } from '$lib/stores/alerts';
  import AddBot from '$lib/dash/AddBot.svelte';
  import Modal from '$lib/kline/Modal.svelte';
  import {site} from '$lib/stores/site';
  import {localizeHref} from "$lib/paraglide/runtime";
  import {onMount} from "svelte";

  let { children } = $props();
  let collapsed = $state(false);
  let showAddBot = $state(false);
  $site.apiReady = false;

  // 检查是否在iframe中
  function checkIfInIframe() {
    try {
      $site.isInIframe = window.self !== window.top;
    } catch (e) {
      $site.isInIframe = true;
    }
  }
  
  let menuItems = $state([
    { path: localizeHref('/dash/board'), icon: 'home', text: m.home() },
    { path: localizeHref('/dash/strat_job'), icon: 'square-stack-3', text: m.strat_job() },
    { path: localizeHref('/dash/kline'), icon: 'chart-bar', text: m.kline() },
    { path: localizeHref('/dash/series'), icon: 'chart-bar', text: m.data() },
    { path: localizeHref('/dash/perf'), icon: 'calender', text: m.perf() },
    { path: localizeHref('/dash/order'), icon: 'number-list', text: m.order() },
    { path: localizeHref('/dash/rebate'), icon: 'dollar', text: m.rebate() },
    { path: localizeHref('/dash/setting'), icon: 'setting', text: m.config() },
    { path: localizeHref('/dash/tool'), icon: 'tool', text: m.tools() },
    { path: localizeHref('/dash/log'), icon: 'document-text', text: m.logs() }
  ]);

  function clickAccount(acc: BotAccount) {
    activeAcc(acc)
    location.reload();
  }
  
  function loginOk(info: BotTicket) {
    const num = Object.keys(info.accounts!).length
    alerts.success(`${m.add_bot_ok()}: ${info.name} (${num} accounts)`);
    showAddBot = false;
  }

  function toggleCollapse(){
    collapsed = !collapsed;
    setTimeout(() => {
      window.dispatchEvent(new Event('resize'));
    }, 500)
  }

  onMount(async () => {
    checkIfInIframe();
    loadAccounts().then(() => {
      if($acc.env === 'dry_run'){
        // Remove rebate page (index 5) and tools page (index 7)
        // Note: Remove in reverse order to maintain correct indices
        menuItems.splice(7, 1); // Remove tools page
        menuItems.splice(5, 1); // Remove rebate page
      }
      $site.apiHost = $acc.url;
      $site.apiReady = true;
      $site.apiReadyCbs.forEach(cb => cb());
      $site.apiReadyCbs = [];
    })
  });
</script>

<Modal title={m.add_bot()} bind:show={showAddBot} buttons={[]}>
  <AddBot newBot={loginOk}/>
</Modal>

<div class="flex flex-1" onclick={() => $ctx.clickPage += 1}>
  <aside class="bg-base-200 text-base-content shadow-lg {collapsed ? 'w-16' : 'w-64 min-w-[200px]'} transition-all duration-300 border-r border-base-300">
    <div class="py-3 px-4 flex justify-center items-center border-b border-base-300">
      <a href={localizeHref("/dash")} class="text-lg font-semibold hover:text-primary transition-colors">
        {#if collapsed}
          <img src="/favicon.png" alt="logo" class="w-10 h-10 object-contain"/>
        {:else}
          <img src="/banbot.png" alt="logo" class="w-24 h-10 object-contain"/>
        {/if}
      </a>
    </div>
    
    <ul class="menu w-full menu-vertical py-2 px-1.5 gap-1">
      {#each menuItems as {path, icon, text}}
        <li>
          <a 
            href={path} 
            class="rounded-md transition-all duration-200 {page.url.pathname.includes(path) ? 'bg-primary text-primary-content font-medium shadow-sm hover:bg-primary' : 'hover:bg-primary/20 hover:text-primary'}"
          >
            <Icon name={icon} class="size-5" />
            {#if !collapsed}
              <span class="ml-1.5 text-sm">{text}</span>
            {/if}
          </a>
        </li>
      {/each}
    </ul>
  </aside> 
	
	<div class="flex-1 flex flex-col bg-base-100">
    {#if !$site.isInIframe}
    <header class="navbar bg-base-100 shadow-sm h-12 px-3 border-b border-base-200">
      <div class="navbar-start">
        <button class="btn btn-ghost btn-sm btn-circle hover:bg-base-200 transition-colors" onclick={toggleCollapse}>
          <Icon name={collapsed ? 'double-right' : 'double-left'} class="w-4 h-4" />
        </button>
      </div>
      
      <div class="navbar-center">
        <ul class="menu w-full menu-horizontal px-1">
          <li>
            <details class="dropdown">
              <summary class="btn btn-ghost btn-sm normal-case text-sm hover:bg-base-200">{$acc.name+'/'+$acc.account}</summary>
              <ul class="p-1.5 bg-base-100 rounded-md shadow-lg z-10 mt-1 w-48 border border-base-200">
                {#each $ctx.accounts as acc}
                  <li>
                    <a 
                      class={`rounded-md py-1.5 px-3 text-sm hover:bg-base-200 transition-colors ${acc.id === $save.current ? 'bg-primary/90 text-primary-content' : ''}`}
                      onclick={() => clickAccount(acc)}
                    >
                      {acc.name}/{acc.account}
                    </a>
                  </li>
                {/each}
              </ul>
            </details>
          </li>
        </ul>
      </div>
      
      <div class="navbar-end gap-1.5">
        <a href={localizeHref("/dash")} class="btn btn-ghost btn-sm normal-case text-sm hover:bg-base-200">{m.bot_list()}</a>
        <button class="btn btn-ghost btn-sm normal-case text-sm hover:bg-base-200" onclick={() => showAddBot = true}>{m.add_bot()}</button>
      </div>
    </header>
    {/if}
		
		<main class="flex-1 flex flex-col relative">
			{@render children()}

			<!-- 悬浮菜单控制按钮 - 仅在iframe模式下显示 -->
			{#if $site.isInIframe}
			<button
				class="fixed bottom-4 left-4 z-50 btn btn-circle btn-primary shadow-lg hover:shadow-xl transition-all duration-200 hover:scale-105"
				onclick={toggleCollapse}
			>
				<Icon name={collapsed ? 'double-right' : 'double-left'} class="w-5 h-5" />
			</button>
			{/if}
		</main>
	</div>
</div>
