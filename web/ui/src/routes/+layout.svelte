<script lang="ts">
	import { ParaglideJS } from '@inlang/paraglide-sveltekit'
	import { i18n } from '$lib/i18n.js'
	import { alerts } from '$lib/stores/alerts';
	import Alert from '$lib/Alert.svelte';
	import Modal from '$lib/kline/Modal.svelte';
	import { modals } from '$lib/stores/modals';
	import { setTimezone } from '$lib/dateutil';
	import {site} from '@/lib/stores/site';
	import { page } from '$app/stores'
	import { derived } from 'svelte/store';

	let { children } = $props();
	setTimezone('UTC')
    import "tailwindcss/tailwind.css";

	const path = derived(page, ($page) => $page.url.pathname);
	path.subscribe((new_val) => {
		site.update((s) => {
			s.path = i18n.route(new_val);
			return s;
		});
	});
</script>

<ParaglideJS {i18n}>
  {#if $site.loading}
    <div class="fixed inset-0 bg-white bg-opacity-20 z-50 flex items-center justify-center">
      <span class="loading loading-spinner loading-lg"></span>
    </div>
  {/if}
	<!-- 添加alerts显示区域 -->
	<div class="alerts-container z-[1000]">
		{#each $alerts as alert (alert.id)}
			<Alert type={alert.type} text={alert.text}/>
		{/each}
	</div>
	{#each $modals as modal (modal.id)}
		<Modal title={modal.title} buttons={modal.buttons} show={true} center={true}
			click={(tag) => modal.resolve(tag)}>{@html modal.text}</Modal>
	{/each}
	{@render children()}
</ParaglideJS>

<style global>
  .alerts-container {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    pointer-events: none; /* 允许点击alerts下面的元素 */
  }
  /*滚动条*/
  * {
    scrollbar-width: thin;
    scrollbar-color: #777 #ddd;
  }
	/*Chrome  Safari*/
	*::-webkit-scrollbar {
			width: 8px;
			height: 8px;
	}
	*::-webkit-scrollbar-track {
			background: #f5f5f5;
			border-radius: 10px;
	}
	*::-webkit-scrollbar-thumb {
			background: #c0c0c0;
			border-radius: 10px;
	}
	*::-webkit-scrollbar-thumb:hover {
			background: #a0a0a0;
	}

</style>