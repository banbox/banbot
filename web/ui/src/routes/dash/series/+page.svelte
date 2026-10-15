<script lang="ts">
	import SeriesViewer from '$lib/series/SeriesViewer.svelte';
	import { getAccApi } from '$lib/netio';
	import { acc } from '$lib/dash/store';
</script>

{#key JSON.stringify([$acc.url, $acc.account, $acc.token])}
	<SeriesViewer request={getAccApi} />
{/key}
