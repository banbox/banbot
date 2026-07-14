<script lang="ts">
	import SeriesViewer from '$lib/series/SeriesViewer.svelte';
	import { getApi } from '$lib/netio';
</script>

<SeriesViewer request={getApi} />
