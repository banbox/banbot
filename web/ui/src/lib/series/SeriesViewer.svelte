<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { alerts } from '$lib/stores/alerts';
	import { fmtDateStr } from '$lib/dateutil';
	import type { ApiResult } from '$lib/netio';

	type Request = (path: string, query?: Record<string, any>) => Promise<ApiResult>;
	type SourceStatus = { name: string; timeframe: string; table: string };
	type SeriesField = { name?: string; Name?: string; type?: string; Type?: string };
	type SeriesRow = Record<string, any>;

	interface Props {
		request: Request;
	}

	let { request }: Props = $props();
	const now = Date.now();
	let source = $state('');
	let sid = $state('');
	let timeFrame = $state('');
	let fields = $state('');
	let start = $state(toDateTimeLocal(now - 30 * 24 * 60 * 60 * 1000));
	let end = $state(toDateTimeLocal(now));
	let limit = $state(100);
	let loading = $state(false);
	let rows = $state<SeriesRow[]>([]);
	let metadata = $state<SeriesField[]>([]);
	let table = $state('');
	let sourceOptions = $state<SourceStatus[]>([]);

	function toDateTimeLocal(ms: number) {
		const value = new Date(ms);
		value.setMinutes(value.getMinutes() - value.getTimezoneOffset());
		return value.toISOString().slice(0, 16);
	}

	function toMS(value: string) {
		return value ? new Date(value).getTime() : 0;
	}

	function rowValues(row: SeriesRow): Record<string, any> {
		return row.Values ?? row.values ?? {};
	}

	function fieldName(field: SeriesField) {
		return field.name ?? field.Name ?? '';
	}

	function fieldType(field: SeriesField) {
		return field.type ?? field.Type ?? '';
	}

	function visibleFields() {
		const configured = metadata.map(fieldName).filter(Boolean);
		if (configured.length > 0) return configured;
		return Array.from(new Set(rows.flatMap((row) => Object.keys(rowValues(row))))).sort();
	}

	function valueText(value: any) {
		if (value === undefined || value === null) return '-';
		return typeof value === 'object' ? JSON.stringify(value) : String(value);
	}

	function rowTime(row: SeriesRow, key: 'TimeMS' | 'EndMS') {
		const lower = key === 'TimeMS' ? 'time_ms' : 'end_ms';
		const camel = key === 'TimeMS' ? 'timeMS' : 'endMS';
		return row[key] ?? row[camel] ?? row[lower] ?? 0;
	}

	async function loadSources() {
		const rsp = await request('/kline/data_sources');
		if (rsp.code === 200) {
			sourceOptions = rsp.data ?? [];
		}
	}

	async function loadSeries() {
		const targetSID = Number(sid);
		if (!source.trim() || !targetSID || !timeFrame.trim()) {
			alerts.error('Source, SID and timeframe are required');
			return;
		}
		const startMS = toMS(start);
		const endMS = toMS(end);
		if (!startMS || !endMS || startMS >= endMS) {
			alerts.error('Choose a valid time range');
			return;
		}

		loading = true;
		const rsp = await request('/kline/series', {
			source: source.trim(),
			sid: targetSID,
			timeframe: timeFrame.trim(),
			fields: fields.trim(),
			start: startMS,
			end: endMS,
			limit: Math.min(Math.max(Number(limit) || 100, 1), 1000)
		});
		loading = false;
		if (rsp.code !== 200) {
			alerts.error(rsp.msg || 'load series failed');
			return;
		}
		rows = rsp.data ?? [];
		metadata = rsp.fields ?? [];
		table = rsp.table ?? '';
	}

	onMount(async () => {
		source = page.url.searchParams.get('source') ?? source;
		sid = page.url.searchParams.get('sid') ?? sid;
		timeFrame =
			page.url.searchParams.get('tf') ?? page.url.searchParams.get('timeframe') ?? timeFrame;
		fields = page.url.searchParams.get('fields') ?? fields;
		await loadSources();
		if (source && sid && timeFrame) {
			await loadSeries();
		}
	});
</script>

<div class="flex min-h-0 flex-1 flex-col gap-4 p-4">
	<section class="border-b border-base-300 pb-4">
		<div class="mb-3 flex flex-wrap items-center justify-between gap-2">
			<h1 class="text-base font-semibold">Series Data</h1>
			<button
				class="btn btn-sm btn-outline"
				title="Refresh series data"
				onclick={loadSeries}
				disabled={loading}>Refresh</button
			>
		</div>
		<div class="grid grid-cols-1 gap-2 md:grid-cols-2 xl:grid-cols-4">
			<label class="fieldset">
				<span class="fieldset-legend">Source</span>
				<input
					class="input input-sm w-full"
					list="series-sources"
					bind:value={source}
					placeholder="source"
				/>
			</label>
			<label class="fieldset">
				<span class="fieldset-legend">SID</span>
				<input
					class="input input-sm w-full"
					type="number"
					min="1"
					bind:value={sid}
					placeholder="symbol id"
				/>
			</label>
			<label class="fieldset">
				<span class="fieldset-legend">Timeframe</span>
				<input class="input input-sm w-full" bind:value={timeFrame} placeholder="1h" />
			</label>
			<label class="fieldset">
				<span class="fieldset-legend">Fields</span>
				<input class="input input-sm w-full" bind:value={fields} placeholder="all fields" />
			</label>
			<label class="fieldset">
				<span class="fieldset-legend">Start</span>
				<input class="input input-sm w-full" type="datetime-local" bind:value={start} />
			</label>
			<label class="fieldset">
				<span class="fieldset-legend">End</span>
				<input class="input input-sm w-full" type="datetime-local" bind:value={end} />
			</label>
			<label class="fieldset">
				<span class="fieldset-legend">Rows</span>
				<input class="input input-sm w-full" type="number" min="1" max="1000" bind:value={limit} />
			</label>
			<div class="flex items-end">
				<button class="btn btn-sm btn-primary w-full" onclick={loadSeries} disabled={loading}
					>{loading ? 'Loading' : 'Query'}</button
				>
			</div>
		</div>
		<datalist id="series-sources">
			<option value="kline"></option>
			{#each sourceOptions as item}
				<option value={item.name}>{item.name} ({item.timeframe})</option>
			{/each}
		</datalist>
	</section>

	<section class="flex min-h-0 flex-1 flex-col gap-3">
		<div class="flex flex-wrap items-center gap-2 text-sm text-base-content/70">
			{#if table}<span>Table: {table}</span>{/if}
			<span>{rows.length} rows</span>
			{#each metadata as field}
				<span class="badge badge-sm badge-outline"
					>{fieldName(field)}{fieldType(field) ? ` (${fieldType(field)})` : ''}</span
				>
			{/each}
		</div>
		<div class="min-h-0 overflow-auto border border-base-300">
			<table class="table table-sm table-pin-rows">
				<thead>
					<tr>
						<th>Start</th>
						<th>End</th>
						<th>Closed</th>
						{#each visibleFields() as field}
							<th>{field}</th>
						{/each}
					</tr>
				</thead>
				<tbody>
					{#if rows.length === 0}
						<tr
							><td colspan={3 + visibleFields().length} class="text-base-content/60"
								>No data in this range.</td
							></tr
						>
					{:else}
						{#each rows as row}
							<tr>
								<td class="whitespace-nowrap">{fmtDateStr(rowTime(row, 'TimeMS'))}</td>
								<td class="whitespace-nowrap">{fmtDateStr(rowTime(row, 'EndMS'))}</td>
								<td>{(row.Closed ?? row.closed) ? 'yes' : 'no'}</td>
								{#each visibleFields() as field}
									<td class="max-w-80 break-all">{valueText(rowValues(row)[field])}</td>
								{/each}
							</tr>
						{/each}
					{/if}
				</tbody>
			</table>
		</div>
	</section>
</div>
