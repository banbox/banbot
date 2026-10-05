<script lang="ts">
	import * as m from '$lib/paraglide/messages.js';
	import type { UnifiedBacktestReport } from '$lib/dev/types';
	import { numericText, reportIdentity, reportStatus, reportBookAvailable } from './report';

	let { report }: { report: UnifiedBacktestReport } = $props();
	const identity = $derived(reportIdentity(report));
	const status = $derived(reportStatus(report));
</script>

<div class="flex flex-col gap-4">
	<div class="card bg-base-200">
		<div class="card-body">
			<h2 class="card-title">
				{m.overview()}
				<span
					class="badge"
					class:badge-success={status === 'complete'}
					class:badge-error={status !== 'complete'}>{status}</span
				>
				<span class="badge badge-outline">v{report.Version}</span>
			</h2>
			<div class="flex flex-wrap gap-x-6 gap-y-2 text-sm">
				<span>{m.result_engine()}: {identity.engines}</span>
				<span>{m.result_execution_mode()}: {identity.modes}</span>
				<span>{m.result_account_id()}: {identity.accounts}</span>
			</div>
			{#if report.Errors?.length}
				<div role="alert" class="alert alert-error">
					<pre class="whitespace-pre-wrap">{report.Errors.join('\n')}</pre>
				</div>
			{/if}
			{#if !report.Results?.length}<p>{m.result_no_results()}</p>{/if}
		</div>
	</div>

	{#each report.Results || [] as result, resultIndex (resultIndex)}
		<section class="card bg-base-200">
			<div class="card-body gap-4">
				<h3 class="card-title break-all">
					{m.result_strategy_id()}: {result.StrategyID || '-'}
					<span class="badge badge-outline">{result.Engine}</span>
					<span class="badge badge-outline">{result.Manifest.ExecutionMode || '-'}</span>
				</h3>
				<p class="text-sm">{m.result_account_id()}: {result.AccountID || '-'}</p>
				<div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
					{#each [[m.result_decisions(), result.Decisions], [m.result_executions(), result.Executions], [m.result_skipped(), result.Skipped], [m.result_incomplete(), result.Incomplete], [m.result_unresolved(), result.Unresolved], [m.result_targets_accepted(), result.TargetsAccepted], [m.result_fills(), result.Fills], [m.result_account_fills(), result.AccountFills]] as [label, value] (label)}
						<div class="rounded-box bg-base-100 p-3">
							<div class="text-xs opacity-60">{label}</div>
							<div class="text-lg font-semibold">{result.Engine === 'factor' ? value : '-'}</div>
						</div>
					{/each}
				</div>
				{#if result.Unresolved > 0}<div role="alert" class="alert alert-warning">
						{m.result_unresolved()}: {result.Unresolved}
					</div>{/if}

				<h4 class="font-semibold">{m.result_book()} · {result.Manifest.Currency || '-'}</h4>
				{#if result.Engine === 'factor' && reportBookAvailable(report)}
					<dl class="grid grid-cols-2 sm:grid-cols-3 gap-3">
						{#each [[m.result_nav(), result.Book.NAV], [m.result_cash(), result.Book.Cash], [m.tot_fee(), result.Book.Fees], [m.result_slippage(), result.Book.Slippage], [m.result_funding(), result.Book.Funding], [m.result_turnover(), result.Book.Turnover]] as [label, value] (label)}
							<div>
								<dt class="text-xs opacity-60">{label}</dt>
								<dd>
									{typeof value === 'number'
										? value.toLocaleString(undefined, { maximumFractionDigits: 8 })
										: '-'}
								</dd>
							</div>
						{/each}
					</dl>
					<details class="rounded-box bg-base-100 p-3">
						<summary class="cursor-pointer">{m.result_quantities()}</summary>
						<table class="table table-sm">
							<thead><tr><th>SID</th><th>{m.result_quantities()}</th></tr></thead>
							<tbody
								>{#each Object.entries(result.Book.Quantities || {}) as [sid, quantity] (sid)}<tr
										><td>{sid}</td><td>{quantity}</td></tr
									>{/each}</tbody
							>
						</table>
					</details>
				{:else if result.Engine === 'factor'}<p class="text-sm opacity-60">
						{m.result_book_unavailable()}
					</p>
				{:else}<p>-</p>{/if}

				<h4 class="font-semibold">{m.result_summary()}</h4>
				{#if Object.keys(result.Summary || {}).length}
					<div class="overflow-x-auto">
						<table class="table table-sm">
							<thead
								><tr
									><th>{m.result_column()}</th><th>{m.result_label()}</th><th
										>{m.result_sections()}</th
									><th>IC</th><th>RankIC</th><th>ICIR</th><th>RankICIR</th><th>Q1 → Q5</th></tr
								></thead
							>
							<tbody
								>{#each Object.entries(result.Summary || {}) as [column, labels] (column)}
									{#each Object.entries(labels) as [label, summary] (label)}
										<tr
											><td>{column}</td><td>{label}</td><td>{summary.Sections}</td>
											<td>{summary.Sections ? summary.MeanIC.toFixed(4) : '-'}</td><td
												>{summary.Sections ? summary.MeanRankIC.toFixed(4) : '-'}</td
											>
											<td>{numericText(summary.ICIR)}</td><td>{numericText(summary.RankICIR)}</td>
											<td>{summary.QuintileMean.map((value) => numericText(value)).join(' / ')}</td
											></tr
										>
									{/each}
								{/each}</tbody
							>
						</table>
					</div>
				{:else}<p class="text-sm opacity-60">{m.result_no_summary()}</p>{/if}

				<h4 class="font-semibold">{m.result_manifest()}</h4>
				<dl class="grid gap-2 text-sm break-all">
					{#each [['ManifestID', result.ManifestID], ['StrategyHash', result.StrategyHash], ['CodeRevision', result.Manifest.CodeRevision], ['FactorPlanHash', result.Manifest.FactorPlanHash], ['UniverseVersion', result.Manifest.UniverseVersion], ['VisibilityPolicy', result.Manifest.VisibilityPolicy], ['StaticUniverse', String(result.Manifest.StaticUniverse)], ['LatencyAssumption', result.Manifest.LatencyAssumption]] as [label, value] (label)}<div
						>
							<dt class="opacity-60">{label}</dt>
							<dd>{value || '-'}</dd>
						</div>{/each}
				</dl>
				<details class="rounded-box bg-base-100 p-3">
					<summary class="cursor-pointer">{m.result_manifest()}</summary>
					<pre class="overflow-auto max-h-96 text-xs mt-3">{JSON.stringify(
							result.Manifest,
							null,
							2
						)}</pre>
				</details>
			</div>
		</section>
	{/each}

	<details class="rounded-box bg-base-200 p-4">
		<summary class="cursor-pointer">{m.result_raw_json()}</summary>
		<pre class="overflow-auto max-h-[60vh] text-xs mt-3">{JSON.stringify(report, null, 2)}</pre>
	</details>
</div>
