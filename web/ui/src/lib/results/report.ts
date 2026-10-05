import type { FactorNumeric, UnifiedBacktestReport } from '../dev/types';

function object(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function numeric(value: unknown): boolean {
	return (
		object(value) &&
		typeof value.Validity === 'string' &&
		(value.Value === null || (typeof value.Value === 'number' && Number.isFinite(value.Value)))
	);
}

// One wire boundary for bt_detail and the JSON stored in BtTask.info.
export function readUnifiedReport(value: unknown): UnifiedBacktestReport | null {
	if (
		!object(value) ||
		value.Version !== 1 ||
		!['complete', 'incomplete'].includes(String(value.Status))
	)
		return null;
	if (value.Results !== null && !Array.isArray(value.Results)) return null;
	if (
		value.Errors !== undefined &&
		(!Array.isArray(value.Errors) || !value.Errors.every((e) => typeof e === 'string'))
	)
		return null;
	for (const result of value.Results || []) {
		if (
			!object(result) ||
			!['Engine', 'StrategyID', 'AccountID'].every((key) => typeof result[key] === 'string')
		)
			return null;
		if (
			![
				'Decisions',
				'Executions',
				'Skipped',
				'Incomplete',
				'Unresolved',
				'TargetsAccepted',
				'Fills',
				'AccountFills'
			].every((key) => typeof result[key] === 'number' && Number.isFinite(result[key]))
		)
			return null;
		if (
			!object(result.Book) ||
			!object(result.Manifest) ||
			typeof result.Manifest.ExecutionMode !== 'string'
		)
			return null;
		const book = result.Book;
		if (
			!['Cash', 'NAV', 'Fees', 'Slippage', 'Funding', 'Turnover'].every(
				(key) => typeof book[key] === 'number' && Number.isFinite(book[key])
			)
		)
			return null;
		if (
			book.Quantities !== null &&
			(!object(book.Quantities) ||
				!Object.values(book.Quantities).every((q) => typeof q === 'number' && Number.isFinite(q)))
		)
			return null;
		if (result.Summary !== null && !object(result.Summary)) return null;
		for (const labels of Object.values(result.Summary || {})) {
			if (!object(labels)) return null;
			for (const summary of Object.values(labels)) {
				if (
					!object(summary) ||
					!['Sections', 'MeanIC', 'MeanRankIC'].every(
						(key) => typeof summary[key] === 'number' && Number.isFinite(summary[key])
					)
				)
					return null;
				if (
					!numeric(summary.ICIR) ||
					!numeric(summary.RankICIR) ||
					!Array.isArray(summary.QuintileMean) ||
					summary.QuintileMean.length !== 5 ||
					!summary.QuintileMean.every(numeric)
				)
					return null;
			}
		}
	}
	return value as unknown as UnifiedBacktestReport;
}

export function taskReport(task: { info?: string; unified?: boolean; run?: unknown }): {
	unified: boolean;
	report: UnifiedBacktestReport | null;
} {
	if (task.unified === true) return { unified: true, report: readUnifiedReport(task.run) };
	try {
		const value: unknown = JSON.parse(task.info || '{}');
		if (!object(value)) return { unified: false, report: null };
		return { unified: value.unified === true, report: readUnifiedReport(value.run) };
	} catch {
		return { unified: false, report: null };
	}
}

export function reportIdentity(report: UnifiedBacktestReport): {
	engines: string;
	modes: string;
	accounts: string;
} {
	const unique = (values: string[]) => [...new Set(values.filter(Boolean))].join(', ') || '-';
	const results = report.Results || [];
	return {
		engines: unique(results.map((result) => result.Engine)),
		modes: unique(results.map((result) => result.Manifest.ExecutionMode)),
		accounts: unique(results.map((result) => result.AccountID))
	};
}

// Match the backend task collector: unresolved labels and cleanup errors fail
// a task even when the raw run status was published as complete.
export function reportStatus(report: UnifiedBacktestReport): 'complete' | 'incomplete' {
	return report.Status === 'complete' &&
		!report.Errors?.length &&
		!!report.Results?.length &&
		report.Results.every((result) => result.Unresolved === 0)
		? 'complete'
		: 'incomplete';
}

export function reportBookAvailable(report: UnifiedBacktestReport): boolean {
	// v1 has no per-result availability flag; failures before replay completion
	// publish an uninitialized Book. Keep those numbers in the raw artifact only.
	return report.Status === 'complete' && !report.Errors?.length;
}

export function numericText(value: FactorNumeric | undefined): string {
	if (!value) return '-';
	return value.Validity === 'valid' && value.Value !== null && Number.isFinite(value.Value)
		? value.Value.toFixed(4)
		: value.Validity || '-';
}

export function legacyMetric(
	value: number | undefined,
	unified: boolean,
	digits?: number,
	suffix = ''
): string {
	if (unified || value === undefined || !Number.isFinite(value)) return '-';
	return (digits === undefined ? String(value) : value.toFixed(digits)) + suffix;
}
