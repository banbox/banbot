import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import {
	legacyMetric,
	numericText,
	readUnifiedReport,
	reportIdentity,
	reportStatus,
	reportBookAvailable,
	taskReport
} from './report.ts';

const result = {
	Engine: 'factor',
	StrategyID: 'alpha',
	AccountID: 'shared',
	Decisions: 2,
	Executions: 1,
	Skipped: 0,
	Incomplete: 1,
	Unresolved: 1,
	TargetsAccepted: 1,
	Fills: 3,
	AccountFills: 2,
	Book: {
		Cash: 90,
		NAV: 100,
		Fees: 0,
		Slippage: 0,
		Funding: -1,
		Turnover: 20,
		Quantities: { 101: 0.5 }
	},
	Manifest: { ExecutionMode: 'events' },
	Summary: null
};
const report = { Version: 1, Status: 'incomplete', Errors: ['cleanup failed'], Results: [result] };

test('reads PascalCase v1 and preserves error, unresolved and negative funding values', () => {
	const parsed = readUnifiedReport(report);
	assert.ok(parsed);
	assert.equal(parsed.Results[0].Unresolved, 1);
	assert.equal(parsed.Results[0].Book.Funding, -1);
	assert.deepEqual(parsed.Errors, ['cleanup failed']);
	assert.equal(readUnifiedReport({ ...report, Results: null })?.Results, null);
	assert.equal(readUnifiedReport({ ...report, Version: 2 }), null);
	assert.equal(readUnifiedReport({ version: 1, status: 'complete', results: [] }), null);
});

test('rejects broken nested data before tables can render it', () => {
	assert.equal(readUnifiedReport({ ...report, Results: [{ ...result, Book: null }] }), null);
	assert.equal(
		readUnifiedReport({ ...report, Results: [{ ...result, Summary: { score: { return: {} } } }] }),
		null
	);
});

test('accepts mature and empty summaries without treating invalid ICIR as zero', () => {
	const missing = { Value: null, Validity: 'missing' };
	const summary = {
		Sections: 0,
		MeanIC: 0,
		MeanRankIC: 0,
		ICIR: missing,
		RankICIR: missing,
		QuintileMean: Array(5).fill(missing)
	};
	const parsed = readUnifiedReport({
		...report,
		Results: [{ ...result, Summary: { score: { next: summary } } }]
	});
	assert.ok(parsed);
	assert.equal(parsed.Results[0].Summary.score.next.Sections, 0);
	assert.equal(numericText(parsed.Results[0].Summary.score.next.ICIR), 'missing');
});

test('task metadata supports actual flattened ToMap payload and legacy Info', () => {
	assert.equal(taskReport({ unified: true, run: report }).report?.Status, 'incomplete');
	assert.equal(
		taskReport({ info: JSON.stringify({ unified: true, run: report }) }).report?.Results.length,
		1
	);
	assert.deepEqual(taskReport({ info: 'ordinary task error' }), { unified: false, report: null });
	assert.deepEqual(taskReport({ unified: true, run: { Version: 2 } }), {
		unified: true,
		report: null
	});
});

test('mixed report identity deduplicates shared account without aggregating account fills', () => {
	const parsed = readUnifiedReport({
		...report,
		Results: [
			result,
			{ ...result, Engine: 'time_series', StrategyID: 'ts', Manifest: { ExecutionMode: '' } }
		]
	});
	assert.ok(parsed);
	assert.deepEqual(reportIdentity(parsed), {
		engines: 'factor, time_series',
		modes: 'events',
		accounts: 'shared'
	});
});

test('missing or invalid numeric views stay distinct from valid zero', () => {
	assert.equal(numericText({ Value: 0, Validity: 'valid' }), '0.0000');
	assert.equal(numericText({ Value: null, Validity: 'missing' }), 'missing');
	assert.equal(numericText({ Value: null, Validity: 'null' }), 'null');
	assert.equal(legacyMetric(0, false, 2), '0.00');
	assert.equal(legacyMetric(0, true, 2), '-');
	assert.equal(legacyMetric(undefined, false), '-');
});

test('unresolved, empty and cleanup-failed reports never display a complete badge', () => {
	assert.equal(reportStatus({ ...report, Status: 'complete' }), 'incomplete');
	assert.equal(
		reportStatus({ ...report, Status: 'complete', Errors: [], Results: [] }),
		'incomplete'
	);
	assert.equal(
		reportStatus({
			...report,
			Status: 'complete',
			Errors: [],
			Results: [{ ...result, Unresolved: 0 }]
		}),
		'complete'
	);
	assert.equal(
		reportStatus({ ...report, Status: 'complete', Errors: [], Results: [result] }),
		'incomplete'
	);
});

test('failed replay books are unavailable while finished runs with unresolved labels keep their book', () => {
	assert.equal(reportBookAvailable(report), false);
	assert.equal(reportBookAvailable({ ...report, Status: 'complete' }), false);
	assert.equal(reportBookAvailable({ ...report, Status: 'complete', Errors: [] }), true);
});
