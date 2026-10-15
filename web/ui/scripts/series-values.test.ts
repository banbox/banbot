import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parseSeriesJSON, seriesCell } from '../src/lib/series/values.ts';

test('series display distinguishes missing, null, false, zero, empty string and JSON', () => {
	const values = {
		nullable: null,
		zero: 0,
		flag: false,
		label: '',
		extra: { flag: false, list: [0, null] }
	};
	assert.deepEqual(seriesCell(values, 'absent'), { kind: 'missing', text: 'Missing' });
	assert.deepEqual(seriesCell(values, 'nullable'), { kind: 'null', text: 'NULL' });
	assert.deepEqual(seriesCell(values, 'zero'), { kind: 'number', text: '0' });
	assert.deepEqual(seriesCell(values, 'flag'), { kind: 'boolean', text: 'false' });
	assert.deepEqual(seriesCell(values, 'label'), { kind: 'string', text: '""' });
	assert.deepEqual(seriesCell(values, 'extra'), {
		kind: 'json',
		text: '{"flag":false,"list":[0,null]}'
	});
	assert.equal(seriesCell({}, 'toString').kind, 'missing');
	assert.equal(values.extra.list[1], null);
});

test('series JSON keeps large integers exact and ordinary field types intact', () => {
	const parsed = parseSeriesJSON(
		'{"Values":{"large":9223372036854775807,"negative":-9223372036854775808,"normal":123,"float":1.25,"flag":false,"nil":null,"str":"9223372036854775807","json":{"n":9007199254740993}}}'
	) as { Values: Record<string, unknown> };
	assert.equal(parsed.Values.large, 9223372036854775807n);
	assert.equal(parsed.Values.negative, -9223372036854775808n);
	assert.equal(parsed.Values.normal, 123);
	assert.equal(parsed.Values.float, 1.25);
	assert.equal(parsed.Values.flag, false);
	assert.equal(parsed.Values.nil, null);
	assert.equal(parsed.Values.str, '9223372036854775807');
	assert.equal(seriesCell(parsed.Values, 'large').text, '9223372036854775807');
	assert.equal(seriesCell(parsed.Values, 'json').text, '{"n":9007199254740993}');
	assert.throws(() => parseSeriesJSON('{broken'), SyntaxError);
});

test('older JSON parsers fail explicitly instead of silently rounding integers', () => {
	const original = JSON.parse;
	JSON.parse = ((text, reviver) =>
		original(text, reviver && ((key, value) => reviver(key, value)))) as typeof JSON.parse;
	try {
		assert.throws(() => parseSeriesJSON('{"n":9223372036854775807}'), /Cannot safely decode/);
		assert.deepEqual(parseSeriesJSON('{"n":0,"flag":false}'), { n: 0, flag: false });
	} finally {
		JSON.parse = original;
	}
});
