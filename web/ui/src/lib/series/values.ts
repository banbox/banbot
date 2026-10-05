/** Preserve integer literals that JSON.parse would otherwise silently round. */
export function parseSeriesJSON(text: string): unknown {
	const parse = JSON.parse as (
		text: string,
		reviver: (key: string, value: unknown, context?: { source?: string }) => unknown
	) => unknown;
	return parse(text, (_key, value, context) => {
		if (typeof value === 'number' && Number.isInteger(value) && !Number.isSafeInteger(value)) {
			if (context?.source && /^-?\d+$/.test(context.source)) return BigInt(context.source);
			if (context?.source) return value; // Float/scientific literals retain number semantics.
			// Fail closed in browsers without JSON.parse source context.
			throw new Error('Cannot safely decode large integer series values; upgrade your browser.');
		}
		return value;
	});
}

export type ValueKind = 'missing' | 'null' | 'string' | 'number' | 'bigint' | 'boolean' | 'json';

// Render bigint as an exact JSON integer token, preserving nested value types.
function jsonText(value: unknown): string {
	if (typeof value === 'bigint') return String(value);
	if (Array.isArray(value)) return `[${value.map(jsonText).join(',')}]`;
	if (value !== null && typeof value === 'object') {
		return `{${Object.entries(value)
			.map(([key, item]) => `${JSON.stringify(key)}:${jsonText(item)}`)
			.join(',')}}`;
	}
	return JSON.stringify(value);
}

export function seriesCell(
	values: Record<string, unknown>,
	field: string
): { kind: ValueKind; text: string } {
	if (!Object.prototype.hasOwnProperty.call(values, field))
		return { kind: 'missing', text: 'Missing' };
	const value = values[field];
	if (value === null) return { kind: 'null', text: 'NULL' };
	if (typeof value === 'string') return { kind: 'string', text: JSON.stringify(value) };
	if (typeof value === 'object') {
		return {
			kind: 'json',
			text: jsonText(value)
		};
	}
	return { kind: typeof value as ValueKind, text: String(value) };
}
