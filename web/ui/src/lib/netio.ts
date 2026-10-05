import { $fetch, type SearchParameters, FetchError } from 'ofetch';
import _ from 'lodash';
import { site } from './stores/site';
import { get } from 'svelte/store';
import { acc } from './dash/store';
import { parseSeriesJSON } from './series/values';

export type ApiResult = Record<string, any> & {
	code: number;
	msg?: string;
};

type ApiType = 'GET' | 'POST' | 'PUT' | 'HEAD' | 'DELETE' | 'OPTIONS';

const requestApi = async function (
	method: ApiType,
	url: string,
	query?: SearchParameters,
	body?: RequestInit['body'] | Record<string, any>,
	headers?: Record<string, any>
): Promise<ApiResult> {
	try {
		if (url.startsWith('/')) {
			const siteShot = get(site);
			if (!siteShot.apiReady) {
				// api not ready, wait for it
				return new Promise((resolve) => {
					site.update((s) => {
						s.apiReadyCbs.push(() => {
							requestApi(method, url, query, body, headers).then(resolve);
						});
						return s;
					});
				});
			}
			url = `${siteShot.apiHost}/api${url}`;
		}
		site.update((s) => {
			s.loading = true;
			return s;
		});
		const rsp = await $fetch(url, {
			method,
			body,
			query,
			headers,
			...(url.endsWith('/kline/series') ? { parseResponse: parseSeriesJSON } : {})
		});
		site.update((s) => {
			s.loading = false;
			return s;
		});
		if (!_.isObject(rsp)) {
			return { code: 200, data: rsp };
		}
		const data = rsp as Record<string, any>;
		return { code: 200, msg: '', ...data };
	} catch (e) {
		site.update((s) => {
			s.loading = false;
			return s;
		});
		const err = e as FetchError;
		let msg = err.toString();
		if (typeof err.data === 'string') {
			msg = err.data;
		} else if (err.data && err.data.detail) {
			msg = `${err.status}: ${err.data.detail}`;
		} else if (err.data && err.data.msg) {
			msg = `${err.status}: ${err.data.msg}`;
		}
		return { code: err.status ?? 400, msg };
	}
};

export async function getApi(url: string, query?: SearchParameters): Promise<ApiResult> {
	return requestApi('GET', url, query, null);
}

export async function postApi(
	url: string,
	body: RequestInit['body'] | Record<string, any>,
	query?: SearchParameters
): Promise<ApiResult> {
	return requestApi('POST', url, query, body);
}

async function accApiRequest(
	method: ApiType,
	path: string,
	body?: RequestInit['body'] | Record<string, any>,
	query?: SearchParameters
): Promise<ApiResult> {
	const accShot = { ...get(acc) };
	if (!get(site).apiReady) {
		return new Promise((resolve) => {
			site.update((s) => {
				s.apiReadyCbs.push(() => {
					const current = get(acc);
					if (
						current.url !== accShot.url ||
						current.account !== accShot.account ||
						current.token !== accShot.token
					) {
						resolve({ code: 409, msg: 'account changed before request' });
						return;
					}
					accApiRequest(method, path, body, query).then(resolve);
				});
				return s;
			});
		});
	}
	if (!accShot.account || !accShot.token) {
		console.error(new Error('invalid account'), accShot);
		return { code: 401, msg: `invalid account: ${accShot}` };
	}
	const url = `${accShot.url}/api/bot${path}`;
	const headers = {
		'X-Account': accShot.account,
		'X-Authorization': 'Bearer ' + accShot.token
	};
	return requestApi(method, url, query, body, headers);
}

export async function getAccApi(path: string, query?: SearchParameters): Promise<ApiResult> {
	return accApiRequest('GET', path, null, query);
}

export async function postAccApi(
	path: string,
	body: RequestInit['body'] | Record<string, any>,
	query?: SearchParameters
): Promise<ApiResult> {
	return accApiRequest('POST', path, body, query);
}
