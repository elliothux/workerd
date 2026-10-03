// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

const entries = new Map();
export default {
  async fetch(request) {
    const namespace = request.headers.get('cf-cache-namespace') ?? '';
    const key = namespace + ':' + request.url;
    if (request.method === 'PUT') {
      const payload = await request.text();
      const end = payload.indexOf('\r\n\r\n');
      if (!payload.startsWith('HTTP/1.1 200 ') || end < 0)
        throw Error('invalid native payload');
      entries.set(key, payload.slice(end + 4));
      return new Response(null, { status: 204 });
    }
    if (request.method === 'GET') {
      if (request.headers.get('cache-control') !== 'only-if-cached')
        throw Error('invalid cache lookup');
      if (!entries.has(key))
        return new Response(null, {
          status: 504,
          headers: { 'cf-cache-status': 'MISS' },
        });
      return new Response(entries.get(key), {
        headers: { 'cf-cache-status': 'HIT', 'x-test-namespace': namespace },
      });
    }
    if (request.method === 'PURGE') {
      if (request.headers.get('x-real-ip') !== '127.0.0.1')
        throw Error('invalid native purge');
      return new Response(null, { status: entries.delete(key) ? 200 : 404 });
    }
    throw Error('unexpected cache operation');
  },
};
