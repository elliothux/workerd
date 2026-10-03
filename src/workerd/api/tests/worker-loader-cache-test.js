// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

import { strict as assert } from 'node:assert';

const main = `import {WorkerEntrypoint, env} from 'cloudflare:workers';
import {strict as assert} from 'node:assert';
export default class extends WorkerEntrypoint {
  async fetch() {
    assert.ok(caches instanceof CacheStorage);
    assert.ok(caches.default instanceof Cache);
    assert.equal(env.backend, undefined);
    assert.equal(this.env.backend, undefined);
    const key = 'https://cache.example/item';
    assert.equal(await caches.default.match(key), undefined);
    const response = new Response('default-body', {headers: {'cache-control':'max-age=60'}});
    const write = caches.default.put(key, response);
    assert.equal(response.bodyUsed, true);
    await write;
    assert.equal(await (await caches.default.match(key)).text(), 'default-body');
    const name = 'reports:中文 /';
    const named = await caches.open(name);
    const encoder = new TextEncoder();
    const stream = new ReadableStream({ start(controller) {
      controller.enqueue(encoder.encode('stream-'));
      controller.enqueue(encoder.encode('body'));
      controller.close();
    }});
    await named.put(key, new Response(stream, {headers: {'cache-control':'max-age=60'}}));
    const hit = await named.match(new Request(key, {method:'POST'}), {ignoreMethod:true});
    assert.equal(hit.headers.get('x-test-namespace'), encodeURIComponent(name));
    assert.equal(await hit.text(), 'stream-body');
    assert.equal(await (await caches.default.match(key)).text(), 'default-body');
    assert.equal(await named.match(new Request(key, {method:'POST'})), undefined);
    assert.equal(await named.delete(new Request(key, {method:'POST'})), false);
    assert.equal(await named.delete(new Request(key, {method:'POST'}), {ignoreMethod:true}), true);
    assert.equal(await named.delete(key), false);
    assert.equal(await named.match(key), undefined);
    await assert.rejects(async () => fetch('https://cache.example/'), /not permitted to access the internet/);
    return new Response('native-cache');
  }
}`;

export default {
  async test(ctrl, env) {
    const code = {
      mainModule: 'main.js',
      modules: { 'main.js': main },
      compatibilityDate: '2026-09-08',
      env: {},
      globalOutbound: null,
      openComputeCache: env.backend,
    };
    const publicLoader = env.factory.get('public');
    const privateLoader = env.factory.getPrivate('private');
    await assert.rejects(
      async () =>
        publicLoader.load(code).getEntrypoint().fetch('https://test.example/'),
      /host-issued/
    );
    const worker = privateLoader.load(code);
    assert.equal(
      await (
        await worker.getEntrypoint().fetch('https://test.example/')
      ).text(),
      'native-cache'
    );
    // Parent cache configuration is not an ambient capability for unconfigured children.
    const child = publicLoader.load({
      mainModule: 'main.js',
      modules: {
        'main.js': `export default {async fetch() {
      try { await caches.default.match('https://cache.example/'); } catch(error) {return new Response(error.message);}
      throw Error('child inherited cache');
    }}`,
      },
      compatibilityDate: '2026-09-08',
      globalOutbound: null,
    });
    assert.match(
      await (await child.getEntrypoint().fetch('https://test.example/')).text(),
      /No Cache was configured/
    );
  },
};
