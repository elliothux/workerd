// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

import { strict as assert } from 'node:assert';
import { DurableObject, WorkerEntrypoint } from 'cloudflare:workers';

let bindingCalls = 0;
export class ProductTransport extends WorkerEntrypoint {
  fetch() {
    ++bindingCalls;
    return new Response(this.ctx.props.value);
  }
  count() {
    return bindingCalls;
  }
}

const pythonDo = `from workers import DurableObject
from js import Response, Promise
from asyncio import sleep
class Counter(DurableObject):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        assert env.POLICY_READY
        assert not hasattr(env, "PRIVATE")
        self.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counts(value INTEGER NOT NULL)")
    async def start_background(self):
        self.done = Promise.withResolvers()
        async def work():
            await sleep(0)
            self.done.resolve(42)
        self.ctx.waitUntil(work())
        return "started"
    async def background_result(self):
        return await self.done.promise
    async def increment(self):
        self.ctx.storage.sql.exec("INSERT INTO counts(value) VALUES (1)")
        return self.ctx.storage.sql.exec("SELECT count(*) AS n FROM counts").one().n
    async def fetch(self, request):
        return Response.new(str(await self.increment()))
`;

function pythonDoCode() {
  return {
    compatibilityDate: '2026-09-08',
    compatibilityFlags: [
      'python_workers',
      'disable_python_external_sdk',
      'python_dedicated_snapshot',
    ],
    mainModule: 'counter.py',
    modules: { 'counter.py': { py: pythonDo } },
    openComputePrivateEnv: { PRIVATE: 'host-secret' },
    openComputeHostPolicy: true,
    globalOutbound: null,
  };
}

export class PythonDoHost extends DurableObject {
  async run(snapshot, expected) {
    const loaded = this.env.factory.getPrivate('python-do').load({
      ...pythonDoCode(),
      openComputePythonSnapshot: snapshot,
    });
    const actorClass = loaded.getDurableObjectClass('Counter');
    const stub = this.ctx.facets.get('tenant', () => ({
      class: actorClass,
      id: 'logical-counter',
    }));
    assert.equal(await stub.start_background(), 'started');
    assert.equal(
      await Promise.race([
        stub.background_result(),
        scheduler.wait(1000).then(() => {
          throw Error('Durable Object waitUntil coroutine did not finish');
        }),
      ]),
      42
    );
    assert.equal(await stub.increment(), expected);
    assert.equal(
      await (await stub.fetch('https://counter.example/')).text(),
      String(expected + 1) + ':policy'
    );
  }
  end() {
    this.ctx.abort('test-host-reset');
  }
}

export const durableObject = {
  async test(ctrl, env, ctx) {
    const snapshot = await env.factory
      .getPrivate('python-do-prepare')
      .preparePython(pythonDoCode());
    const first = ctx.exports.PythonDoHost.getByName('counter');
    await first.run(snapshot, 1);
    await assert.rejects(
      first.end(),
      (error) => error.durableObjectReset === true
    );
    await ctx.exports.PythonDoHost.getByName('counter').run(snapshot, 3);
  },
};

const python = `from workers import WorkerEntrypoint, env
from js import Response, caches
assert env.POLICY_READY
assert not hasattr(env, "PRIVATE")
MARKER = str(env.SEED)
KV = env.KV
assert not hasattr(KV, "fetch")
class Default(WorkerEntrypoint):
    async def fetch(self, request):
        assert self.env.POLICY_READY
        assert not hasattr(self.env, "PRIVATE")
        assert not hasattr(self.env, "CACHE_BACKEND")
        key = 'https://cache.example/python'
        await caches.default.put(key, Response.new('python-default'))
        named = await caches.open('reports:中文 /')
        await named.put(key, Response.new('python-named'))
        assert await (await caches.default.match(key)).text() == 'python-default'
        assert await (await named.match(key)).text() == 'python-named'
        assert await named.delete(key)
        assert not await named.delete(key)
        assert await named.match(key) is None
        assert await KV.get('key') == str(self.env.SEED)
        return Response.new(self.env.SEED + ':' + MARKER)
    def test(self, ctrl):
        raise RuntimeError('Preparation invoked a user handler')
`;

export default {
  async test(ctrl, env, ctx) {
    const transport = ctx.exports.ProductTransport({
      props: { value: 'prepared' },
    });
    const code = {
      compatibilityDate: '2026-09-08',
      compatibilityFlags: [
        'python_workers',
        'disable_python_external_sdk',
        'python_dedicated_snapshot',
      ],
      mainModule: 'entry.py',
      modules: { 'entry.py': { py: python } },
      env: { SEED: 'prepared' },
      openComputeBindings: { KV: { kind: 'kvNamespace', fetcher: transport } },
      openComputePrivateEnv: { PRIVATE: 'host-secret' },
      openComputeHostPolicy: true,
      openComputeCache: env.cacheBackend,
      globalOutbound: null,
    };
    const publicLoader = env.factory.get('public');
    const loader = env.factory.getPrivate('private');
    await assert.rejects(
      async () => publicLoader.preparePython(code),
      /host-issued/
    );
    await assert.rejects(
      async () => loader.preparePython({ ...code, mainModule: 'main.js' }),
      /Python main module/
    );
    await assert.rejects(
      async () =>
        loader.preparePython({
          ...code,
          openComputePythonSnapshot: new Uint8Array(16),
        }),
      /existing snapshot/
    );
    await assert.rejects(
      async () =>
        loader.preparePython({
          ...code,
          compatibilityFlags: [
            'python_workers',
            'disable_python_dedicated_snapshot',
          ],
        }),
      /dedicated snapshots/
    );
    const snapshot = await loader.preparePython(code);
    assert.ok(snapshot instanceof Uint8Array && snapshot.length >= 16);
    assert.equal(await transport.count(), 0);
    await assert.rejects(
      () =>
        loader.preparePython({
          ...code,
          modules: {
            'entry.py': { py: "from workers import env\nenv.KV.get('key')" },
          },
        }),
      /Disallowed operation called within global scope/
    );
    assert.equal(await transport.count(), 0);
    // Change only this primitive's env to prove top-level imports are not run during restore.
    // Product deployment identity forbids reusing a snapshot across differing env generations.
    const restored = {
      ...code,
      env: { SEED: 'restored' },
      openComputeBindings: {
        KV: {
          kind: 'kvNamespace',
          fetcher: ctx.exports.ProductTransport({
            props: { value: 'restored' },
          }),
        },
      },
      openComputePythonSnapshot: snapshot,
    };
    for (let i = 0; i < 2; ++i) {
      const response = await loader
        .load(restored)
        .getEntrypoint()
        .fetch('https://python.example/');
      assert.equal(await response.text(), 'restored:prepared:policy');
    }
    assert.equal(await transport.count(), 2);
    await assert.rejects(
      async () =>
        publicLoader
          .load(restored)
          .getEntrypoint()
          .fetch('https://python.example/'),
      /host-issued/
    );
    assert.throws(
      () =>
        loader.load({
          ...restored,
          openComputePythonSnapshot: new Uint8Array(15),
        }),
      /snapshot size/
    );
    await assert.rejects(
      async () =>
        loader
          .load({
            ...restored,
            openComputePythonSnapshot: new Uint8Array(16),
          })
          .getEntrypoint()
          .fetch('https://python.example/'),
      /snapshot|SyntaxError|JSON|Failed/
    );
    await assert.rejects(
      async () =>
        loader.preparePython({
          ...code,
          modules: { 'entry.py': { py: 'import nonexistent_module' } },
        }),
      /ModuleNotFoundError/
    );
    await assert.rejects(
      async () =>
        loader.preparePython({
          ...code,
          modules: { 'entry.py': { py: 'import uuid\nvalue = uuid.uuid4()' } },
        }),
      /Randomness is not allowed/
    );
  },
};
