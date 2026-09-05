// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0
import assert from 'node:assert/strict';
import { WorkerEntrypoint, DurableObject } from 'cloudflare:workers';

// All barriers belong to one actor IoContext; ordinary requests must not await another
// request's JavaScript promises through module globals.
export class Gate extends DurableObject {
  gates = new Map();
  initialize(limit) {
    for (let i = 0; i <= limit; ++i) {
      this.gates.set(i, {
        expected: i === 0 ? 2 : 1,
        arrived: 0,
        entered: Promise.withResolvers(),
        finish: Promise.withResolvers(),
      });
    }
  }
  async wait(id) {
    const gate = this.gates.get(id);
    if (++gate.arrived === gate.expected) gate.entered.resolve();
    await gate.finish.promise;
    return id;
  }
  async entered(id) {
    await this.gates.get(id).entered.promise;
  }
  finish(id) {
    this.gates.get(id).finish.resolve();
  }
  finishAll() {
    for (const gate of this.gates.values()) gate.finish.resolve();
  }
}

function code(value, env = {}) {
  return {
    compatibilityDate: '2026-08-30',
    mainModule: 'main.js',
    modules: {
      'main.js': `
        import { WorkerEntrypoint } from 'cloudflare:workers';
        export default class extends WorkerEntrypoint {
          value() { return ${JSON.stringify(value)}; }
        }
      `,
    },
    env,
    globalOutbound: null,
  };
}

function parent(loader) {
  const source = code(null, { LOADER: loader });
  source.compatibilityFlags = ['nodejs_compat'];
  source.modules['main.js'] = `
    import { WorkerEntrypoint } from 'cloudflare:workers';
    export default class extends WorkerEntrypoint {
      async run(id, value) {
        const child = this.env.LOADER.get(id, () => ({
          compatibilityDate: '2026-08-30',
          mainModule: 'main.js',
          modules: { 'main.js': 'export default { fetch() { return new Response(' +
            JSON.stringify(value) + '); } };' },
          globalOutbound: null,
        }));
        if (typeof child.getEntrypoint !== 'function') throw new Error('not synchronous');
        return await (await child.getEntrypoint().fetch('https://example.test')).text();
      }
      kind() {
        return [this.env.LOADER.constructor.name, typeof this.env.LOADER.load,
          typeof this.env.LOADER.get, typeof this.env.LOADER.revoke];
      }
      async delegateAgain() {
        this.env.LOADER.load({
          compatibilityDate: '2026-08-30',
          mainModule: 'main.js',
          modules: { 'main.js': 'export default {}' },
          env: { LOADER: this.env.LOADER },
        });
      }
    }
  `;
  return source;
}

export const nativeNestedLoader = {
  async test(ctrl, env) {
    const worker = env.system
      .load(parent(env.factory.get('native')))
      .getEntrypoint();
    assert.deepEqual(await worker.kind(), [
      'WorkerLoader',
      'function',
      'function',
      'undefined',
    ]);
    assert.equal(await worker.run('child', 'nested'), 'nested');
    await assert.rejects(worker.delegateAgain(), /cannot be delegated again/);
  },
};

export const namespaceAndFactoryIsolation = {
  async test(ctrl, env) {
    const first = env.factory.get('first');
    const second = env.factory.get('second');
    const other = env.otherFactory.get('first');
    assert.equal(
      await first
        .get('same', () => code('first'))
        .getEntrypoint()
        .value(),
      'first'
    );
    assert.equal(
      await second
        .get('same', () => code('second'))
        .getEntrypoint()
        .value(),
      'second'
    );
    assert.equal(
      await other
        .get('same', () => code('other'))
        .getEntrypoint()
        .value(),
      'other'
    );
    assert.equal(
      await env.system
        .get('same', () => code('system'))
        .getEntrypoint()
        .value(),
      'system'
    );
    // Reacquiring an authorized namespace preserves its cache opportunity within the process.
    assert.equal(
      await env.factory
        .get('first')
        .get('same', () => code('first'))
        .getEntrypoint()
        .value(),
      'first'
    );
  },
};

export const systemAndFactoryNotTransferable = {
  async test(ctrl, env) {
    assert.throws(
      () => env.system.load(parent(env.system)),
      /cannot be delegated/
    );
    assert.throws(() => env.system.load(parent(env.factory)), {
      name: 'DataCloneError',
    });
    assert.throws(() => structuredClone(env.factory.get('clone')), {
      name: 'DataCloneError',
    });
    assert.throws(() => env.factory.get(''), /between 1 and 256/);
    assert.throws(() => env.factory.get('x'.repeat(257)), /between 1 and 256/);
  },
};

export const failedStartupCanRetry = {
  async test(ctrl, env) {
    const loader = env.factory.get('retry');
    await assert.rejects(
      loader
        .get('same', async () => {
          throw new Error('callback failed');
        })
        .getEntrypoint()
        .value(),
      /callback failed/
    );
    assert.equal(
      await loader
        .get('same', () => code('retry'))
        .getEntrypoint()
        .value(),
      'retry'
    );
    await assert.rejects(
      loader
        .get('invalid', () => ({ ...code('invalid'), limits: {} }))
        .getEntrypoint()
        .value(),
      /resource limits are not supported/
    );
    assert.equal(
      await loader
        .get('invalid', () => code('valid'))
        .getEntrypoint()
        .value(),
      'valid'
    );
  },
};

export const revokeRetainedCapabilities = {
  async test(ctrl, env) {
    const loader = env.factory.get('revoked');
    const stub = loader.get('child', () => code('old'));
    const entrypoint = stub.getEntrypoint();
    assert.equal(await entrypoint.value(), 'old');
    env.factory.revoke('revoked');
    assert.throws(
      () => loader.load(code('denied')),
      /namespace has been revoked/
    );
    await assert.rejects(entrypoint.value(), /namespace has been revoked/);
    await assert.rejects(
      stub.getEntrypoint().value(),
      /namespace has been revoked/
    );
    const replacement = env.factory.get('revoked');
    assert.equal(
      await replacement
        .get('child', () => code('new'))
        .getEntrypoint()
        .value(),
      'new'
    );
    env.factory.revoke('absent');
  },
};

export const nestedCallbackGc = {
  async test(ctrl, env) {
    const loader = env.factory.get('gc');
    let stub = loader.get(null, async () => {
      stub = null;
      gc();
      return code('alive');
    });
    const response = stub.getEntrypoint().value();
    assert.equal(await response, 'alive');
  },
};

export const scopedDynamicLoopbackLifetime = {
  async test(ctrl, env) {
    const loader = env.factory.get('scoped-loopback');
    const parentCode = code(null, { LOADER: loader });
    parentCode.modules['main.js'] = `
      import { WorkerEntrypoint } from 'cloudflare:workers';
      export class Scoped extends WorkerEntrypoint {
        fetch(request) {
          return new Response(this.ctx.props.prefix + new URL(request.url).pathname);
        }
      }
      export default class extends WorkerEntrypoint {
        async initialize() {
          const scoped = this.ctx.exports.Scoped({ props: { prefix: 'allowed:' } });
          const child = this.env.LOADER.get('child', () => ({
            compatibilityDate: '2026-08-30', mainModule: 'main.js',
            modules: { 'main.js': 'export default { fetch(request, env) { return env.SCOPED.fetch(request); } }' },
            env: { SCOPED: scoped }, globalOutbound: null,
          }));
          return (await child.getEntrypoint().fetch('https://example.test/first')).text();
        }
      }
    `;
    assert.equal(
      await env.system.load(parentCode).getEntrypoint().initialize(),
      'allowed:/first'
    );
    gc();
    // The child's explicit service capability retains the unnamed parent after its handle and
    // invocation are gone. The callback remains immutable if the cache is cold.
    const child = loader.get('child', () => {
      throw new Error('unexpected eviction');
    });
    assert.equal(
      await (
        await child.getEntrypoint().fetch('https://example.test/retained')
      ).text(),
      'allowed:/retained'
    );
    env.factory.revoke('scoped-loopback');
  },
};

async function checkInflight(env, ctx, limit, facets = false) {
  const loader = env.factory.get('inflight-' + limit + '-' + facets);
  const gate = ctx.exports.Gate.getByName('inflight-' + limit + '-' + facets);
  await gate.initialize(limit);
  const workerCode = {
    compatibilityDate: '2026-08-30',
    mainModule: 'main.js',
    modules: {
      'main.js': `
          import { WorkerEntrypoint, DurableObject } from 'cloudflare:workers';
          ${facets ? 'export class Child extends DurableObject' : 'export default class extends WorkerEntrypoint'} {
            run(id) { return this.env.GATE.wait(id); }
          }
        `,
    },
    env: { GATE: gate },
    globalOutbound: null,
  };
  const invoke = (i) => {
    const stub = loader.get(String(i), () => workerCode);
    return facets
      ? ctx.facets
          .get('child-' + i, () => ({
            class: stub.getDurableObjectClass('Child'),
          }))
          .run(i)
      : stub.getEntrypoint().run(i);
  };
  const pending = [];
  try {
    for (let i = 0; i < limit; ++i) {
      pending.push(invoke(i));
    }
    pending.push(invoke(0));
    await Promise.race([
      Promise.all(Array.from({ length: limit }, (_, i) => gate.entered(i))),
      Promise.all(pending).then(() =>
        assert.fail('requests ended before admission barriers')
      ),
    ]);
    await assert.rejects(
      invoke(limit),
      /Too many distinct Dynamic Workers in flight/
    );
    await gate.finish(0);
    assert.deepEqual(await Promise.all([pending[0], pending[limit]]), [0, 0]);
    // RpcPromise is also a pipeline stub. Dispose it to end the sessions, independently of GC.
    pending[0][Symbol.dispose]();
    pending[limit][Symbol.dispose]();
    await gate.entered(0);
    const replacement = invoke(limit);
    pending.push(replacement);
    await Promise.race([
      gate.entered(limit),
      replacement.then(() =>
        assert.fail('replacement ended before admission barrier')
      ),
    ]);
    await gate.finish(limit);
    assert.equal(await replacement, limit);
  } finally {
    await gate.finishAll();
    await Promise.allSettled(pending);
    for (const request of pending) request[Symbol.dispose]();
  }
}

export const distinctInflightAndRelease = {
  async test(ctrl, env, ctx) {
    await checkInflight(env, ctx, 4);
  },
};

export class InflightActor extends DurableObject {
  async checkFacets() {
    await checkInflight(this.env, this.ctx, 10, true);
    return 'passed';
  }
  async check() {
    await checkInflight(this.env, this.ctx, 10);
    return 'passed';
  }
}

export const durableObjectInflight = {
  async test(ctrl, env, ctx) {
    assert.equal(
      await ctx.exports.InflightActor.getByName('inflight').check(),
      'passed'
    );
  },
};

export const boundedCachePreservesLiveHandles = {
  async test(ctrl, env) {
    const loader = env.factory.get('bounded-cache');
    let handles = [];
    for (let i = 0; i < 64; ++i) {
      const stub = loader.get(String(i), () => code(String(i)));
      assert.equal(await stub.getEntrypoint().value(), String(i));
      handles.push(stub);
    }
    assert.throws(
      () => loader.get('overflow', () => code('overflow')),
      /cache capacity exceeded/
    );
    const retained = handles[0];
    handles = null;
    gc();
    assert.equal(
      await loader
        .get('replacement', () => code('replacement'))
        .getEntrypoint()
        .value(),
      'replacement'
    );
    assert.equal(await retained.getEntrypoint().value(), '0');
    env.factory.revoke('bounded-cache');
    await assert.rejects(
      retained.getEntrypoint().value(),
      /namespace has been revoked/
    );
  },
};

export const boundedNamespacesAndCleanup = {
  async test(ctrl, env) {
    for (let i = 0; i < 1024; ++i) env.capacityFactory.get(String(i));
    assert.throws(
      () => env.capacityFactory.get('overflow'),
      /namespace capacity exceeded/
    );
    env.capacityFactory.revoke('0');
    const replacement = env.capacityFactory.get('replacement');
    assert.equal(
      await replacement.load(code('replacement')).getEntrypoint().value(),
      'replacement'
    );
    for (let i = 1; i < 1024; ++i) env.capacityFactory.revoke(String(i));
    env.capacityFactory.revoke('replacement');
  },
};

export const revokeDrainsAdmittedRequests = {
  async test(ctrl, env, ctx) {
    const loader = env.factory.get('drain');
    const gate = ctx.exports.Gate.getByName('drain');
    await gate.initialize(1);
    const source = code(null, { GATE: gate });
    source.modules['main.js'] = `
      import { WorkerEntrypoint } from 'cloudflare:workers';
      export default class extends WorkerEntrypoint {
        run() { return this.env.GATE.wait(1); }
      }
    `;
    const entrypoint = loader.load(source).getEntrypoint();
    const request = entrypoint.run();
    try {
      await Promise.race([
        gate.entered(1),
        request.then(() => assert.fail('request ended early')),
      ]);
      env.factory.revoke('drain');
      await assert.rejects(entrypoint.run(), /namespace has been revoked/);
      await gate.finish(1);
      assert.equal(await request, 1);
    } finally {
      await gate.finishAll();
      request[Symbol.dispose]();
    }
  },
};

export const facetInflight = {
  async test(ctrl, env, ctx) {
    assert.equal(
      await ctx.exports.InflightActor.getByName('facets').checkFacets(),
      'passed'
    );
  },
};

export class FactoryPeer extends WorkerEntrypoint {
  async read(key, value) {
    return await this.env.factory
      .get(key)
      .get('shared', () => code(value))
      .getEntrypoint()
      .value();
  }
  revoke(key) {
    this.env.factory.revoke(key);
  }
}

export const sharedFactoryAcrossServices = {
  async test(ctrl, env) {
    const loader = env.factory.get('shared-factory');
    const original = loader.get('shared', () => code('first')).getEntrypoint();
    assert.equal(await original.value(), 'first');
    assert.equal(await env.peer.read('shared-factory', 'second'), 'first');
    await env.peer.revoke('shared-factory');
    await assert.rejects(original.value(), /namespace has been revoked/);
    assert.equal(await env.peer.read('shared-factory', 'second'), 'second');
    await assert.rejects(original.value(), /namespace has been revoked/);
    env.factory.revoke('shared-factory');
  },
};

export const httpStreamsRetainAndReleaseInflight = {
  async test(ctrl, env, ctx) {
    const loader = env.factory.get('http-streams');
    const gate = ctx.exports.Gate.getByName('http-streams');
    await gate.initialize(4);
    const source = code(null, { GATE: gate });
    source.modules['main.js'] = `
      export default {
        fetch(request, env) {
          const id = Number(new URL(request.url).pathname.slice(1));
          return new Response(new ReadableStream({
            async start(controller) {
              controller.enqueue(new Uint8Array([id]));
              await env.GATE.wait(id);
              controller.close();
            },
          }));
        }
      };
    `;
    const fetchChild = (id, gateId = id) =>
      loader
        .get(String(id), () => source)
        .getEntrypoint()
        .fetch('https://example.test/' + gateId);
    const responses = [];
    try {
      for (let i = 0; i < 4; ++i) responses.push(await fetchChild(i));
      const sameChild = await fetchChild(0);
      responses.push(sameChild);
      for (let i = 0; i < 4; ++i) await gate.entered(i);
      await assert.rejects(fetchChild(4), /Too many distinct Dynamic Workers/);
      await gate.finish(1);
      assert.deepEqual(
        new Uint8Array(await responses[1].arrayBuffer()),
        new Uint8Array([1])
      );
      // A completed HTTP response releases its slot while other bodies are still open.
      await gate.finish(4);
      assert.deepEqual(
        new Uint8Array(await (await fetchChild(4)).arrayBuffer()),
        new Uint8Array([4])
      );
      await responses[2].body.cancel();
      // Canceling the caller's body releases its transport lease, even when the child still
      // owns asynchronous work. This does not assert general cancellation of tenant JavaScript.
      assert.deepEqual(
        new Uint8Array(await (await fetchChild(5, 4)).arrayBuffer()),
        new Uint8Array([4])
      );
      await gate.finishAll();
      for (const response of responses) {
        if (!response.bodyUsed) await response.arrayBuffer();
      }
    } finally {
      await gate.finishAll();
      for (const response of responses) {
        if (!response.bodyUsed) await response.body.cancel();
      }
      env.factory.revoke('http-streams');
    }
  },
};
