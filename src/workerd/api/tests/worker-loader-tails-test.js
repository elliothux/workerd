// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0
import assert from 'node:assert/strict';
import { DurableObject, WorkerEntrypoint } from 'cloudflare:workers';

export class Collector extends DurableObject {
  events = [];
  waiting = new Map();
  tail(events) {
    this.events.push(...JSON.parse(JSON.stringify(events)));
    for (const [count, resolver] of this.waiting) {
      if (this.events.length >= count) {
        resolver.resolve(this.events);
        this.waiting.delete(count);
      }
    }
  }
  wait(count) {
    if (this.events.length >= count) return this.events;
    const resolver = Promise.withResolvers();
    this.waiting.set(count, resolver);
    return resolver.promise;
  }
}

export class FailingTail extends WorkerEntrypoint {
  tail() {
    throw new Error('intentional collector failure');
  }
}

export class StaticService extends WorkerEntrypoint {
  fetch() {
    console.log('static service');
    return new Response('static');
  }
}

const source = (modules, env = {}, tails = []) => ({
  compatibilityDate: '2026-08-30',
  mainModule: 'main.js',
  modules: { 'main.js': modules },
  env,
  tails,
  globalOutbound: null,
});

function tree(loader, userTail, service) {
  return source(
    `
    let count = 0;
    export default {
      async fetch(request, env) {
        const label = new URL(request.url).pathname;
        console.log('parent', label);
        const child = env.LOADER.get('child', () => ({
          compatibilityDate: '2026-08-30',
          mainModule: 'main.js',
          modules: { 'main.js': ${JSON.stringify(`
            export default {
              async fetch(request, env) {
                console.log('child', new URL(request.url).pathname);
                if (env.SERVICE) await env.SERVICE.fetch('https://example.test');
                return new Response('child');
              }
            };
          `)} },
          env: { SERVICE: env.SERVICE },
          tails: [env.USER_TAIL],
          globalOutbound: null,
        }));
        await (await child.getEntrypoint().fetch(request)).text();
        return Response.json({ count: ++count, keys: Object.keys(env).sort() });
      }
    };
  `,
    { LOADER: loader, USER_TAIL: userTail, SERVICE: service }
  );
}

function messages(events) {
  return events.flatMap((event) => event.logs.map((log) => log.message)).sort();
}

export const perInvocationCollectors = {
  async test(ctrl, env, ctx) {
    const user = ctx.exports.Collector.getByName('user');
    const first = ctx.exports.Collector.getByName('first');
    const second = ctx.exports.Collector.getByName('second');
    const loader = env.factory.get('tree');
    const worker = env.loader.get('tree', () =>
      tree(loader, user, ctx.exports.StaticService({ props: {} }))
    );
    for (const [tail, label, count] of [
      [first, '/first', 1],
      [second, '/second', 2],
    ]) {
      const entry = env.factory.getEntrypoint(worker, [tail]);
      assert.deepEqual(
        await (await entry.fetch('https://example.test' + label)).json(),
        {
          count,
          keys: ['LOADER', 'SERVICE', 'USER_TAIL'],
        }
      );
      assert.deepEqual(messages(await tail.wait(2)), [
        ['child', label],
        ['parent', label],
      ]);
    }
    assert.deepEqual(messages(await user.wait(2)), [
      ['child', '/first'],
      ['child', '/second'],
    ]);
    assert.deepEqual(messages(await first.wait(2)), [
      ['child', '/first'],
      ['parent', '/first'],
    ]);
  },
};

export const collectorFailureIsIndependent = {
  async test(ctrl, env, ctx) {
    const host = ctx.exports.Collector.getByName('failure-host');
    const worker = env.factory.get('failure').load(
      source(
        `
      export default { fetch() { console.log('successful response'); return new Response('ok'); } };
    `,
        {},
        [ctx.exports.FailingTail({ props: {} })]
      )
    );
    const entry = env.factory.getEntrypoint(worker, [host]);
    assert.equal(
      await (await entry.fetch('https://example.test')).text(),
      'ok'
    );
    assert.deepEqual(messages(await host.wait(1)), [['successful response']]);
  },
};

export const inheritedFixedCollector = {
  async test(ctrl, env, ctx) {
    const host = ctx.exports.Collector.getByName('fixed-host');
    const user = ctx.exports.Collector.getByName('fixed-user');
    const code = tree(env.factory.get('fixed'), user);
    code.tails = [host];
    const worker = env.inheritingLoader.load(code);
    await (
      await worker.getEntrypoint().fetch('https://example.test/fixed')
    ).text();
    assert.deepEqual(messages(await host.wait(2)), [
      ['child', '/fixed'],
      ['parent', '/fixed'],
    ]);
    assert.deepEqual(messages(await user.wait(1)), [['child', '/fixed']]);
  },
};

export const protectedEntrypointIsNotTransferable = {
  async test(ctrl, env, ctx) {
    const host = ctx.exports.Collector.getByName('transfer-host');
    const worker = env.loader.load(source('export default {}'));
    const entry = env.factory.getEntrypoint(worker, [host]);
    assert.throws(
      () => env.loader.load(source('export default {}', { entry })),
      /dynamically-loaded worker/
    );
    assert.throws(
      () => env.factory.getEntrypoint(worker, Array(17).fill(host)),
      /Too many host/
    );
  },
};

export const perInvocationRpcCollectors = {
  async test(ctrl, env, ctx) {
    const childCode = `
      import { WorkerEntrypoint } from 'cloudflare:workers';
      export default class extends WorkerEntrypoint {
        run(label) { console.log('rpc child', label); return label; }
      }
    `;
    const worker = env.loader.load(
      source(
        `
      import { WorkerEntrypoint } from 'cloudflare:workers';
      export default class extends WorkerEntrypoint {
        async run(label) {
          console.log('rpc parent', label);
          const child = this.env.LOADER.get('child', () => ({
            compatibilityDate: '2026-08-30', mainModule: 'main.js',
            modules: { 'main.js': ${JSON.stringify(childCode)} }, globalOutbound: null,
          }));
          using result = child.getEntrypoint().run(label);
          return await result;
        }
      }
    `,
        { LOADER: env.factory.get('rpc') }
      )
    );
    for (const label of ['first', 'second']) {
      const host = ctx.exports.Collector.getByName('rpc-' + label);
      {
        using result = env.factory.getEntrypoint(worker, [host]).run(label);
        assert.equal(await result, label);
      }
      assert.deepEqual(messages(await host.wait(2)), [
        ['rpc child', label],
        ['rpc parent', label],
      ]);
    }
  },
};
