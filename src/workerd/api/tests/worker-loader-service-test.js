import assert from 'node:assert/strict';
import { WorkerEntrypoint, RpcStub } from 'cloudflare:workers';

export class Backend extends WorkerEntrypoint {
  fetch(request) {
    assert.equal(request.headers.get('x-policy'), 'trusted');
    return new Response('service');
  }
  echo(value) {
    return value;
  }
}

const main = `import assert from 'node:assert/strict';
import { WorkerEntrypoint, RpcStub } from 'cloudflare:workers';
export default class extends WorkerEntrypoint {
  async check() {
    const service = this.env.SERVICE;
    assert.equal(service.constructor.name, 'Fetcher');
    const nativeBase = Object.getPrototypeOf(Object.getPrototypeOf(service));
    assert.equal(nativeBase.constructor.name, 'Fetcher');
    assert.throws(() => new service.constructor(), /Illegal constructor/i);
    assert.deepEqual(Object.keys(service), []);
    assert.equal(service.then, undefined);
    assert.throws(() => service.rpc, /SERVICE_BINDING_DENIED/);
    assert.throws(() => service.transport, /SERVICE_BINDING_DENIED/);
    assert.throws(() => service.__openComputeServiceRpc, /SERVICE_BINDING_DENIED/);
    await assert.rejects(async () => service.queue('q', []), /SERVICE_BINDING_DENIED/);
    await assert.rejects(async () => service.scheduled(), /SERVICE_BINDING_DENIED/);
    await assert.rejects(async () => nativeBase.fetch.call(service, 'https://service/'), /SERVICE_BINDING_DENIED/);
    assert.throws(() => nativeBase.connect.call(service, 'service:443'), /SERVICE_BINDING_DENIED/);
    assert.deepEqual(await service.echo({nested: [1, true]}), {nested: [1, true]});
    assert.equal(await (await service.fetch('https://service/')).text(), 'service');
    const pipeline = service.target().echo;
    assert.equal(pipeline.constructor.name, 'RpcProperty');
    assert.equal(await pipeline(43), 43);
    assert.equal(await service.callback()(44), 44);
    for (const name of ['target', 'callback']) {
      const pending = service[name]();
      assert.equal(pending.constructor.name, 'RpcPromise');
      const stub = await pending;
      assert.equal(stub.constructor, RpcStub);
      assert.equal(Object.getPrototypeOf(stub), RpcStub.prototype);
      assert.ok(stub instanceof RpcStub);
      assert.equal(stub.then, undefined);
      const copy = RpcStub.prototype.dup.call(stub);
      const held = name === 'target' ? stub.echo : stub;
      assert.equal(await held(41), 41);
      if (name === 'target') await assert.rejects(stub(41), TypeError);
      const failure = stub.fail();
      assert.equal(failure.constructor.name, 'RpcPromise');
      await assert.rejects(failure, {name: 'TypeError', message: 'fixture rejection'});
      await assert.rejects(failure.child(), {name: 'TypeError', message: 'fixture rejection'});
      RpcStub.prototype[Symbol.dispose].call(stub);
      RpcStub.prototype[Symbol.dispose].call(stub);
      if (name === 'target') await assert.rejects(stub.echo(1), /RPC stub used after being disposed/);
      else await assert.rejects(stub(1), /SERVICE_BINDING_DENIED/);
      const rejected = held(1);
      assert.equal(rejected.constructor.name, 'RpcPromise');
      await assert.rejects(rejected, /SERVICE_BINDING_DENIED/);
      assert.throws(() => RpcStub.prototype.dup.call(stub), /SERVICE_BINDING_DENIED/);
      assert.equal(await (name === 'target' ? copy.echo(42) : copy(42)), 42);
      RpcStub.prototype[Symbol.dispose].call(copy);
    }
    return { service, capability: await service.target() };
  }
}`;

export const lifetime = {
  async test(_ctrl, env, ctx) {
    const child = env.FACTORY.getPrivate('lifetime').load({
      compatibilityDate: '2026-09-08',
      compatibilityFlags: ['experimental'],
      allowExperimental: true,
      mainModule: 'main.js',
      modules: {
        'main.js': `import assert from 'node:assert/strict';
          import { WorkerEntrypoint } from 'cloudflare:workers';
          let saved;
          export default class extends WorkerEntrypoint {
            async fetch(request) {
              const service = this.env.SERVICE;
              if (new URL(request.url).pathname === '/store') {
                const target = await service.target();
                const pending = service.target();
                await pending;
                const callable = service.callback();
                const callback = await callable;
                const copy = target.dup();
                const held = target.echo;
                for (const invoke of [() => target.echo(41), () => copy.echo(41),
                    () => held(41), () => callback(41), () => pending.echo(41),
                    () => callable(41)]) assert.equal(await invoke(), 41);
                saved = {target, copy, held, callback, pending, callable,
                  root: service.echo};
                return new Response('stored');
              }
              const operations = [() => saved.target.echo(42), () => saved.copy.echo(42),
                () => saved.held(42), () => saved.callback(42), () => saved.target.dup(),
                () => saved.pending.echo(42), () => saved.callable(42),
                () => saved.held.then(() => 42), () => saved.held.finally(() => {})];
              for (const invoke of operations) await assert.rejects(async () => invoke(),
                /Cannot perform .* on behalf of a different request/);
              assert.equal(await saved.held.catch(error => {
                assert.match(error.message, /Cannot perform .* on behalf of a different request/);
                return 42;
              }), 42);
              for (const invoke of [() => saved.target.echo(42), () => saved.copy.echo(42),
                  () => saved.held(42), () => saved.callback(42),
                  () => saved.pending.echo(42), () => saved.callable(42)]) {
                const rejected = invoke();
                assert.equal(rejected.constructor.name, 'RpcPromise');
                assert.equal(await rejected.catch(error => {
                  assert.match(error.message, /Cannot perform .* on behalf of a different request/);
                  return 42;
                }), 42);
              }
              assert.equal(await saved.root(42), 42);
              assert.equal(await (await service.target()).echo(43), 43);
              return new Response('lifetime checked');
            }
          }`,
      },
      env: {},
      openComputeBindings: {
        SERVICE: {
          kind: 'service',
          fetcher: ctx.exports.Backend({ props: {} }),
        },
      },
      openComputeHostPolicy: true,
      globalOutbound: null,
    });
    const entrypoint = child.getEntrypoint();
    assert.equal(
      await (await entrypoint.fetch('https://test/store')).text(),
      'stored'
    );
    assert.equal(
      await (await entrypoint.fetch('https://test/use')).text(),
      'lifetime checked'
    );
  },
};

export const waitUntil = {
  async test(_ctrl, env) {
    const child = env.FACTORY.getPrivate('wait-until').load({
      compatibilityDate: '2026-09-08',
      compatibilityFlags: ['experimental'],
      allowExperimental: true,
      mainModule: 'main.js',
      modules: {
        'main.js': `import assert from 'node:assert/strict';
          import * as workers from 'cloudflare:workers';
          export default class extends workers.WorkerEntrypoint {
            async observe() {
              assert.equal(workers.withWaitUntilObserver, undefined);
              const original = Promise.resolve(41);
              workers.waitUntil(original);
              workers.waitUntil(Promise.resolve().then(() => {
                workers.waitUntil(Promise.resolve(42));
              }));
              await workers.withEnv({}, async () => {
                await Promise.resolve();
                workers.waitUntil(Promise.resolve(43));
              });
              this.ctx.waitUntil(Promise.resolve(44));
              return { answer: 17, promise: original };
            }
          }`,
      },
      env: { WAIT_UNTIL_OBSERVER: 'enabled' },
      openComputeHostPolicy: true,
      globalOutbound: null,
    });
    assert.deepEqual(await child.getEntrypoint().observe(), {
      value: 17,
      originalPromise: true,
      count: 5,
      statuses: [
        'fulfilled',
        'fulfilled',
        'fulfilled',
        'fulfilled',
        'fulfilled',
      ],
    });
    await assert.rejects(
      env.FACTORY.getPrivate('wait-until-import')
        .load({
          compatibilityDate: '2026-09-08',
          mainModule: 'main.js',
          modules: {
            'main.js': `import internal from 'cloudflare-internal:workers';
            export default {fetch() {return internal.withWaitUntilObserver;}};`,
          },
          env: {},
          globalOutbound: null,
        })
        .getEntrypoint()
        .fetch('https://test/'),
      /No such module|not found/i
    );
  },
};

export default {
  async test(_ctrl, env, ctx) {
    const code = {
      compatibilityDate: '2026-09-08',
      compatibilityFlags: ['experimental', 'service_binding_extra_handlers'],
      allowExperimental: true,
      mainModule: 'main.js',
      modules: { 'main.js': main },
      env: {},
      openComputeBindings: {
        SERVICE: {
          kind: 'service',
          fetcher: ctx.exports.Backend({ props: {} }),
        },
      },
      openComputeHostPolicy: true,
      globalOutbound: null,
    };
    assert.throws(() => env.FACTORY.get('public').load(code), /host-issued/);
    const parent = env.FACTORY.getPrivate('parent').load(code);
    const received = await parent.getEntrypoint().check();
    const transferred = received.service;
    const capability = received.capability;
    assert.equal(capability.constructor, RpcStub);
    assert.equal(await capability.echo(41), 41);
    const duplicate = RpcStub.prototype.dup.call(capability);
    RpcStub.prototype[Symbol.dispose].call(capability);
    assert.equal(await duplicate.echo(42), 42);
    RpcStub.prototype[Symbol.dispose].call(duplicate);
    assert.equal(transferred.constructor.name, 'Fetcher');
    assert.throws(() => transferred.rpc, /SERVICE_BINDING_DENIED/);
    assert.throws(
      () =>
        env.FACTORY.getPrivate('retype').load({
          ...code,
          openComputeBindings: {
            SERVICE: { kind: 'kvNamespace', fetcher: transferred },
          },
        }),
      /SERVICE_BINDING_DENIED/
    );
    assert.equal(
      await (await transferred.fetch('https://service/')).text(),
      'service'
    );
    const forwarded = env.FACTORY.getPrivate('child').load({
      ...code,
      env: { SERVICE: transferred },
      openComputeBindings: {},
    });
    const forwardedResult = await forwarded.getEntrypoint().check();
    const restored = forwardedResult.service;
    forwardedResult.capability[Symbol.dispose]();
    assert.deepEqual(await restored.echo({ roundTrip: true }), {
      roundTrip: true,
    });
    const shadow = env.FACTORY.getPrivate('shadow').load({
      ...code,
      modules: {
        ...code.modules,
        'cloudflare-internal:open-compute-host-policy': {
          js: 'throw Error("tenant takeover")',
        },
      },
    });
    const shadowResult = await shadow.getEntrypoint().check();
    shadowResult.capability[Symbol.dispose]();
    await assert.rejects(
      env.FACTORY.getPrivate('import')
        .load({
          ...code,
          modules: {
            'main.js':
              'import policy from "cloudflare-internal:open-compute-host-policy"; export default policy;',
          },
        })
        .getEntrypoint()
        .fetch('https://service/'),
      /No such module|not found/i
    );
  },
};
