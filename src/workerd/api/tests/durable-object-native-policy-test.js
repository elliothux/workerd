import assert from 'node:assert/strict';
import { WorkerEntrypoint } from 'cloudflare:workers';

export class Transport extends WorkerEntrypoint {
  fetch(request) {
    assert.equal(new URL(request.url).pathname, '/' + 'a'.repeat(64));
    return new Response('native fetch');
  }
  echo(id, value) {
    assert.equal(id, 'a'.repeat(64));
    return value;
  }
  identity(id) {
    return id;
  }
  name() {
    return 'remote name';
  }
}

export class Pipe extends WorkerEntrypoint {
  async relay(stub) {
    assert.equal(stub.constructor.name, 'ServiceStub');
    assert.equal(Reflect.ownKeys(stub).length, 0);
    assert.equal(typeof stub.id, 'function');
    assert.equal(await stub.id(), 'a'.repeat(64));
    assert.equal(await stub.name(), 'remote name');
    assert.equal(await stub.echo('transferred'), 'transferred');
    assert.equal(
      await (await stub.fetch('https://user/')).text(),
      'native fetch'
    );
    return stub;
  }
}

export default {
  async test(_controller, env, ctx) {
    const worker = env.FACTORY.getPrivate('native-do').load({
      compatibilityDate: '2026-09-08',
      compatibilityFlags: ['experimental', 'nodejs_compat'],
      allowExperimental: true,
      mainModule: 'main.js',
      modules: {
        'main.js': `import assert from 'node:assert/strict';
        export default {async fetch(_request, env) {
          env.CHECK();
          const ns = env.NS;
          const prototype = Object.getPrototypeOf(ns);
          assert.equal(ns.constructor.name, 'DurableObjectNamespace');
          assert.deepEqual(Reflect.ownKeys(ns), []);
          assert.throws(() => new ns.constructor(), /Illegal constructor/);
          assert.throws(() => prototype.idFromName.call({}, 'alpha'), /Illegal invocation/);
          const id = prototype.idFromName.call(ns, 'alpha');
          assert.equal(id.constructor.name, 'DurableObjectId');
          assert.equal(prototype.idFromString.call(ns, id.toString()).equals(id), true);
          assert.equal(prototype.newUniqueId.call(ns).constructor.name, 'DurableObjectId');
          assert.equal(prototype.jurisdiction.call(ns, 'eu'), ns);
          const stub = prototype.get.call(ns, id, {locationHint: 'enam'});
          assert.equal(stub.constructor.name, 'DurableObject');
          assert.equal(stub.id, id);
          assert.equal(stub.name, 'alpha');
          assert.deepEqual(Reflect.ownKeys(stub).sort(), ['id', 'name']);
          assert.throws(() => new stub.constructor(), /Illegal constructor/);
          assert.throws(() => prototype.get.call(ns, {}), TypeError);
          const stubPrototype = Object.getPrototypeOf(stub);
          assert.throws(() => stubPrototype.fetch.call({}, 'https://user/'), /Illegal invocation/);
          assert.equal(await (await stubPrototype.fetch.call(stub, 'https://user/')).text(), 'native fetch');
          assert.throws(() => stubPrototype.connect.call(stub, 'test:1'), /DO_RPC_UNSUPPORTED/);
          const method = stub.echo;
          assert.equal(method.constructor.name, 'RpcProperty');
          const memberPrototype = Object.getPrototypeOf(method);
          assert.throws(() => new method.constructor(), /Illegal constructor/);
          assert.throws(() => memberPrototype.then.call({}, () => {}), /Illegal invocation/);
          assert.equal(await stub.release, 'remote name');
          assert.equal(await stub.release.catch(() => 'failed'), 'remote name');
          let finished = 0;
          assert.equal(await stub.release.finally(() => finished++), 'remote name');
          assert.equal(finished, 1);
          const pending = method('same context');
          assert.equal(pending.constructor.name, 'RpcPromise');
          const pendingPrototype = Object.getPrototypeOf(pending);
          assert.throws(() => new pending.constructor(), /Illegal constructor/);
          assert.throws(() => pendingPrototype.then.call({}, () => {}), /Illegal invocation/);
          assert.equal(await pendingPrototype.then.call(pending, value => value), 'same context');
          assert.equal(await pending.catch(() => 'failed'), 'same context');
          assert.equal(await pending.finally(() => finished++), 'same context');
          assert.equal(finished, 2);
          pending[Symbol.dispose]();
          await assert.rejects(() => pending.child(), /disposed/);
          const failingDispose = stub.echo('dispose-failure');
          assert.throws(() => failingDispose[Symbol.dispose](), /fixture dispose failure/);
          await assert.rejects(() => failingDispose.child(), /disposed/);
          const graph = stub.echo({label: 'nested'});
          assert.equal(graph.label.constructor.name, 'RpcProperty');
          assert.equal(await graph.label, 'nested');
          let chain = graph;
          for (let depth = 0; depth < 65; depth++) chain = chain.child;
          assert.throws(() => chain.child, /property path is too deep/);
          const byName = prototype.getByName.call(ns, 'alpha');
          assert.equal(await byName.echo('named'), 'named');
          const transferred = await env.PIPE.relay(stub);
          assert.equal(transferred.constructor.name, 'ServiceStub');
          assert.equal(await transferred.echo('returned'), 'returned');
          assert.equal(await (await transferred.fetch('https://user/')).text(), 'native fetch');
          assert.equal(await (await env.PIPE.relay(transferred)).echo('twice'), 'twice');
          assert.throws(() => structuredClone(ns), {name: 'DataCloneError'});
          await assert.rejects(() => import('cloudflare-internal:wrapped-binding'));
          return new Response('native DO policy passed');
        }};`,
      },
      env: {},
      openComputePrivateEnv: {
        RAW: ctx.exports.Transport({ props: {} }),
        PIPE: ctx.exports.Pipe({ props: {} }),
      },
      openComputeHostPolicy: true,
      globalOutbound: null,
    });
    assert.equal(
      await (await worker.getEntrypoint().fetch('https://test/')).text(),
      'native DO policy passed'
    );
  },
};
