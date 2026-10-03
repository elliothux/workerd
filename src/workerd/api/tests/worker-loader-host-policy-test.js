import assert from 'node:assert/strict';

const main = `import {env, WorkerEntrypoint} from 'cloudflare:workers';
if (!env.POLICY_READY || 'PRIVATE' in env) throw Error('invalid startup env');
// The trusted bootstrap captures its operations before tenant evaluation.
Reflect.apply = () => { throw Error('tenant Reflect.apply'); };
export default class extends WorkerEntrypoint {
  async fetch() {
    if (env.POLICY_READY !== this.env.POLICY_READY || 'PRIVATE' in this.env) throw Error('private handler env');
    if (Object.keys(this.env).some(key => key === 'PRIVATE')) throw Error('enumerable private env');
    return new Response('tenant');
  }
}
export const object = {fetch(_request, handlerEnv) {
  if ('PRIVATE' in handlerEnv) throw Error('private object handler env');
  return new Response('object');
}};`;

export default {
  async test(_ctrl, env) {
    const code = {
      compatibilityDate: '2026-09-08',
      mainModule: 'main.js',
      modules: { 'main.js': main },
      env: { MODE: 'valid' },
      openComputePrivateEnv: { PRIVATE: 'host-secret' },
      openComputeHostPolicy: true,
      globalOutbound: null,
    };
    const host = env.FACTORY.getPrivate('host');
    for (const value of [true, false]) {
      assert.throws(
        () =>
          env.FACTORY.get('public').load({
            ...code,
            openComputeHostPolicy: value,
          }),
        /host-issued/
      );
    }
    assert.throws(
      () => host.load({ ...code, openComputeHostPolicy: false }),
      /must be enabled/
    );
    for (let i = 0; i < 2; ++i) {
      const stub = host.load(code);
      assert.equal(
        await (
          await stub.getEntrypoint().fetch('https://tenant.example')
        ).text(),
        'tenant:policy'
      );
      assert.equal(
        await (
          await stub.getEntrypoint('object').fetch('https://tenant.example')
        ).text(),
        'object:policy'
      );
    }
    const shadow = host.load({
      ...code,
      modules: {
        ...code.modules,
        'cloudflare-internal:open-compute-host-policy': {
          js: 'export default () => {throw Error("tenant policy takeover");};',
        },
      },
    });
    assert.equal(
      await (
        await shadow.getEntrypoint().fetch('https://tenant.example')
      ).text(),
      'tenant:policy'
    );
    for (const [mode, expected] of [
      ['factory-value', /export policy/],
      ['exports-value', /synchronous exports/],
      ['exports-promise', /synchronous exports/],
      ['throw', /host policy failure/],
      ['bootstrap-loop', /CPU time limit/i],
      ['exports-loop', /CPU time limit/i],
    ]) {
      await assert.rejects(
        async () =>
          host
            .load({ ...code, env: { MODE: mode } })
            .getEntrypoint()
            .fetch('https://tenant.example'),
        expected
      );
    }
    await assert.rejects(
      async () =>
        host
          .load({
            ...code,
            modules: {
              'main.js':
                'import policy from "cloudflare-internal:open-compute-host-policy"; export default policy;',
            },
          })
          .getEntrypoint()
          .fetch('https://tenant.example'),
      /No such module|not found/i
    );
  },
};
