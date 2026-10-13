import assert from 'node:assert/strict';
import { DurableObject } from 'cloudflare:workers';

const source = `
  import { DurableObject } from 'cloudflare:workers';
  export class App extends DurableObject {
    constructor(ctx, env) {
      super(ctx, env);
      this.configured = this.ctx.container.setInactivityTimeout(21600000);
    }
    async inactivity() {
      await this.configured;
      const errors = [];
      for (const duration of [-1, 0, 21600001]) {
        try { await this.ctx.container.setInactivityTimeout(duration); errors.push('accepted'); }
        catch (error) { errors.push(error.name); }
      }
      return errors;
    }
    state() { return {brand: this.ctx.container.constructor.name, running: this.ctx.container.running, snapshotDirectory: typeof this.ctx.container.snapshotDirectory}; }
    async inspect() { return await this.ctx.container.inspect(); }
    async registerChannels() {
      for (const binding of [this.env.OBJECT, this.env.SERVICE]) {
        await this.ctx.container.interceptOutboundHttp('public.invalid:80', binding);
        await this.ctx.container.interceptOutboundHttps('public.invalid:443', binding);
      }
    }
    async snapshots() {
      await this.ctx.container.snapshotContainer();
    }
  }
  export class TcpOnly extends DurableObject {
    async registerTcpChannels() {
      for (const binding of [this.env.OBJECT, this.env.SERVICE]) {
        await this.ctx.container.interceptOutboundTcp('public.invalid:1234', binding);
      }
    }
  }
  export class InspectOnly extends DurableObject {
    async inspect() { return await this.ctx.container.inspect(); }
  }
  export class Plain extends DurableObject {
    state() { return this.ctx.container === undefined; }
  }
`;

export class Host extends DurableObject {
  async check() {
    assert.equal(this.ctx.container, undefined);
    assert.throws(
      () =>
        this.env.factory.getContainer(
          this.ctx.facets,
          'App',
          async () => undefined
        ),
      TypeError
    );
    assert.throws(
      () =>
        this.env.factory.getContainer(
          this.ctx.facets,
          'App',
          async () => undefined,
          async () => {
            throw new Error('Unexpected capture');
          }
        ),
      TypeError
    );
    const calls = [];
    const resolve = async (start, stop, completed, startup, expectedLeaseKey, monitor) => {
      assert.equal(typeof expectedLeaseKey, 'string');
      assert.equal(typeof monitor, 'boolean');
      calls.push({ start, stop, completed, startup, monitor });
      return undefined;
    };
    const grant = this.env.factory.getContainer(
      this.ctx.facets,
      'App',
      resolve,
      async () => {
        throw new Error('Unexpected capture');
      },
      {}
    );
    const loader = this.env.loader;
    const loaded = loader.load({
      compatibilityDate: '2026-09-08',
      compatibilityFlags: ['enable_ctx_exports'],
      mainModule: 'tenant.js',
      modules: { 'tenant.js': source },
      env: { DECLARED: 'registration event', SERVICE: this.env.outbound },
      openComputePrivateEnv: { raw: this.env.outbound },
      openComputeHostPolicy: true,
      openComputeContainer: grant,
      globalOutbound: this.env.outbound,
    });
    const child = this.ctx.facets.get('tenant', () => ({
      class: loaded.getDurableObjectClass('App'),
      id: 'app',
    }));
    assert.deepEqual(await child.state(), {
      brand: 'Container',
      running: false,
      snapshotDirectory: 'undefined',
    });
    assert.equal(await child.inspect(), null);
    await child.registerChannels();
    assert.deepEqual(await child.inactivity(), [
      'TypeError',
      'TypeError',
      'Error',
    ]);
    assert.ok(calls.every((call) => !call.start && !call.stop));
    const beforeSnapshot = calls.length;
    await assert.rejects(
      child.snapshots(),
      /snapshotContainer\(\) cannot be called on a container that is not running/
    );
    assert.equal(calls.length, beforeSnapshot);
    await assert.rejects(grant.start(), /Container operation failed/);
    assert.ok(calls.some((call) => call.start && call.startup === ''));
    const plain = this.ctx.facets.get('plain', () => ({
      class: loaded.getDurableObjectClass('Plain'),
      id: 'plain',
    }));
    assert.equal(await plain.state(), true);
    grant.revoke();
    await assert.rejects(grant.start(), /Container operation failed/);
    this.ctx.facets.abort('tenant', new Error('Stable attachment case completed'));
    const tcpGrant = this.env.factory.getContainer(
      this.ctx.facets,
      'TcpOnly',
      resolve,
      async () => { throw new Error('Unexpected capture'); },
      {}
    );
    try {
      const tcpLoaded = loader.load({
        compatibilityDate: '2026-09-08',
        compatibilityFlags: ['enable_ctx_exports', 'experimental'],
        allowExperimental: true,
        mainModule: 'tenant.js',
        modules: { 'tenant.js': source },
        env: { DECLARED: 'registration event', SERVICE: this.env.outbound },
        openComputePrivateEnv: { raw: this.env.outbound },
        openComputeHostPolicy: true,
        openComputeContainer: tcpGrant,
        globalOutbound: this.env.outbound,
      });
      const tcpChild = this.ctx.facets.get('tenant', () => ({
        class: tcpLoaded.getDurableObjectClass('TcpOnly'),
        id: 'app',
      }));
      const beforeTcp = calls.length;
      await tcpChild.registerTcpChannels();
      assert.ok(calls.slice(beforeTcp).every((call) => !call.start && !call.stop));
    } finally {
      tcpGrant.revoke();
      this.ctx.facets.abort('tenant', new Error('TCP owning case completed'));
    }
  }
  async checkInspectionAuthority() {
    const malformed = [
      undefined,
      { image: 'public/image', labels: undefined },
      { image: 'public/image\t', labels: [] },
      { image: 'x'.repeat(585), labels: [] },
      { image: 'public/image', labels: [{ name: '', value: 'value' }] },
      {
        image: 'public/image',
        labels: [
          { name: 'purpose', value: 'a' },
          { name: 'purpose', value: 'b' },
        ],
      },
      {
        image: 'public/image',
        labels: [{ name: 'purpose', value: 'line\nvalue' }],
      },
      {
        image: 'public/image',
        labels: Array.from({ length: 11 }, (_, i) => ({
          name: `n${i}`,
          value: 'value',
        })),
      },
    ];
    for (const [index, inspection] of malformed.entries()) {
      const grant = this.env.factory.getContainer(
        this.ctx.facets,
        'InspectOnly',
        async () => ({
          leaseKey: 'a'.repeat(64),
          imageRef: 'sha256:' + 'b'.repeat(64),
          providerContractSha256: 'c'.repeat(64),
          controlToken: 'd'.repeat(64),
          callbackToken: 'e'.repeat(64),
          nativeStartupJson: '{}',
          inspection,
        }),
        async () => {
          throw new Error('Unexpected capture');
        },
        {}
      );
      const loaded = this.env.loader.load({
        compatibilityDate: '2026-09-08',
        compatibilityFlags: ['enable_ctx_exports'],
        mainModule: 'tenant.js',
        modules: { 'tenant.js': source },
        env: {},
        openComputePrivateEnv: {},
        openComputeHostPolicy: true,
        openComputeContainer: grant,
        globalOutbound: this.env.outbound,
      });
      const child = this.ctx.facets.get('tenant', () => ({
        class: loaded.getDurableObjectClass('InspectOnly'),
        id: `inspection-${index}`,
      }));
      await assert.rejects(child.inspect(), /Container operation failed/);
      grant.revoke();
      this.ctx.facets.abort('tenant', new Error('Inspection case completed'));
    }
  }
}

export const attachment = {
  async test(_controller, _env, ctx) {
    await ctx.exports.Host.getByName('attachment').check();
    await ctx.exports.Host.getByName(
      'inspection-authority'
    ).checkInspectionAuthority();
  },
};
