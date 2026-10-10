import assert from 'node:assert/strict';
import { DurableObject } from 'cloudflare:workers';

const source = `
  import { DurableObject } from 'cloudflare:workers';
  export class App extends DurableObject {
    state() { return {brand: this.ctx.container.constructor.name, running: this.ctx.container.running}; }
    async snapshots() {
      await this.ctx.container.snapshotContainer();
    }
  }
  export class Plain extends DurableObject {
    state() { return this.ctx.container === undefined; }
  }
`;

export class Host extends DurableObject {
  async check() {
    assert.equal(this.ctx.container, undefined);
    const calls = [];
    const grant = this.env.factory.getContainer(this.ctx.facets, 'App', async (start, stop, completed, startup) => {
      calls.push({start, stop, completed, startup});
      return undefined;
    });
    const loader = this.env.loader;
    const loaded = loader.load({
      compatibilityDate: '2026-09-08',
      compatibilityFlags: ['enable_ctx_exports'],
      mainModule: 'tenant.js', modules: {'tenant.js': source},
      env: {}, openComputePrivateEnv: {}, openComputeHostPolicy: true,
      openComputeContainer: grant, globalOutbound: this.env.outbound,
    });
    const child = this.ctx.facets.get('tenant', () => ({class: loaded.getDurableObjectClass('App'), id:'app'}));
    assert.deepEqual(await child.state(), {brand:'Container', running:false});
    const beforeSnapshot = calls.length;
    await assert.rejects(child.snapshots(), /snapshotContainer\(\) cannot be called on a container that is not running/);
    assert.equal(calls.length, beforeSnapshot);
    await assert.rejects(grant.start(), /Container operation failed/);
    assert.ok(calls.some(call => call.start && call.startup === ''));
    const plain = this.ctx.facets.get('plain', () => ({class:loaded.getDurableObjectClass('Plain'), id:'plain'}));
    assert.equal(await plain.state(), true);
    grant.revoke();
    await assert.rejects(grant.start(), /Container operation failed/);
  }
}

export const attachment = {
  async test(_controller, _env, ctx) {
    await ctx.exports.Host.getByName('attachment').check();
  },
};
