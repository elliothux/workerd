// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0
import assert from 'node:assert/strict';
import { DurableObject } from 'cloudflare:workers';

const childSource = `
  import { DurableObject } from 'cloudflare:workers';
  export class Child extends DurableObject {
    async increment() {
      const count = (await this.ctx.storage.get('count') ?? 0) + 1;
      await this.ctx.storage.put('count', count);
      return { count, id: String(this.ctx.id), marker: this.ctx.props.marker };
    }
    descend() {
      return this.ctx.facets.get('child', () => ({ class: this.ctx.exports.Child })).increment();
    }
  }
`;
const wrapperSource = `
  import { WorkerEntrypoint } from 'cloudflare:workers';
  export default class extends WorkerEntrypoint {
    create(name, depth, marker) {
      const child = this.env.LOADER.load({
        compatibilityDate: '2026-09-05',
        compatibilityFlags: ['enable_ctx_exports'],
        mainModule: 'child.js', modules: { 'child.js': ${JSON.stringify(childSource)} },
      });
      const actorClass = child.getDurableObjectClass('Child', { props: { marker } });
      this.env.FACETS.create(name, depth, 'custom-id', actorClass);
    }
    redelegate() {
      return this.env.LOADER.load({
        compatibilityDate: '2026-09-05', mainModule: 'main.js',
        modules: { 'main.js': 'export default {}' }, env: { stolen: this.env.FACETS },
      }).getEntrypoint().fetch('https://example.com');
    }
    revoke() { this.env.FACETS.revoke(); }
    transfer() { return this.env.FACETS; }
  }
`;

export class StaticChild extends DurableObject {
  async increment() {
    const count = ((await this.ctx.storage.get('count')) ?? 0) + 1;
    await this.ctx.storage.put('count', count);
    return count;
  }
}

export class Host extends DurableObject {
  wrapper(grant) {
    return this.env.loader
      .load({
        compatibilityDate: '2026-09-05',
        mainModule: 'wrapper.js',
        modules: { 'wrapper.js': wrapperSource },
        env: { FACETS: grant, LOADER: this.env.factory.get('host-facets') },
      })
      .getEntrypoint();
  }
  facet(name) {
    return this.ctx.facets.get(name, () => {
      throw new Error('creation missing');
    });
  }
  async installRetained() {
    const grant = this.env.factory.getFacets(this.ctx.facets);
    const wrapper = this.env.loader
      .get('retained-grant', () => ({
        compatibilityDate: '2026-09-05',
        mainModule: 'wrapper.js',
        modules: { 'wrapper.js': wrapperSource },
        env: { FACETS: grant, LOADER: this.env.factory.get('host-facets') },
      }))
      .getEntrypoint();
    await wrapper.create('alive', 1, 'alive');
    assert.equal((await this.facet('alive').increment()).count, 1);
  }
  end() {
    this.ctx.abort('host-origin-ended');
  }
  async checkStorage() {
    const grant = this.env.factory.getFacets(this.ctx.facets);
    const wrapper = this.wrapper(grant);
    await wrapper.create('one', 1, 'first');
    assert.deepEqual(await this.facet('one').increment(), {
      count: 1,
      id: 'custom-id',
      marker: 'first',
    });
    this.ctx.facets.clone('one', 'copy');
    await wrapper.create('copy', 1, 'copy');
    assert.equal((await this.facet('copy').increment()).count, 2);
    this.ctx.facets.abort('one', 'replace');
    await wrapper.create('one', 1, 'second');
    assert.deepEqual(await this.facet('one').increment(), {
      count: 2,
      id: 'custom-id',
      marker: 'second',
    });
    this.ctx.facets.abort('one', 'switch-to-static');
    const staticFacet = this.ctx.facets.get('one', () => ({
      class: this.ctx.exports.StaticChild,
    }));
    assert.equal(await staticFacet.increment(), 3);
    this.ctx.facets.abort('one', 'switch-to-dynamic');
    await wrapper.create('one', 1, 'dynamic-again');
    assert.equal((await this.facet('one').increment()).count, 4);
    this.ctx.facets.delete('one');
    await wrapper.create('one', 1, 'fresh');
    assert.equal((await this.facet('one').increment()).count, 1);
    grant.revoke();
    await assert.rejects(
      wrapper.create('late', 1, 'late'),
      /ended or been revoked/
    );
    // Existing facets are owned by the manager, independently of the creation grant.
    assert.equal((await this.facet('one').increment()).count, 2);
  }
  async checkBoundary() {
    const grant = this.env.factory.getFacets(this.ctx.facets);
    const wrapper = this.wrapper(grant);
    await assert.rejects(
      async () => this.ctx.storage.put('grant', grant),
      /DataCloneError/
    );
    await assert.rejects(wrapper.redelegate(), /DataCloneError/);
    await assert.rejects(wrapper.transfer(), /DataCloneError/);
    await assert.rejects(wrapper.revoke(), /originating host/);
    await assert.rejects(
      wrapper.create('invalid', 4, 'bad'),
      /Invalid host facet depth/
    );
    await wrapper.create('deep', 3, 'deep');
    await assert.rejects(this.facet('deep').descend(), /depth limit/);
    grant.revoke();
  }
}
export const storage = {
  async test(ctrl, env, ctx) {
    await ctx.exports.Host.getByName('storage').checkStorage();
  },
};
export const boundary = {
  async test(ctrl, env, ctx) {
    await ctx.exports.Host.getByName('boundary').checkBoundary();
  },
};

export const lifetime = {
  async test(ctrl, env, ctx) {
    const host = ctx.exports.Host.getByName('lifetime');
    await host.installRetained();
    await assert.rejects(host.end(), /host-origin-ended/);
    const wrapper = env.loader
      .get('retained-grant', () => {
        throw new Error('unexpected wrapper eviction');
      })
      .getEntrypoint();
    await assert.rejects(
      wrapper.create('late', 1, 'late'),
      /ended or been revoked/
    );
  },
};
