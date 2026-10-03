import assert from 'node:assert/strict';
import { DurableObject } from 'cloudflare:workers';

export class Native extends DurableObject {}

export default {
  async test(_controller, env, ctx) {
    const id = env.IDS.NAMED;
    const prototype = Object.getPrototypeOf(id);
    assert.equal(id.constructor.name, 'DurableObjectId');
    assert.equal(id.name, 'alpha');
    assert.equal(id.jurisdiction, 'eu');
    assert.equal(env.IDS.PARSED.name, undefined);
    assert.equal(env.IDS.OTHER.jurisdiction, undefined);
    assert.equal(prototype.toString.call(id), 'a'.repeat(64));
    assert.equal(prototype.equals.call(id, env.IDS.PARSED), true);
    assert.equal(env.IDS.PARSED.equals(id), true);
    assert.equal(id.equals(env.IDS.OTHER), false);
    assert.throws(() => prototype.toString.call({}), /Illegal invocation/);
    assert.throws(() => prototype.equals.call({}, id), /Illegal invocation/);
    assert.throws(
      () => id.equals({ toString: () => id.toString() }),
      TypeError
    );
    assert.throws(() => new id.constructor(), /Illegal constructor/);
    assert.throws(() => {
      id.name = 'forged';
    }, TypeError);
    assert.throws(() => {
      id.jurisdiction = 'us';
    }, TypeError);
    assert.throws(() => structuredClone(id), { name: 'DataCloneError' });
    assert.equal(id.createDurableObjectId, undefined);
    id.toString = () => 'forged';
    assert.equal(prototype.toString.call(id), 'a'.repeat(64));
    assert.equal(prototype.equals.call(id, env.IDS.PARSED), true);
    delete id.toString;
    const native = ctx.exports.Native.idFromName('alpha');
    assert.equal(native.equals(id), false);
    assert.equal(id.equals(native), false);
    assert.throws(() => ctx.exports.Native.get(id), {
      name: 'TypeError',
      message: 'DO_ID_INVALID',
    });
    assert.doesNotThrow(() => ctx.exports.Native.get(native));
    assert.equal(Object.getPrototypeOf(native), prototype);
    await assert.rejects(() => import('cloudflare-internal:wrapped-binding'));
  },
};
