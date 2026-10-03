import assert from 'node:assert/strict';
import { WorkerEntrypoint } from 'cloudflare:workers';

export class PrivateQueueTransport extends WorkerEntrypoint {
  fetch() {
    throw Error('private Queue transport must not bypass publication policy');
  }
}

export const codec = {
  async test(_controller, env, ctx) {
    assert.equal(globalThis.QueueWireCodec, undefined);
    assert.throws(() => structuredClone(env.CODEC), { name: 'DataCloneError' });
    for (const input of [
      new Uint8Array(),
      new Uint8Array([255, 14, 0]),
      new Uint8Array([255, 15]),
    ]) {
      assert.throws(() => env.CODEC.decodeV8(input), /QUEUE_V8_MALFORMED/);
    }
    assert.throws(
      () => env.CODEC.decodeV8(new Uint8Array(128001)),
      /QUEUE_MESSAGE_TOO_LARGE/
    );
    assert.throws(
      () => env.CODEC.decodeV8(new ArrayBuffer(8, { maxByteLength: 16 })),
      /QUEUE_V8_UNSUPPORTED/
    );
    assert.throws(
      () => env.CODEC.decodeV8(new Uint8Array([255, 15, 255])),
      /deserialize|deserialization/i
    );
    const loader = env.FACTORY.get('public');
    assert.throws(
      () =>
        loader.load({
          compatibilityDate: '2026-09-08',
          mainModule: 'main.js',
          modules: {
            'main.js': 'export default {fetch(){return new Response();}};',
          },
          env: { CODEC: env.CODEC },
          globalOutbound: null,
        }),
      { name: 'DataCloneError' }
    );
    const value = {
      date: new Date(42),
      big: 12n,
      map: new Map([['a', 1]]),
      set: new Set([3]),
      bytes: new Uint8Array([4, 5]),
    };
    value.self = value;
    const result = await env.QUEUE.send(value, { contentType: 'v8' });
    assert.equal(result.metadata.metrics.backlogCount, 1);
    assert.equal(
      result.metadata.metrics.oldestMessageTimestamp.getTime(),
      1000
    );

    const child = env.FACTORY.getPrivate('native-policy').load({
      compatibilityDate: '2026-09-08',
      compatibilityFlags: ['experimental', 'nodejs_compat'],
      allowExperimental: true,
      mainModule: 'main.js',
      modules: {
        'main.js': `import assert from 'node:assert/strict';
        export default {async fetch(request, env) {
          const queue = env.QUEUE;
          const prototype = Object.getPrototypeOf(queue);
          assert.equal(queue.constructor.name, 'WorkerQueue');
          assert.throws(() => new queue.constructor(), /Illegal constructor/);
          assert.deepEqual(Reflect.ownKeys(queue), []);
          assert.equal(queue[Symbol.for('open-compute.flush-output')], undefined);
          assert.equal(queue[Symbol.for('open-compute.finalize-output')], undefined);
          assert.equal(queue.setPublicationPolicy, undefined);
          assert.throws(() => prototype.send.call({}, 'text'), /Illegal invocation/);
          await queue.send({ok: true}, {delaySeconds: 3});
          await prototype.send.call(queue, 'text', {contentType: 'text', delaySeconds: 3});
          const bytes = new Uint8Array([1, 2]);
          await queue.send(bytes, {contentType: 'bytes', delaySeconds: 3});
          assert.equal(bytes.byteLength, 0);
          const graph = {big: 12n}; graph.self = graph;
          await queue.send(graph, {contentType: 'v8', delaySeconds: 3});
          const batchBytes = new Uint8Array([3, 4]);
          const result = await queue.sendBatch([
            {body: batchBytes, contentType: 'bytes', delaySeconds: 0}, {body: 1},
          ], {delaySeconds: 4});
          assert.equal(batchBytes.byteLength, 2);
          assert.equal(result.metadata.metrics.backlogCount, 6);
          assert.equal(result.metadata.metrics.oldestMessageTimestamp.getTime(), 1000);
          assert.equal((await prototype.metrics.call(queue)).backlogCount, 6);
          await assert.rejects(async () => queue.send(undefined), /cannot be undefined/);
          await assert.rejects(async () => queue.send('x', {contentType: 'xml'}), /Unsupported/);
          await assert.rejects(async () => queue.sendBatch([]), /at least one/);
          assert.throws(() => structuredClone(queue), {name: 'DataCloneError'});
          return new Response('native Queue policy passed');
        }};`,
      },
      env: {},
      openComputeBindings: {
        QUEUE: {
          kind: 'queue',
          fetcher: ctx.exports.PrivateQueueTransport({ props: {} }),
        },
      },
      openComputeHostPolicy: true,
      globalOutbound: null,
    });
    assert.equal(
      await (await child.getEntrypoint().fetch('https://test/')).text(),
      'native Queue policy passed'
    );
  },
};

export default {
  async fetch(request, env) {
    assert.equal(new URL(request.url).pathname, '/message');
    assert.equal(request.headers.get('x-msg-fmt'), 'v8');
    const bytes = new Uint8Array(await request.arrayBuffer());
    const value = env.CODEC.decodeV8(bytes);
    assert.equal(value.date.getTime(), 42);
    assert.equal(value.big, 12n);
    assert.equal(value.map.get('a'), 1);
    assert.equal(value.set.has(3), true);
    assert.deepEqual([...value.bytes], [4, 5]);
    assert.equal(value.self, value);
    return Response.json({
      metadata: {
        metrics: {
          backlogCount: 1,
          backlogBytes: bytes.byteLength,
          oldestMessageTimestamp: 1000,
        },
      },
    });
  },
};
