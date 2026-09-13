// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0
import assert from 'node:assert';

// These must match the limits hard-coded in worker-loader.c++.
const MAX_CODE_SIZE = 64 * 1024 * 1024; // 64 MiB
const MAX_ENV_SIZE = 1 * 1024 * 1024; // 1 MiB

const MAIN_MODULE = `
  import {WorkerEntrypoint} from "cloudflare:workers";
  export default class extends WorkerEntrypoint {
    ping() { return "pong"; }
    envBigLength() { return this.env.big ? this.env.big.length : 0; }
  }
`;

function makeCode(overrides) {
  return {
    compatibilityDate: '2025-01-01',
    mainModule: 'main.js',
    modules: { 'main.js': MAIN_MODULE },
    globalOutbound: null,
    ...overrides,
  };
}

// A worker whose total (uncompressed) size is comfortably under the limit loads and runs fine.
export let codeSizeWithinLimit = {
  async test(ctrl, env, ctx) {
    let worker = env.loader.get('codeSizeWithinLimit', () =>
      makeCode({
        modules: {
          'main.js': MAIN_MODULE,
          // ~1 MiB of additional (uncompiled) module content, well under the limit.
          'pad.js': '// ' + 'x'.repeat(1 * 1024 * 1024),
        },
      })
    );

    assert.strictEqual(await worker.getEntrypoint().ping(), 'pong');
  },
};

// A worker whose total module size exceeds the limit fails to load with a clear error.
export let codeSizeExceedsLimit = {
  async test(ctrl, env, ctx) {
    let worker = env.loader.get('codeSizeExceedsLimit', () =>
      makeCode({
        modules: {
          'main.js': MAIN_MODULE,
          // Push the total just over the limit. This module is never compiled because the size
          // check throws first.
          'big.js': '// ' + 'x'.repeat(MAX_CODE_SIZE),
        },
      })
    );

    await assert.rejects(worker.getEntrypoint().ping(), (e) => {
      assert.strictEqual(e.name, 'Error');
      assert.match(
        e.message,
        /^Dynamic Worker code size \(\d+ bytes\) exceeds the maximum allowed size of 67108864 bytes\.$/
      );
      return true;
    });
  },
};

// An env value under the limit is passed through to the dynamic worker.
export let envSizeWithinLimit = {
  async test(ctrl, env, ctx) {
    const big = 'x'.repeat(512 * 1024); // 512 KiB, under the 1 MiB limit.
    let worker = env.loader.get('envSizeWithinLimit', () =>
      makeCode({ env: { big } })
    );

    assert.strictEqual(await worker.getEntrypoint().envBigLength(), big.length);
  },
};

// An env value over the limit fails to load with a clear error.
export let envSizeExceedsLimit = {
  async test(ctrl, env, ctx) {
    let worker = env.loader.get('envSizeExceedsLimit', () =>
      makeCode({ env: { big: 'x'.repeat(2 * MAX_ENV_SIZE) } })
    );

    await assert.rejects(worker.getEntrypoint().ping(), (e) => {
      assert.strictEqual(e.name, 'Error');
      assert.match(
        e.message,
        /^Dynamic Worker env size \(\d+ bytes\) exceeds the maximum allowed size of 1048576 bytes\.$/
      );
      return true;
    });
  },
};

export let omittedLimitsAndUnknownFieldsAccepted = {
  async test(ctrl, env, ctx) {
    const worker = env.loader.load(
      makeCode({ limits: undefined, unknownOption: true })
    );
    assert.strictEqual(await worker.getEntrypoint().ping(), 'pong');
    assert.strictEqual(
      await worker.getEntrypoint(undefined, { unknownOption: true }).ping(),
      'pong'
    );
    assert.strictEqual(
      typeof worker.getDurableObjectClass(undefined, {}),
      'object'
    );
  },
};

export let aggregateCodeSizeBoundary = {
  async test(ctrl, env) {
    const prefixBytes = new TextEncoder().encode(MAIN_MODULE).byteLength;
    const data = new Uint8Array(17);
    const text =
      'x'.repeat(MAX_CODE_SIZE - prefixBytes - data.byteLength - 2) + 'π';
    const modules = { 'main.js': MAIN_MODULE, text: { text }, data: { data } };
    const worker = env.loader.load(makeCode({ modules }));
    // The structural 64 MiB code ceiling is independent from the 128 MiB isolate memory
    // ceiling. Materializing a source exactly at the structural boundary needs more than
    // 128 MiB of V8 heap and therefore fails with the narrower Standard memory limit.
    await assert.rejects(worker.getEntrypoint().ping(), /memory limit/i);
    const oversized = { ...modules, text: { text: text + 'x' } };
    assert.throws(
      () => env.loader.load(makeCode({ modules: oversized })),
      /Dynamic Worker code size \(67108865 bytes\) exceeds/
    );
    await assert.rejects(
      env.loader
        .get(null, () => makeCode({ modules: oversized }))
        .getEntrypoint()
        .ping(),
      /Dynamic Worker code size \(67108865 bytes\) exceeds/
    );
  },
};

// =====================================================================================
// Native enforcement of declared Standard resource limits. Every child here uses
// `globalOutbound: null`, so each fetch() attempt is counted as a subrequest and then fails
// with the null-outbound error -- letting the tests observe exactly where the budget ran out
// without any network access.

const PROBE_MODULE = `
  import {WorkerEntrypoint} from "cloudflare:workers";
  export default class extends WorkerEntrypoint {
    ping() { return "pong"; }
    // Attempt count fetches; return how many failed with the null-outbound error (counted)
    // versus the subrequest budget error (not counted, rejected before any side effect).
    async probe(count) {
      const outbound = [];
      const budget = [];
      for (let i = 0; i < count; i++) {
        try {
          await fetch("https://example.invalid/");
          outbound.push(i);
        } catch (e) {
          if (/not permitted/i.test(e.message)) outbound.push(i);
          else if (/Too many subrequests/i.test(e.message)) budget.push(i);
          else budget.push("unexpected: " + e.message);
        }
      }
      return { outbound: outbound.length, budget: budget.length, first: budget[0] };
    }
    async spin() {
      // Busy loop that can only end through CPU-limit termination.
      let x = 0;
      while (true) { x = (x + 1) | 0; }
      return x;
    }
  }
`;

function makeProbeCode(overrides) {
  return makeCode({
    mainModule: 'main.js',
    modules: { 'main.js': PROBE_MODULE },
    ...overrides,
  });
}

// A declared invocation CPU budget terminates a busy loop promptly. The terminated isolate is
// condemned: retained stubs keep failing, while a freshly loaded Worker (same immutable code)
// starts from a clean isolate.
export let cpuLimitTerminatesBusyLoop = {
  async test(ctrl, env, ctx) {
    const code = makeProbeCode({ limits: { cpuMs: 100 } });
    const entrypoint = env.loader.load(code).getEntrypoint();

    await assert.rejects(entrypoint.spin(), /cpu time limit/i);
    // The condemned isolate never admits another invocation through retained stubs.
    await assert.rejects(
      entrypoint.ping(),
      /cpu time limit|condemned|not permitted/i
    );

    // A fresh load rebuilds from the same code and runs normally.
    assert.strictEqual(
      await env.loader.load(code).getEntrypoint().ping(),
      'pong'
    );
  },
};

// Startup JS is subject to the fixed one-second CPU budget even when WorkerCode omits request
// limits. The failed isolate is evicted and does not wedge the loader host.
export let startupCpuLimitTerminatesModuleEvaluation = {
  async test(ctrl, env, ctx) {
    const code = makeCode({
      modules: {
        'main.js': 'while (true) {}\n' + MAIN_MODULE,
      },
    });
    await assert.rejects(
      env.loader.load(code).getEntrypoint().ping(),
      /cpu time limit/i
    );
    assert.strictEqual(
      await env.loader.load(makeCode()).getEntrypoint().ping(),
      'pong'
    );
  },
};

// ArrayBuffer backing stores contribute to the fixed 128 MiB isolate memory budget. The
// standalone profile terminates the crossing invocation, condemns and evicts its isolate,
// and leaves a neighboring child running.
export let memoryLimitCondemnsOnlyTheOffendingIsolate = {
  async test(ctrl, env, ctx) {
    const code = makeCode({
      modules: {
        'main.js': `
          export default {
            fetch() {
              const chunk = new Uint8Array(160 * 1024 * 1024);
              chunk[0] = 1;
              return new Response(String(chunk.byteLength));
            }
          };
        `,
      },
    });
    const limited = env.loader.load(code).getEntrypoint();
    const response = await limited.fetch('https://memory.invalid/');
    await assert.rejects(response.text(), /memory limit/i);
    await assert.rejects(
      limited.fetch('https://memory.invalid/'),
      /memory limit/i
    );
    assert.strictEqual(
      await env.loader.load(makeCode()).getEntrypoint().ping(),
      'pong'
    );
  },
};

// Omitted request dimensions stay unlimited even though fixed isolate and connection limits
// still apply to every Dynamic Worker.
export let unlimitedCpuWithoutDeclaredLimits = {
  async test(ctrl, env, ctx) {
    const worker = env.loader.load(makeProbeCode());
    const result = await worker.getEntrypoint().probe(2);
    assert.deepStrictEqual(result, {
      outbound: 2,
      budget: 0,
      first: undefined,
    });
  },
};

// The subrequest budget allows exactly N calls and rejects the (N+1)th before any side
// effect: attempts zero..N-1 produce the null-outbound error, the rest "Too many
// subrequests."
export let subrequestBudgetEnforced = {
  async test(ctrl, env, ctx) {
    const worker = env.loader.load(
      makeProbeCode({ limits: { subRequests: 3 } })
    );
    const result = await worker.getEntrypoint().probe(6);
    assert.deepStrictEqual(result, { outbound: 3, budget: 3, first: 3 });
  },
};

// Entrypoint-level limits combine per-dimension with the WorkerCode limits by minimum; an
// omitted entrypoint value inherits the WorkerCode budget, and a larger entrypoint value can
// never widen it.
export let entrypointLimitsMinWithWorkerCode = {
  async test(ctrl, env, ctx) {
    // Omitted entrypoint limits inherit the WorkerCode budget of 3.
    const inherit = env.loader.load(
      makeProbeCode({ limits: { subRequests: 3 } })
    );
    assert.deepStrictEqual(await inherit.getEntrypoint().probe(5), {
      outbound: 3,
      budget: 2,
      first: 3,
    });

    // A narrower entrypoint budget (1) wins over the WorkerCode budget (3).
    const narrower = env.loader.load(
      makeProbeCode({ limits: { subRequests: 3 } })
    );
    assert.deepStrictEqual(
      await narrower
        .getEntrypoint(undefined, { limits: { subRequests: 1 } })
        .probe(5),
      { outbound: 1, budget: 4, first: 1 }
    );

    // A wider entrypoint budget (10) cannot widen the WorkerCode budget of 3.
    const wider = env.loader.load(
      makeProbeCode({ limits: { subRequests: 3 } })
    );
    assert.deepStrictEqual(
      await wider
        .getEntrypoint(undefined, { limits: { subRequests: 10 } })
        .probe(5),
      { outbound: 3, budget: 2, first: 3 }
    );
  },
};

// Entrypoint-level limits work when WorkerCode omitted that dimension: the entrypoint declaration
// becomes the effective invocation limit instead of widening to unlimited.
export let entrypointLimitsApplyWithoutWorkerCodeLimits = {
  async test(ctrl, env, ctx) {
    const worker = env.loader.load(makeProbeCode());
    const limited = worker.getEntrypoint(undefined, {
      limits: { subRequests: 1 },
    });
    assert.deepStrictEqual(await limited.probe(3), {
      outbound: 1,
      budget: 2,
      first: 1,
    });
  },
};

// A dynamic Worker cannot use a delegated WorkerLoader to create a descendant whose limits
// exceed its own immutable WorkerCode ceiling. The ceiling follows the capability transfer and
// is applied per dimension to the descendant source.
export let delegatedLoaderCannotWidenContainingWorkerLimits = {
  async test(ctrl, env, ctx) {
    const parentModule = `
      import {WorkerEntrypoint} from "cloudflare:workers";
      export default class extends WorkerEntrypoint {
        async probeDescendant(count) {
          const child = this.env.loader.load({
            compatibilityDate: '2025-01-01',
            mainModule: 'main.js',
            modules: { 'main.js': ${JSON.stringify(PROBE_MODULE)} },
            limits: { subRequests: 10 },
            globalOutbound: null,
          });
          return child.getEntrypoint().probe(count);
        }
      }
    `;
    const parent = env.loader.load(
      makeCode({
        modules: { 'main.js': parentModule },
        limits: { subRequests: 3 },
        env: { loader: env.factory.get('nested-limit-ceiling') },
      })
    );
    assert.deepStrictEqual(await parent.getEntrypoint().probeDescendant(5), {
      outbound: 3,
      budget: 2,
      first: 3,
    });
  },
};

// Out-of-range limits values are rejected before the worker ever runs; the source is
// validated asynchronously, so the rejection surfaces on the first invocation.
export let invalidLimitValuesRejected = {
  async test(ctrl, env, ctx) {
    const cases = [
      [{ cpuMs: 0 }, /cpuMs must be a positive integer/],
      [{ cpuMs: 300001 }, /cpuMs must be at most 300000/],
      [{ subRequests: 0 }, /subRequests must be a positive integer/],
      [{ subRequests: 10000001 }, /subRequests must be at most 10000000/],
    ];
    for (const [limits, expected] of cases) {
      const entrypoint = env.loader
        .load(makeProbeCode({ limits }))
        .getEntrypoint();
      await assert.rejects(entrypoint.ping(), expected);
    }
  },
};

// Standard permits six outbound connections simultaneously waiting for response headers. A
// seventh fetch waits without reaching the outbound service, then starts as soon as one of the
// first six returns its headers.
export let seventhOutboundConnectionWaitsBeforeSideEffect = {
  async test(ctrl, env, ctx) {
    const code = makeCode({
      globalOutbound: env.outbound,
      modules: {
        'main.js': `
          import {WorkerEntrypoint} from "cloudflare:workers";
          let heldResponses = [];
          export default class extends WorkerEntrypoint {
            async holdSeven() {
              heldResponses = await Promise.all(Array.from({ length: 7 }, () =>
                fetch("https://outbound.invalid/hold")
              ));
              return heldResponses.map((response) => response.status);
            }
          }
        `,
      },
    });
    const result = await env.loader.load(code).getEntrypoint().holdSeven();
    assert.deepStrictEqual(result, Array(7).fill(200));
    assert.strictEqual(
      Number(
        await (
          await env.outbound.fetch('https://outbound.invalid/maximum')
        ).text()
      ),
      6
    );
  },
};
