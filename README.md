<p align="center">
  <a href="https://open-compute.dev">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/elliothux/open-compute/main/share/brand/logo-text-white.svg">
      <img src="https://raw.githubusercontent.com/elliothux/open-compute/main/share/brand/logo-text-black.svg" alt="open-compute" width="420">
    </picture>
  </a>
</p>

# workerd for open-compute

This repository is the [open-compute](https://github.com/elliothux/open-compute) fork of
[Cloudflare workerd](https://github.com/cloudflare/workerd). It is maintained specifically as the
native runtime used by open-compute, a self-hosted Cloudflare Workers compatible platform.

The fork keeps Cloudflare's workerd as its foundation and adds the native host interfaces and
standalone runtime controls that open-compute needs. It is not a general replacement for upstream
workerd, and its open-compute interfaces are not Cloudflare Workers APIs.

## What this fork adds

| Area | Upstream workerd | open-compute extension |
| --- | --- | --- |
| Dynamic Workers | Experimental Worker Loader primitives | Host-owned loader namespaces, delegated capabilities, bounded caches, revocation, invocation accounting, tail propagation, and dynamic Durable Object facets |
| Resource limits | Core limit interfaces and Cloudflare production integration points | Standalone enforcement for invocation CPU, subrequests, isolate memory, startup CPU, and simultaneous outbound connections |
| Native bindings | Static capabilities configured inside workerd | Session-scoped native providers opened by a trusted host and exposed through private `HostExtensionFactory` / `HostExtensionPort` capabilities |
| Private dynamic bindings | Public, RPC-serializable dynamic Worker environment | A handler-only private environment for one-time host capability grants, with prefix revocation and no tenant minting or onward transfer |
| Binaries | Official workerd release artifacts | Manually triggered, source-identified binaries used by open-compute's coordinated runtime pin process |

### Delegated Worker Loader namespaces

open-compute can grant a trusted system Worker an isolated `WorkerLoaderFactory`. The factory creates
named loaders whose capabilities, cache entries, active invocations, tails, streams, and Durable
Object facets stay inside one host-owned namespace.

The fork enforces bounded namespace and cache sizes, distinguishes live references from idle cache
entries, drains accepted work during revocation, and rejects new work after a namespace is revoked.
These rules let open-compute load tenant code without exposing loader keys, internal routing tokens,
or a process-wide loader authority to tenants.

### Workers Standard resource limits

Dynamic Workers run with a native standalone limits implementation:

- per-invocation CPU accounting across JavaScript turns;
- subrequest budgets checked before the external side effect;
- a 128 MiB isolate heap limit and a 1 second startup CPU limit;
- six simultaneous outbound connection slots per invocation;
- isolate condemnation, loader-cache eviction, and clean reconstruction after a fatal limit.

Worker code, entrypoint, and delegated child limits compose by taking the strictest value for each
dimension, so descendants cannot widen their parent's ceiling. These limits implement the
open-compute Standard profile; Cloudflare's hosted scheduling, billing, analytics, and fleet
placement remain outside this repository.

### User-extensible native host bindings

The fork provides a narrow data plane between workerd and an operator-owned native process. A
trusted host opens a session through an inherited Unix broker FD, then workerd communicates directly
with that provider through a session-scoped Cap'n Proto capability.

`HostExtensionPort` deliberately exposes only two operations:

```ts
interface HostExtensionPort {
  call(method: number, payload: Uint8Array): Promise<Uint8Array>;
  stream(method: number, payload: Uint8Array): ReadableStream<Uint8Array>;
}
```

The capability is private, cannot be minted by tenant code, cannot be persisted, and can be
delegated only once into a dynamic Worker's handler environment. Provider discovery, executable
verification, process supervision, authorization, deadlines, and protocol bounds are owned by
open-compute's `ocd` host.

### Private dynamic Worker grants

Trusted loaders may add `openComputePrivateEnv` bindings that are visible to the target Worker's
handler but absent from the importable `cloudflare:workers` environment. This is the path used for
native host ports and other non-public facets. Only loaders created through the host-only private
factory can set it; ordinary Worker Loader users are rejected.

Private grants use native capability tables rather than serialized credentials. They cannot cross a
second RPC boundary, and the host can revoke either one namespace or a complete namespace prefix
when an open-compute generation, deployment, or extension session ends.

## Compatibility and release policy

- Existing workerd behavior remains governed by the upstream compatibility date and flag model
  unless a fork extension is explicitly involved.
- The additions above are private integration seams for open-compute. Applications should use the
  public open-compute and Cloudflare-compatible surfaces instead of depending on them directly.
- open-compute pins an exact fork revision and verifies source identity, archives, binaries, version
  output, compatibility settings, and process flags. A newer commit in this repository does not
  automatically become the production runtime.
- Upstream changes are incorporated deliberately and retested against the fork-specific Worker
  Loader, limits, native-provider, and revocation coverage.
- General workerd bugs belong in the
  [Cloudflare repository](https://github.com/cloudflare/workerd/issues). Bugs in the extensions
  described here belong in [open-compute](https://github.com/elliothux/open-compute/issues).

The current source and binary identity is recorded in
[`packages/runtime/workerd.lock.json`](https://github.com/elliothux/open-compute/blob/main/packages/runtime/workerd.lock.json).
The design and qualification evidence lives in the
[`docs/workerd`](https://github.com/elliothux/open-compute/tree/main/docs/workerd) and
[`docs/implemented`](https://github.com/elliothux/open-compute/tree/main/docs/implemented)
directories.

## Building

The build remains the upstream Bazel build:

```sh
bazel build --config=release //src/workerd/server:workerd
```

The binary is written to `bazel-bin/src/workerd/server/workerd`. See the
[upstream README](https://github.com/cloudflare/workerd/blob/main/README.md) for supported platforms,
toolchain setup, configuration, local development, and general workerd usage. Contributors to this
fork should also follow [`AGENTS.md`](AGENTS.md) and the component-specific instructions in the
source tree.

## Security

Like upstream workerd, this runtime is not by itself a complete defense-in-depth sandbox for
untrusted code. open-compute runs it as one component of a wider boundary that includes capability
scoping, verified immutable inputs, loopback-only internal listeners, supervised process lifecycle,
and host-level isolation. See [SECURITY.md](SECURITY.md) for reporting instructions.

## License

This fork retains workerd's upstream license and notices. See [LICENSE](LICENSE).
