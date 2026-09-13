// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Standalone runtime isolate-level resource limits for dynamically-loaded Workers: the 128 MiB
// isolate memory budget and the 1 s startup CPU budget of the Standard profile. Request-level
// limits (invocation CPU, subrequests, simultaneous connections) live in
// standalone-resource-limits.*; this module only covers limits whose lifetime is the isolate.
//
// The enforcer delegates every unspecified behavior to the same defaults the unlimited
// standalone enforcer uses, so the only observable differences are the enforced budgets.

#pragma once

#include <workerd/io/limit-enforcer.h>
#include <workerd/server/standalone-resource-limits.h>

#include <v8-isolate.h>

#include <kj/time.h>

namespace workerd {

// Standard profile constants. Fixed implementation values, not operator-tunable; they mirror
// Cloudflare's documented Workers Standard limits.
constexpr size_t STANDARD_ISOLATE_MEMORY_LIMIT = 128u * 1024 * 1024;  // 128 MiB
constexpr kj::Duration STANDARD_STARTUP_CPU_LIMIT = 1 * kj::SECONDS;

// IsolateLimitEnforcer for one dynamically-loaded Worker isolate.
//
// Memory: the V8 old-generation heap is capped at the Standard limit via isolate creation
// parameters; exhausting it produces the usual V8 RangeError while this enforcer condemns the
// isolate so its loader entry is evicted and in-flight invocations settle. ArrayBuffer/Wasm
// backing stores are additionally checked against the same budget via V8's external-memory
// accounting at lock exit. Memory that V8 cannot attribute (native caches, isolate-external
// allocations) is out of scope for this budget and remains the deployment environment's
// responsibility.
//
// Startup CPU: module compilation is bounded by the thread CPU clock through the same
// watchdog as request turns; an overrunning startup is terminated and reported as a permanent
// startup failure with the stable CPU-limit exception.
class StandaloneIsolateLimitEnforcer final: public IsolateLimitEnforcer {
 public:
  StandaloneIsolateLimitEnforcer(
      kj::Rc<StandaloneIsolateLimitState> isolateState, StandaloneLimitWatchdog& watchdog);

  // ---------------------------------------------------------------------------
  // IsolateLimitEnforcer

  v8::Isolate::CreateParams getCreateParams() override;
  void customizeIsolate(v8::Isolate* isolate) override;
  ActorCacheSharedLruOptions getActorCacheLruOptions() override;
  kj::Own<void> enterStartupJs(
      jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const override;
  kj::Own<void> enterStartupPython(
      jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const override;
  kj::Own<void> enterDynamicImportJs(
      jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const override;
  kj::Own<void> enterLoggingJs(
      jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const override;
  kj::Own<void> enterInspectorJs(
      jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const override;
  void completedRequest(kj::StringPtr id) const override;
  bool exitJs(jsg::Lock& lock) const override;
  void reportMetrics(IsolateObserver& isolateMetrics) const override;
  bool hasExcessivelyExceededHeapLimit() const override;
  const TrackedWasmInstanceList& getTrackedWasmInstances() const override;

  // Common body of the bounded startup scopes. Public only so deterministic tests can drive
  // the startup budget without a live isolate lock.
  kj::Own<void> enterBoundedStartup(kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const;

 private:
  struct StartupScope;

  // Near-heap-limit callback: refuses heap growth and condemns the isolate. V8 raises its
  // usual RangeError for the crossing allocation; the isolate is never reused.
  static size_t onNearHeapLimit(void* data, size_t currentHeapLimit, size_t initialHeapLimit);

  mutable kj::Rc<StandaloneIsolateLimitState> isolateState;
  StandaloneLimitWatchdog& watchdog;

  // Workerd-thread-only. Set when the heap limit was hit, so memory-limit failures can be
  // reported accurately even when V8 surfaces them as ordinary RangeErrors.
  mutable bool heapLimitHit = false;

  TrackedWasmInstanceList trackedWasmInstances;
};

}  // namespace workerd
