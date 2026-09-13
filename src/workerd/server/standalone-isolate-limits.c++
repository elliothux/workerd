// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "standalone-isolate-limits.h"

#include <workerd/io/actor-cache.h>
#include <workerd/io/tracked-wasm-instance.h>
#include <workerd/jsg/exception.h>
#include <workerd/jsg/setup.h>
#include <workerd/util/exception.h>

#include <kj/debug.h>

namespace workerd {

namespace {

constexpr size_t STANDARD_ACTOR_CACHE_SOFT_LIMIT = 16 * (1ull << 20);   // 16 MiB
constexpr size_t STANDARD_ACTOR_CACHE_HARD_LIMIT = 128 * (1ull << 20);  // 128 MiB
constexpr size_t MEMORY_TERMINATION_HEADROOM = 16 * (1ull << 20);       // 16 MiB

}  // namespace

// ========================================================================================
// Startup scopes

struct StandaloneIsolateLimitEnforcer::StartupScope final {
  StartupScope(const StandaloneIsolateLimitEnforcer& enforcer,
      kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime)
      : enforcer(enforcer),
        limitErrorOrTime(limitErrorOrTime),
        turnStart(enforcer.watchdog.clock().currentThreadCpu()),
        token(enforcer.watchdog.registerTurn(
            *enforcer.isolateState, &enforcer, STANDARD_STARTUP_CPU_LIMIT)) {}

  ~StartupScope() noexcept(false) {
    auto turnEnd = enforcer.watchdog.clock().currentThreadCpu();
    auto delta = turnEnd - turnStart;
    enforcer.watchdog.finishTurn(token, delta);
    if (delta > STANDARD_STARTUP_CPU_LIMIT) {
      // Terminated by the watchdog (or a boundary-exact overrun): the startup fails
      // permanently with the stable CPU-limit exception.
      limitErrorOrTime =
          enforcer.isolateState->takeCondemnedException(StandaloneLimitViolation::CPU);
    }
  }

  const StandaloneIsolateLimitEnforcer& enforcer;
  kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime;
  kj::Duration turnStart;
  StandaloneLimitWatchdog::TurnToken token;
};

kj::Own<void> StandaloneIsolateLimitEnforcer::enterBoundedStartup(
    kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const {
  return kj::heap<StartupScope>(*this, limitErrorOrTime);
}

// ========================================================================================
// IsolateLimitEnforcer

StandaloneIsolateLimitEnforcer::StandaloneIsolateLimitEnforcer(
    kj::Rc<StandaloneIsolateLimitState> isolateStateParam, StandaloneLimitWatchdog& watchdogParam)
    : isolateState(kj::mv(isolateStateParam)),
      watchdog(watchdogParam) {}

v8::Isolate::CreateParams StandaloneIsolateLimitEnforcer::getCreateParams() {
  v8::ResourceConstraints constraints;
  constraints.ConfigureDefaults(STANDARD_ISOLATE_MEMORY_LIMIT, 0);
  constraints.set_max_old_generation_size_in_bytes(STANDARD_ISOLATE_MEMORY_LIMIT);
  v8::Isolate::CreateParams params;
  params.constraints = constraints;
  return params;
}

size_t StandaloneIsolateLimitEnforcer::onNearHeapLimit(
    void* data, size_t currentHeapLimit, size_t initialHeapLimit) {
  (void)initialHeapLimit;
  auto& enforcer = *static_cast<StandaloneIsolateLimitEnforcer*>(data);
  enforcer.heapLimitHit = true;
  // Condemn on the workerd thread (heap limits are hit while allocating on it): evict the
  // loader entry and terminate every in-flight invocation of this isolate. V8 requires a
  // small emergency increase here; returning the unchanged limit makes a tight allocation
  // loop abort the entire process before TerminateExecution can unwind the isolate.
  enforcer.isolateState->condemnFromWatchdog(StandaloneLimitViolation::MEMORY);
  enforcer.isolateState->takeCondemnedException(StandaloneLimitViolation::MEMORY);
  KJ_REQUIRE(currentHeapLimit <= static_cast<size_t>(kj::maxValue) - MEMORY_TERMINATION_HEADROOM);
  return currentHeapLimit + MEMORY_TERMINATION_HEADROOM;
}

void StandaloneIsolateLimitEnforcer::customizeIsolate(v8::Isolate* isolate) {
  // Register for cross-thread termination before any turn can run.
  isolateState->setV8Isolate(*isolate, jsg::IsolateBase::from(isolate).getIsolateLiveness());
  isolate->AddNearHeapLimitCallback(onNearHeapLimit, this);
}

ActorCacheSharedLruOptions StandaloneIsolateLimitEnforcer::getActorCacheLruOptions() {
  // Identical to the unlimited standalone enforcer: Durable Object cache sizing is not part
  // of the Standard Worker limits.
  return {.softLimit = STANDARD_ACTOR_CACHE_SOFT_LIMIT,
    .hardLimit = STANDARD_ACTOR_CACHE_HARD_LIMIT,
    .staleTimeout = 30 * kj::SECONDS,
    .dirtyListByteLimit = 8 * (1ull << 20),
    .maxKeysPerRpc = 128,
    .neverFlush = true};
}

kj::Own<void> StandaloneIsolateLimitEnforcer::enterStartupJs(
    jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const {
  return enterBoundedStartup(limitErrorOrTime);
}

kj::Own<void> StandaloneIsolateLimitEnforcer::enterStartupPython(
    jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const {
  return enterBoundedStartup(limitErrorOrTime);
}

kj::Own<void> StandaloneIsolateLimitEnforcer::enterDynamicImportJs(
    jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const {
  // Dynamic imports re-run the bounded startup budget: a lazily compiled module is startup
  // work just like initial compilation.
  return enterBoundedStartup(limitErrorOrTime);
}

kj::Own<void> StandaloneIsolateLimitEnforcer::enterLoggingJs(
    jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const {
  return {};
}

kj::Own<void> StandaloneIsolateLimitEnforcer::enterInspectorJs(
    jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& limitErrorOrTime) const {
  return {};
}

void StandaloneIsolateLimitEnforcer::completedRequest(kj::StringPtr id) const {}

bool StandaloneIsolateLimitEnforcer::exitJs(jsg::Lock& lock) const {
  // Secondary memory check covering ArrayBuffer/Wasm backing stores, which the V8 heap
  // constraint does not bound. Attributed memory only; native caches and isolate-external
  // allocations are the deployment environment's responsibility.
  v8::HeapStatistics stats;
  lock.v8Isolate->GetHeapStatistics(&stats);
  auto attributed =
      static_cast<size_t>(stats.used_heap_size()) + static_cast<size_t>(stats.external_memory());
  if (attributed > STANDARD_ISOLATE_MEMORY_LIMIT) {
    heapLimitHit = true;
    isolateState->takeCondemnedException(StandaloneLimitViolation::MEMORY);
  }
  return false;
}

void StandaloneIsolateLimitEnforcer::reportMetrics(IsolateObserver& isolateMetrics) const {}

bool StandaloneIsolateLimitEnforcer::hasExcessivelyExceededHeapLimit() const {
  return heapLimitHit;
}

const TrackedWasmInstanceList& StandaloneIsolateLimitEnforcer::getTrackedWasmInstances() const {
  return trackedWasmInstances;
}

}  // namespace workerd
