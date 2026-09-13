// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Standalone runtime request resource limits: invocation CPU time and subrequest budget for
// dynamically-loaded Workers, plus the per-isolate state that condemns a broken isolate, and
// the simultaneous-outbound-connection accounting for one invocation.
//
// This module keeps every standalone-specific limit implementation out of the shared I/O core.
// The shared code (`IoContext`, `LimitEnforcer`) only calls the generic virtual hooks; all
// policy, accounting, and the cross-thread watchdog live here so the module can be ported or
// upstreamed independently.

#pragma once

#include <workerd/io/io-channels.h>
#include <workerd/io/limit-enforcer.h>
#include <workerd/io/outcome.capnp.h>
#include <workerd/jsg/setup.h>
#include <workerd/server/thread-cpu-clock.h>

#include <v8-isolate.h>

#include <kj/async.h>
#include <kj/mutex.h>
#include <kj/refcount.h>
#include <kj/thread.h>
#include <kj/vector.h>

namespace workerd {

// The per-invocation request limits for one dynamically-loaded Worker, already resolved to
// final values by the caller (the JSG/Loader DTO is validated and converted at the entry
// point; native counters never hold JS values). `kj::none` means the dimension is not limited;
// the trusted loader host materializes Standard defaults before reaching this point.
struct EffectiveResourceLimits {
  kj::Maybe<kj::Duration> cpu;
  kj::Maybe<uint32_t> subrequests;
};

// Per-dimension minimum of two limit sets. Omitted values inherit the other set rather than
// becoming unlimited, so a narrower entrypoint limit can never widen the Worker-level budget.
EffectiveResourceLimits minEffectiveResourceLimits(
    const EffectiveResourceLimits& a, const EffectiveResourceLimits& b);

// The Standard configurability ceiling of the Worker Loader limits dictionary.
constexpr kj::Duration STANDARD_MAX_INVOCATION_CPU = 300 * kj::SECONDS;
constexpr uint32_t STANDARD_MAX_SUBREQUESTS = 10'000'000;

// Validates and converts the Worker Loader `limits` DTO into native limits. Throws with a
// stable message when a present value is zero or above the Standard configurability ceiling;
// omitted values stay omitted (defaults are materialized by the trusted loader host, not
// re-applied here).
EffectiveResourceLimits validateResourceLimits(const ResourceLimits& limits);

// Which budget condemned an isolate, so outcomes and exceptions stay accurate.
enum class StandaloneLimitViolation { CPU, MEMORY };

// Guard against a v8 isolate being destroyed while the watchdog thread targets it. Defined in
// the .c++ file; it holds the isolate's jsg::IsolateLiveness, whose detached flag turns the
// watchdog's termination call into a no-op once the isolate is torn down.
struct StandaloneWatchdogIsolateRefs;

// Shared per-isolate limit state for one dynamically-loaded Worker isolate. It owns:
//
// - the condemned flag and violation kind (atomic; the watchdog thread transitions these),
// - the shared failure promise (KJ objects; only used on the workerd event-loop thread),
// - the isolate references the watchdog needs to request V8 termination cross-thread.
//
// The state never holds per-request counters. It is created when the dynamic isolate is
// created and kept alive by both the isolate's enforcer and the loader stub, so it always
// outlives any active turn registered against it.
class StandaloneIsolateLimitState final: public kj::Refcounted {
 public:
  StandaloneIsolateLimitState();
  ~StandaloneIsolateLimitState() noexcept(false);

  // Marks the isolate condemned and requests V8 termination. Safe to call from the watchdog
  // thread: performs only atomic transitions and the thread-safe V8 termination primitive.
  // All KJ work (promise rejection, loader-cache removal) happens later on the workerd thread
  // via takeCondemnedException().
  void condemnFromWatchdog(StandaloneLimitViolation violation);

  // True once condemnFromWatchdog() or takeCondemnedException() has fired. Safe from any
  // thread.
  bool isCondemned() const {
    return condemned.load(std::memory_order_acquire);
  }

  // The violation that condemned the isolate; only meaningful once isCondemned() is true.
  kj::Maybe<StandaloneLimitViolation> violation() const;

  // Registers the running isolate for cross-thread termination. Called once, on the workerd
  // thread, before the isolate's first watchdog-registered JS turn.
  void setV8Isolate(v8::Isolate& isolate, kj::Arc<const jsg::IsolateLiveness> liveness);

  // On the workerd thread: observes a condemned isolate (or an enforce-side overrun) and
  // performs the one-time KJ-side teardown -- rejects the shared failure promise and invokes
  // the eviction callback that removes the isolate from the loader cache. Returns the stable
  // exception callers must propagate; every call after the first returns a clone.
  kj::Exception takeCondemnedException(StandaloneLimitViolation violation);

  // A per-request branch of the shared failure promise. It rejects with the stable
  // condemnation exception when the isolate is condemned, so every in-flight IoContext of the
  // isolate settles in bounded time.
  kj::Promise<void> onLimitsExceeded();

  // Installs the callback that removes the condemned isolate from the loader cache. Called
  // once by the loader wiring when the isolate is created; must be set before the first JS
  // turn.
  void setEvictionCallback(kj::Function<void()> callback);

 private:
  std::atomic<bool> condemned{false};
  std::atomic<StandaloneLimitViolation> violation_{StandaloneLimitViolation::CPU};

  kj::Own<StandaloneWatchdogIsolateRefs> watchdogRefs;

  // KJ state, touched only on the workerd event-loop thread (like every promise graph of the
  // isolate). `resolved` guards the one-time teardown.
  struct ThreadState {
    kj::Maybe<kj::ForkedPromise<void>> failure;
    kj::Maybe<kj::Own<kj::PromiseFulfiller<void>>> failureFulfiller;
    kj::Maybe<kj::Exception> failureException;
    kj::Maybe<kj::Function<void()>> evictionCallback;
    bool resolved = false;
  };
  kj::MutexGuarded<ThreadState> threadState;
};

// Samples registered JS turns on a bounded cadence and terminates isolates whose invocation
// exceeded its CPU budget. The watchdog thread performs no KJ work and takes no locks in
// common with the event loop other than its own registry mutex; it only transitions atomic
// state and calls the thread-safe V8 termination primitive.
//
// The production instance is process-global (`defaultInstance()`); it starts its sampling
// thread lazily on the first registration. Tests construct standalone instances with an
// injected fake clock and drive `pollOnce()` manually.
class StandaloneLimitWatchdog final {
 public:
  // Sampling cadence and the resulting worst-case overshoot of the CPU budget. Fixed
  // implementation constants: the watchdog is not operator-tunable.
  static constexpr kj::Duration SAMPLE_INTERVAL = 5 * kj::MILLISECONDS;

  // `runSamplingThread` starts the background sampling thread; tests pass false and drive
  // pollOnce() themselves.
  explicit StandaloneLimitWatchdog(ThreadCpuClock& clock, bool runSamplingThread);
  ~StandaloneLimitWatchdog() noexcept(false);

  // The process-global production watchdog, sampling ThreadCpuClock::get().
  static StandaloneLimitWatchdog& defaultInstance();

  // One sampling sweep: checks the innermost registered turn of every thread and condemns the
  // isolates of turns that exceeded their deadline. Exposed for deterministic tests.
  void pollOnce();

  // A live JS-turn registration. Dropping the owning scope must call finishTurn() on the
  // turn's own thread; the registration itself is a plain token.
  struct TurnToken {
    uint64_t epoch;
    uint64_t threadHandle;
  };

  // Registers one JS turn of `invocation` running on the calling thread. `remainingCpu` is
  // the turn's deadline relative to the thread CPU clock, or kj::none when the invocation has
  // no CPU budget (the registration then only serves nested-turn bookkeeping).
  TurnToken registerTurn(StandaloneIsolateLimitState& state,
      const void* invocation,
      kj::Maybe<kj::Duration> remainingCpu);

  // Removes the registration, shifts the deadline of the turn that now becomes innermost on
  // this thread so nested turns are not double-billed, and releases the thread capture.
  void finishTurn(const TurnToken& token, kj::Duration turnDelta);

  ThreadCpuClock& clock() const {
    return *clock_;
  }

 private:
  struct ActiveTurn {
    uint64_t threadHandle;
    StandaloneIsolateLimitState* state;
    uint64_t epoch;
    kj::Maybe<kj::Duration> deadline;  // thread-CPU reading at which the turn is over budget
  };

  void runThreadIfNeeded();
  void samplingLoop();

  ThreadCpuClock* clock_;
  kj::MutexGuarded<kj::Vector<ActiveTurn>> registry;
  std::atomic<uint64_t> nextEpoch{1};
  std::atomic<bool> threadStarted{false};
  std::atomic<bool> stopThread{false};
  kj::Maybe<kj::Thread> samplingThread;
};

// The simultaneous-outbound-connection budget of one Standard invocation.
constexpr uint32_t STANDARD_MAX_SIMULTANEOUS_OUTBOUND_CONNECTIONS = 6;

// Creates the per-request LimitEnforcer for one IoContext of a dynamically-loaded Worker.
//
// The enforcer owns this invocation's CPU accumulation, subrequest count, and outbound
// connection accounting. CPU enforcement uses the isolate state's watchdog: each JS turn
// registers a deadline, and overruns terminate the isolate via V8. Subrequest overruns throw a
// stable exception before the call produces any side effect.
kj::Own<LimitEnforcer> newRequestLimitEnforcer(
    kj::Rc<StandaloneIsolateLimitState> isolateState, EffectiveResourceLimits limits);

// Test-only overload binding the enforcer to a specific (injected) watchdog and clock.
kj::Own<LimitEnforcer> newRequestLimitEnforcer(kj::Rc<StandaloneIsolateLimitState> isolateState,
    EffectiveResourceLimits limits,
    StandaloneLimitWatchdog& watchdog);

// Wraps an outbound channel so that every tunneled connect() it dispatches on behalf of the
// current invocation acquires one simultaneous-connection slot first. The seventh and later
// concurrent connects queue before any side effect and proceed when a slot is released; the
// lease attached to the tunnel releases the slot when the pump settles or is cancelled.
// Everything else (fetch requests, transfer checks, tokens) forwards untouched. Apply to the
// outbound channels of dynamic Workers whose invocation limits were configured; channels of
// trusted platform Workers are never wrapped.
kj::Own<IoChannelFactory::SubrequestChannel> newConnectionAccountingChannel(
    kj::Own<IoChannelFactory::SubrequestChannel> inner);

}  // namespace workerd
