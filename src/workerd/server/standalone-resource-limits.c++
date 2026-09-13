// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "standalone-resource-limits.h"

#include <workerd/io/io-channels.h>
#include <workerd/io/io-context.h>
#include <workerd/jsg/exception.h>
#include <workerd/util/exception.h>

#include <kj/debug.h>
#include <kj/hash.h>
#include <kj/thread.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace workerd {
namespace {

// Removes one element, preserving order; kj::Vector has no index removal.
template <typename T>
void removeIndex(kj::Vector<T>& vector, uint index) {
  for (uint i = index; i + 1 < vector.size(); ++i) {
    vector[i] = kj::mv(vector[i + 1]);
  }
  vector.removeLast();
}

}  // namespace

// ========================================================================================
// Watchdog isolate references
//
// Defined at workerd scope to match the forward declaration in the header.

struct StandaloneWatchdogIsolateRefs final {
  // Written once on the workerd thread before any turn of the isolate can be registered; the
  // watchdog thread reads these only for turns already present in the registry, and a turn's
  // registration happens after this write on the same thread (release/acquire ordering).
  std::atomic<v8::Isolate*> isolate{nullptr};

  // Keeps the liveness object alive for as long as this state exists. `livenessRaw` is what
  // the watchdog dereferences; the liveness object reports death via detach() just before the
  // v8 isolate is destroyed, turning termination calls into no-ops.
  kj::Arc<const jsg::IsolateLiveness> liveness;
  std::atomic<const jsg::IsolateLiveness*> livenessRaw{nullptr};
};

namespace {

// ========================================================================================
// Stable exception text

kj::Exception stableCondemnationException(StandaloneLimitViolation violation) {
  switch (violation) {
    case StandaloneLimitViolation::MEMORY: {
      auto e = JSG_KJ_EXCEPTION(OVERLOADED, Error, "Worker has exceeded memory limit.");
      e.setDetail(MEMORY_LIMIT_DETAIL_ID, kj::heapArray<kj::byte>(0));
      return e;
    }
    case StandaloneLimitViolation::CPU: {
      auto e = JSG_KJ_EXCEPTION(OVERLOADED, Error, "Worker exceeded CPU time limit.");
      e.setDetail(CPU_LIMIT_DETAIL_ID, kj::heapArray<kj::byte>(0));
      return e;
    }
  }
  KJ_UNREACHABLE;
}

// ========================================================================================
// Outbound connection slots

// Per-invocation simultaneous-connection accounting. Refcounted so outstanding response-header
// waits can outlive the RequestLimitEnforcer during IoContext teardown. All access is on the
// workerd event-loop thread.
struct OutboundConnectionSlots final: public kj::Refcounted {
  explicit OutboundConnectionSlots(uint32_t limitParam): limit(limitParam) {}

  kj::Promise<kj::Own<LimitEnforcer::OutboundConnectionLease>> acquire() {
    if (active < limit) {
      ++active;
      return newLease();
    }
    auto paf = kj::newPromiseAndFulfiller<void>();
    queue.add(kj::mv(paf.fulfiller));
    return paf.promise.then([self = kj::addRef(*this)]() mutable { return self->newLease(); });
  }

  void release() {
    // Hand the slot to the oldest waiter that is still waiting; cancelled waiters (their
    // request settled) are dropped, keeping the slot available.
    while (!queue.empty()) {
      auto fulfiller = kj::mv(queue.front());
      removeIndex(queue, 0);
      if (fulfiller->isWaiting()) {
        fulfiller->fulfill();
        return;
      }
    }
    KJ_REQUIRE(active > 0, "unbalanced outbound connection release");
    --active;
  }

  const uint32_t limit;
  uint32_t active = 0;
  kj::Vector<kj::Own<kj::PromiseFulfiller<void>>> queue;

 private:
  class Lease final: public LimitEnforcer::OutboundConnectionLease {
   public:
    explicit Lease(kj::Rc<OutboundConnectionSlots> slotsParam): slots(kj::mv(slotsParam)) {}
    ~Lease() noexcept(false) override {
      slots->release();
    }

   private:
    kj::Rc<OutboundConnectionSlots> slots;
  };

  kj::Own<LimitEnforcer::OutboundConnectionLease> newLease() {
    return kj::heap<Lease>(kj::addRef(*this));
  }
};

// ========================================================================================
// RequestLimitEnforcer

class RequestLimitEnforcer final: public LimitEnforcer {
 public:
  RequestLimitEnforcer(kj::Rc<StandaloneIsolateLimitState> isolateStateParam,
      EffectiveResourceLimits limitsParam,
      StandaloneLimitWatchdog& watchdogParam)
      : isolateState(kj::mv(isolateStateParam)),
        limits(limitsParam),
        watchdog(watchdogParam),
        slots(kj::rc<OutboundConnectionSlots>(STANDARD_MAX_SIMULTANEOUS_OUTBOUND_CONNECTIONS)) {}

  void requireLimitsNotExceeded() override {
    throwIfCondemned();
    KJ_IF_SOME(budget, limits.cpu) {
      if (cpuUsed > budget) {
        kj::throwRecoverableException(
            isolateState->takeCondemnedException(StandaloneLimitViolation::CPU));
      }
    }
  }

  kj::Own<void> enterJs(jsg::Lock& lock, IoContext& context) override {
    throwIfCondemned();
    auto turnStart = watchdog.clock().currentThreadCpu();
    kj::Maybe<kj::Duration> remaining;
    KJ_IF_SOME(budget, limits.cpu) {
      remaining = budget > cpuUsed ? budget - cpuUsed : 0 * kj::SECONDS;
    }
    auto token = watchdog.registerTurn(*isolateState, static_cast<const void*>(this), remaining);
    return kj::heap<TurnScope>(*this, token, turnStart);
  }

  void topUpActor() override {}

  void newSubrequest(bool isInHouse) override {
    // The in-house flag marks Cloudflare-internal service traffic in other runtime hosts. In
    // the standalone runtime every subrequest that reaches a tenant isolate's enforcer is
    // tenant-observable (platform traffic uses channels of the trusted system Workers, which
    // are not limited), so the budget applies to all of them. The unit tests freeze this
    // audit.
    (void)isInHouse;
    throwIfCondemned();
    KJ_IF_SOME(budget, limits.subrequests) {
      if (subrequestsUsed >= budget) {
        // Fail closed before any side effect of the (N+1)th subrequest.
        JSG_FAIL_REQUIRE(Error, "Too many subrequests.");
      }
      ++subrequestsUsed;
    }
  }

  void newKvRequest(KvOpType op) override {}
  void newAnalyticsEngineRequest() override {}

  kj::Promise<void> limitDrain() override {
    return kj::NEVER_DONE;
  }
  kj::Promise<void> limitScheduled() override {
    return kj::NEVER_DONE;
  }
  kj::Duration getAlarmLimit() override {
    return 15 * kj::MINUTES;
  }
  size_t getBufferingLimit() override {
    return kj::maxValue;
  }

  kj::Maybe<EventOutcome> getLimitsExceeded() override {
    KJ_IF_SOME(condemnViolation, isolateState->violation()) {
      switch (condemnViolation) {
        case StandaloneLimitViolation::CPU:
          return EventOutcome::EXCEEDED_CPU;
        case StandaloneLimitViolation::MEMORY:
          return EventOutcome::EXCEEDED_MEMORY;
      }
    }
    return kj::none;
  }

  kj::Promise<void> onLimitsExceeded() override {
    return isolateState->onLimitsExceeded();
  }

  void setCpuLimitNearlyExceededCallback(kj::Function<void(void)>) override {}

  kj::Promise<kj::Own<OutboundConnectionLease>> newOutboundConnection() override {
    throwIfCondemned();
    return slots->acquire();
  }

  void reportMetrics(RequestObserver& requestMetrics) override {}
  kj::Duration consumeTimeElapsedForPeriodicLogging() override {
    return 0 * kj::SECONDS;
  }
  size_t getSqliteMemoryUsage() const override {
    return 0;
  }

 private:
  class TurnScope final {
   public:
    TurnScope(RequestLimitEnforcer& enforcerParam,
        StandaloneLimitWatchdog::TurnToken tokenParam,
        kj::Duration turnStartParam)
        : enforcer(enforcerParam),
          token(tokenParam),
          turnStart(turnStartParam) {}

    ~TurnScope() noexcept(false) {
      auto turnEnd = enforcer.watchdog.clock().currentThreadCpu();
      auto turnDelta = turnEnd - turnStart;
      // Unregister first so the enclosing turn's deadline is adjusted before any later
      // sampling, then bill this turn's CPU (excluding nested turns) to the invocation.
      enforcer.watchdog.finishTurn(token, turnDelta);
      enforcer.cpuUsed += turnDelta;

      // If the invocation overran its budget but the watchdog did not terminate this turn
      // (possible only with deterministic test clocks or a boundary-exact overrun), condemn
      // now. The exception is stashed in the state; the next limits check throws it.
      KJ_IF_SOME(budget, enforcer.limits.cpu) {
        if (enforcer.cpuUsed > budget) {
          enforcer.isolateState->takeCondemnedException(StandaloneLimitViolation::CPU);
        }
      }
    }

   private:
    RequestLimitEnforcer& enforcer;
    StandaloneLimitWatchdog::TurnToken token;
    kj::Duration turnStart;
  };

  void throwIfCondemned() {
    if (isolateState->isCondemned()) {
      kj::throwRecoverableException(isolateState->takeCondemnedException(
          isolateState->violation().orDefault(StandaloneLimitViolation::CPU)));
    }
  }

  kj::Rc<StandaloneIsolateLimitState> isolateState;
  EffectiveResourceLimits limits;
  StandaloneLimitWatchdog& watchdog;
  kj::Rc<OutboundConnectionSlots> slots;

  // Workerd-thread-only counters; JS turns and subrequest dispatch are serialized on the
  // event-loop thread.
  kj::Duration cpuUsed = 0 * kj::SECONDS;
  uint32_t subrequestsUsed = 0;
};

// ========================================================================================
// Connection accounting channel

class ConnectionAccountingResponse final: public kj::HttpService::Response {
 public:
  ConnectionAccountingResponse(
      kj::HttpService::Response& inner, kj::Own<LimitEnforcer::OutboundConnectionLease> lease)
      : inner(inner),
        lease(kj::mv(lease)) {}

  kj::Own<kj::AsyncOutputStream> send(uint statusCode,
      kj::StringPtr statusText,
      const kj::HttpHeaders& headers,
      kj::Maybe<uint64_t> expectedBodySize = kj::none) override {
    auto body = inner.send(statusCode, statusText, headers, expectedBodySize);
    release();
    return body;
  }

  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override {
    auto webSocket = inner.acceptWebSocket(headers);
    release();
    return webSocket;
  }

 private:
  void release() {
    KJ_REQUIRE(lease != kj::none, "response was sent more than once");
    lease = kj::none;
  }

  kj::HttpService::Response& inner;
  kj::Maybe<kj::Own<LimitEnforcer::OutboundConnectionLease>> lease;
};

class ConnectionAccountingConnectResponse final: public kj::HttpService::ConnectResponse {
 public:
  ConnectionAccountingConnectResponse(kj::HttpService::ConnectResponse& inner,
      kj::Own<LimitEnforcer::OutboundConnectionLease> lease)
      : inner(inner),
        lease(kj::mv(lease)) {}

  void accept(uint statusCode, kj::StringPtr statusText, const kj::HttpHeaders& headers) override {
    inner.accept(statusCode, statusText, headers);
    release();
  }

  kj::Own<kj::AsyncOutputStream> reject(uint statusCode,
      kj::StringPtr statusText,
      const kj::HttpHeaders& headers,
      kj::Maybe<uint64_t> expectedBodySize = kj::none) override {
    auto body = inner.reject(statusCode, statusText, headers, expectedBodySize);
    release();
    return body;
  }

 private:
  void release() {
    KJ_REQUIRE(lease != kj::none, "connection response was sent more than once");
    lease = kj::none;
  }

  kj::HttpService::ConnectResponse& inner;
  kj::Maybe<kj::Own<LimitEnforcer::OutboundConnectionLease>> lease;
};

class ConnectionAccountingWorkerInterface final: public WorkerInterface {
 public:
  ConnectionAccountingWorkerInterface(kj::Own<WorkerInterface> inner, LimitEnforcer& limitEnforcer)
      : inner(kj::mv(inner)),
        limitEnforcer(limitEnforcer) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& response) override {
    // A fetch-style subrequest occupies one of the invocation's six outbound slots from
    // dispatch until its response headers arrive. Acquiring before forwarding also guarantees
    // that a queued seventh request cannot produce an upstream side effect. KV, R2, D1,
    // Cache, service-binding, and ordinary fetch operations all use this request path.
    auto lease = co_await limitEnforcer.newOutboundConnection();
    ConnectionAccountingResponse wrappedResponse(response, kj::mv(lease));
    co_await inner->request(method, url, headers, requestBody, wrappedResponse);
  }

  kj::Promise<void> connect(kj::StringPtr host,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings settings) override {
    // Acquire the invocation's connection slot before any dial can happen; the caller's
    // connect stream buffers until accept(), so queueing produces no side effect. The slot is
    // released on accept/reject (the CONNECT response), not when the resulting tunnel closes.
    auto lease = co_await limitEnforcer.newOutboundConnection();
    ConnectionAccountingConnectResponse wrappedResponse(response, kj::mv(lease));
    co_await inner->connect(host, headers, connection, wrappedResponse, kj::mv(settings));
  }

  kj::Promise<void> prewarm(kj::StringPtr url) override {
    return inner->prewarm(url);
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date scheduledTime, kj::StringPtr cron) override {
    return inner->runScheduled(scheduledTime, cron);
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date scheduledTime, uint32_t retryCount) override {
    return inner->runAlarm(scheduledTime, retryCount);
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return inner->customEvent(kj::mv(event));
  }

 private:
  kj::Own<WorkerInterface> inner;
  LimitEnforcer& limitEnforcer;
};

class ConnectionAccountingChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  explicit ConnectionAccountingChannel(kj::Own<IoChannelFactory::SubrequestChannel> inner)
      : inner(kj::mv(inner)) {}

  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata metadata) override {
    return kj::heap<ConnectionAccountingWorkerInterface>(
        inner->startRequest(kj::mv(metadata)), IoContext::current().getLimitEnforcer());
  }

  void requireAllowsTransfer() override {
    inner->requireAllowsTransfer();
  }

  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage usage) override {
    return inner->getTokenMaybeSync(usage);
  }

 private:
  kj::Own<IoChannelFactory::SubrequestChannel> inner;
};

kj::Maybe<ResourceLimits> toResourceLimits(const EffectiveResourceLimits& limits) {
  if (limits.cpu == kj::none && limits.subrequests == kj::none) {
    return kj::none;
  }

  ResourceLimits result;
  KJ_IF_SOME(cpu, limits.cpu) {
    result.cpuMs = static_cast<uint32_t>(cpu / kj::MILLISECONDS);
  }
  result.subRequests = limits.subrequests;
  return kj::mv(result);
}

// Attenuates a WorkerLoader capability placed in a dynamic Worker's env. A child may delegate
// that capability again, but no descendant can widen either dimension above the immutable
// containing Worker's effective request limits.
class ResourceLimitedWorkerLoaderChannel final: public IoChannelFactory::WorkerLoaderChannel {
 public:
  ResourceLimitedWorkerLoaderChannel(
      kj::Own<IoChannelFactory::WorkerLoaderChannel> inner, EffectiveResourceLimits ceiling)
      : inner(kj::mv(inner)),
        ceiling(kj::mv(ceiling)) {}

  kj::Own<WorkerStubChannel> loadIsolate(kj::Maybe<kj::String> name,
      kj::Function<kj::Promise<DynamicWorkerSource>()> fetchSource) override {
    auto attenuatedFetchSource = [fetchSource = kj::mv(fetchSource),
                                     ceiling =
                                         ceiling]() mutable -> kj::Promise<DynamicWorkerSource> {
      return fetchSource().then([ceiling](DynamicWorkerSource source) mutable {
        EffectiveResourceLimits requested;
        KJ_IF_SOME(limits, source.limits) {
          requested = validateResourceLimits(limits);
        }
        source.limits = toResourceLimits(minEffectiveResourceLimits(ceiling, requested));
        return kj::mv(source);
      });
    };
    return inner->loadIsolate(kj::mv(name), kj::mv(attenuatedFetchSource));
  }

  kj::Own<IoChannelFactory::WorkerLoaderChannel> forTransfer() override {
    return kj::refcounted<ResourceLimitedWorkerLoaderChannel>(inner->forTransfer(), ceiling);
  }

 private:
  kj::Own<IoChannelFactory::WorkerLoaderChannel> inner;
  EffectiveResourceLimits ceiling;
};

}  // namespace

// ========================================================================================
// EffectiveResourceLimits

EffectiveResourceLimits validateResourceLimits(const ResourceLimits& limits) {
  EffectiveResourceLimits result;
  KJ_IF_SOME(cpuMs, limits.cpuMs) {
    JSG_REQUIRE(cpuMs > 0, TypeError, "Dynamic Worker cpuMs must be a positive integer.");
    JSG_REQUIRE(cpuMs * kj::MILLISECONDS <= STANDARD_MAX_INVOCATION_CPU, TypeError,
        "Dynamic Worker cpuMs must be at most 300000 (300 seconds).");
    result.cpu = cpuMs * kj::MILLISECONDS;
  }
  KJ_IF_SOME(subRequests, limits.subRequests) {
    JSG_REQUIRE(
        subRequests > 0, TypeError, "Dynamic Worker subRequests must be a positive integer.");
    JSG_REQUIRE(subRequests <= STANDARD_MAX_SUBREQUESTS, TypeError,
        "Dynamic Worker subRequests must be at most 10000000.");
    result.subrequests = subRequests;
  }
  return result;
}

EffectiveResourceLimits minEffectiveResourceLimits(
    const EffectiveResourceLimits& a, const EffectiveResourceLimits& b) {
  EffectiveResourceLimits result;
  // Per dimension: the narrower declared value wins; omission never widens to unlimited.
  KJ_IF_SOME(ac, a.cpu) {
    KJ_IF_SOME(bc, b.cpu) {
      result.cpu = kj::min(ac, bc);
    } else {
      result.cpu = ac;
    }
  } else {
    result.cpu = b.cpu;
  }
  KJ_IF_SOME(as, a.subrequests) {
    KJ_IF_SOME(bs, b.subrequests) {
      result.subrequests = kj::min(as, bs);
    } else {
      result.subrequests = as;
    }
  } else {
    result.subrequests = b.subrequests;
  }
  return result;
}

// ========================================================================================
// StandaloneIsolateLimitState

StandaloneIsolateLimitState::StandaloneIsolateLimitState()
    : watchdogRefs(kj::heap<StandaloneWatchdogIsolateRefs>()) {
  auto paf = kj::newPromiseAndFulfiller<void>();
  auto state = threadState.lockExclusive();
  state->failure = kj::mv(paf.promise).fork();
  state->failureFulfiller = kj::mv(paf.fulfiller);
}

StandaloneIsolateLimitState::~StandaloneIsolateLimitState() noexcept(false) = default;

void StandaloneIsolateLimitState::condemnFromWatchdog(StandaloneLimitViolation violationParam) {
  violation_.store(violationParam, std::memory_order_release);
  condemned.store(true, std::memory_order_release);

  // Terminate the running script. TerminateExecution is documented as safe to call across
  // threads; the liveness guard turns the call into a no-op once the isolate is torn down.
  auto* refs = watchdogRefs.get();
  auto* liveness = refs->livenessRaw.load(std::memory_order_acquire);
  if (liveness != nullptr && liveness->isIsolateAlive()) {
    if (auto* isolate = refs->isolate.load(std::memory_order_acquire)) {
      isolate->TerminateExecution();
    }
  }
}

kj::Maybe<StandaloneLimitViolation> StandaloneIsolateLimitState::violation() const {
  if (!isCondemned()) return kj::none;
  return violation_.load(std::memory_order_relaxed);
}

void StandaloneIsolateLimitState::setV8Isolate(
    v8::Isolate& isolate, kj::Arc<const jsg::IsolateLiveness> liveness) {
  auto* refs = watchdogRefs.get();
  refs->liveness = kj::mv(liveness);
  refs->livenessRaw.store(refs->liveness.get(), std::memory_order_release);
  refs->isolate.store(&isolate, std::memory_order_release);
}

kj::Exception StandaloneIsolateLimitState::takeCondemnedException(
    StandaloneLimitViolation violationParam) {
  auto state = threadState.lockExclusive();
  if (state->resolved) {
    return KJ_ASSERT_NONNULL(state->failureException).clone();
  }
  state->resolved = true;
  violation_.store(violationParam, std::memory_order_release);
  condemned.store(true, std::memory_order_release);

  auto exception = stableCondemnationException(violationParam);
  state->failureException = exception.clone();

  KJ_IF_SOME(evict, state->evictionCallback) {
    // Remove the isolate from the loader cache so later invocations of the same immutable
    // Version create a fresh isolate.
    evict();
  }
  KJ_IF_SOME(fulfiller, state->failureFulfiller) {
    // Settle every in-flight invocation of this isolate.
    fulfiller->reject(exception.clone());
  }
  return exception;
}

kj::Promise<void> StandaloneIsolateLimitState::onLimitsExceeded() {
  auto state = threadState.lockExclusive();
  return KJ_REQUIRE_NONNULL(state->failure, "failure promise must be initialized").addBranch();
}

void StandaloneIsolateLimitState::setEvictionCallback(kj::Function<void()> callback) {
  threadState.lockExclusive()->evictionCallback = kj::mv(callback);
}

// ========================================================================================
// StandaloneLimitWatchdog

StandaloneLimitWatchdog::StandaloneLimitWatchdog(ThreadCpuClock& clock, bool runSamplingThread)
    : clock_(&clock) {
  if (runSamplingThread) runThreadIfNeeded();
}

StandaloneLimitWatchdog::~StandaloneLimitWatchdog() noexcept(false) {
  stopThread.store(true, std::memory_order_relaxed);
  // Dropping kj::Thread joins the sampling thread.
}

StandaloneLimitWatchdog& StandaloneLimitWatchdog::defaultInstance() {
  static StandaloneLimitWatchdog instance(ThreadCpuClock::get(), true);
  return instance;
}

void StandaloneLimitWatchdog::runThreadIfNeeded() {
  bool expected = false;
  if (stopThread.load(std::memory_order_relaxed)) return;
  if (threadStarted.compare_exchange_strong(expected, true)) {
    samplingThread.emplace([this]() { samplingLoop(); });
  }
}

void StandaloneLimitWatchdog::samplingLoop() {
  const auto interval = std::chrono::nanoseconds(SAMPLE_INTERVAL / kj::NANOSECONDS);
  while (!stopThread.load(std::memory_order_relaxed)) {
    pollOnce();
    std::this_thread::sleep_for(interval);
  }
}

StandaloneLimitWatchdog::TurnToken StandaloneLimitWatchdog::registerTurn(
    StandaloneIsolateLimitState& state,
    const void* invocation,
    kj::Maybe<kj::Duration> remainingCpu) {
  auto threadHandle = clock_->captureCurrentThread();
  // Read the thread's CPU time after capturing the handle so the deadline lives on the same
  // per-thread timeline the watchdog samples.
  auto now = clock_->currentThreadCpu();
  uint64_t epoch = nextEpoch.fetch_add(1, std::memory_order_relaxed);

  {
    auto entries = registry.lockExclusive();
    entries->add(ActiveTurn{.threadHandle = threadHandle,
      .state = &state,
      .epoch = epoch,
      .deadline = remainingCpu.map([&now](kj::Duration remaining) { return now + remaining; })});
  }
  return TurnToken{.epoch = epoch, .threadHandle = threadHandle};
}

void StandaloneLimitWatchdog::finishTurn(const TurnToken& token, kj::Duration turnDelta) {
  clock_->releaseCapture(token.threadHandle);
  auto entries = registry.lockExclusive();
  for (uint i = 0; i < entries->size(); ++i) {
    if ((*entries)[i].epoch == token.epoch) {
      removeIndex(*entries, i);
      break;
    }
  }
  // The turn that now becomes innermost on this thread is an enclosing turn; it must not be
  // billed for the CPU the finished (nested) turn consumed, so shift its deadline forward.
  uint64_t latestEpoch = 0;
  ActiveTurn* enclosing = nullptr;
  for (auto& entry: *entries) {
    if (entry.threadHandle == token.threadHandle && entry.epoch > latestEpoch) {
      latestEpoch = entry.epoch;
      enclosing = &entry;
    }
  }
  if (enclosing != nullptr) {
    KJ_IF_SOME(deadline, enclosing->deadline) {
      enclosing->deadline = deadline + turnDelta;
    }
  }
}

void StandaloneLimitWatchdog::pollOnce() {
  auto entries = registry.lockExclusive();
  // Turns on one thread are stack-ordered, so scanning from the newest entry and skipping
  // threads already sampled checks exactly the innermost turn of every thread. The registry
  // lock also covers the thread captures: entries are removed on their owning thread under
  // this same mutex, so a capture read here is valid, and holding the lock across the
  // condemnation decision guarantees the target cannot be unregistered mid-check.
  kj::HashSet<uint64_t> sampledThreads;
  for (uint i = entries->size(); i > 0; --i) {
    auto& entry = (*entries)[i - 1];
    if (!sampledThreads.insert(entry.threadHandle)) continue;
    KJ_IF_SOME(deadline, entry.deadline) {
      KJ_IF_SOME(cpuNow, clock_->readCapturedThread(entry.threadHandle)) {
        if (cpuNow >= deadline) {
          entry.state->condemnFromWatchdog(StandaloneLimitViolation::CPU);
        }
      }
    }
  }
}

// ========================================================================================
// Factories

kj::Own<LimitEnforcer> newRequestLimitEnforcer(
    kj::Rc<StandaloneIsolateLimitState> isolateState, EffectiveResourceLimits limits) {
  return newRequestLimitEnforcer(
      kj::mv(isolateState), kj::mv(limits), StandaloneLimitWatchdog::defaultInstance());
}

kj::Own<LimitEnforcer> newRequestLimitEnforcer(kj::Rc<StandaloneIsolateLimitState> isolateState,
    EffectiveResourceLimits limits,
    StandaloneLimitWatchdog& watchdog) {
  return kj::heap<RequestLimitEnforcer>(kj::mv(isolateState), kj::mv(limits), watchdog);
}

kj::Own<IoChannelFactory::SubrequestChannel> newConnectionAccountingChannel(
    kj::Own<IoChannelFactory::SubrequestChannel> inner) {
  return kj::refcounted<ConnectionAccountingChannel>(kj::mv(inner));
}

kj::Own<IoChannelFactory::WorkerLoaderChannel> newResourceLimitedWorkerLoaderChannel(
    kj::Own<IoChannelFactory::WorkerLoaderChannel> inner, EffectiveResourceLimits ceiling) {
  return kj::refcounted<ResourceLimitedWorkerLoaderChannel>(kj::mv(inner), kj::mv(ceiling));
}

}  // namespace workerd
