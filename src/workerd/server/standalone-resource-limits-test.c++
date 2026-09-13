// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Unit tests for the standalone request resource limits: watchdog sampling semantics
// (boundary, overrun, disarm, nested turns), subrequest budgets, limit value rules, and
// simultaneous-connection accounting. JS-turn enforcement against a real V8 isolate is
// covered by the worker-loader limits wd-tests; these tests use a deterministic fake clock.

#include "standalone-resource-limits.h"

#include <workerd/jsg/exception.h>
#include <workerd/util/exception.h>

#include <kj/test.h>

namespace workerd {
namespace {

struct FakeClock final: public ThreadCpuClock {
  // Single shared CPU reading; unit tests exercise one logical thread at a time. Captured
  // handles resolve to the same reading, mirroring a single JS thread observed by the
  // watchdog.
  kj::Duration now = 0 * kj::SECONDS;

  kj::Duration currentThreadCpu() const override {
    return now;
  }
  kj::Maybe<kj::Duration> readCapturedThread(uint64_t) const override {
    return now;
  }
  uint64_t captureCurrentThread() const override {
    return 1;
  }
  void releaseCapture(uint64_t) const override {}
};

// A per-isolate state wired to count loader-cache evictions. Condemnation never touches a
// real v8 isolate in these tests (no isolate registered => termination is a no-op). Promise
// plumbing needs an event loop, so every fixture owns one.
struct TestIsolate {
  // The test itself owns the thread's single EventLoop and WaitScope.
  kj::Rc<StandaloneIsolateLimitState> state = kj::rc<StandaloneIsolateLimitState>();
  uint evictions = 0;

  TestIsolate() {
    state->setEvictionCallback([this]() { ++evictions; });
  }
};

// ========================================================================================
// Limit value helpers

KJ_TEST("EffectiveResourceLimits min rules: narrower value wins, omission never widens") {
  EffectiveResourceLimits unlimited{};

  // Unlimited combined with anything is the other set.
  auto a = minEffectiveResourceLimits(
      EffectiveResourceLimits{.cpu = 30 * kj::SECONDS, .subrequests = 50u}, unlimited);
  KJ_EXPECT(a.cpu.orDefault(0 * kj::SECONDS) == 30 * kj::SECONDS);
  KJ_EXPECT(a.subrequests.orDefault(0) == 50u);

  // Per-dimension minimum.
  auto b = minEffectiveResourceLimits(
      EffectiveResourceLimits{.cpu = 10 * kj::SECONDS, .subrequests = 100u},
      EffectiveResourceLimits{.cpu = 20 * kj::SECONDS, .subrequests = 40u});
  KJ_EXPECT(b.cpu.orDefault(0 * kj::SECONDS) == 10 * kj::SECONDS);
  KJ_EXPECT(b.subrequests.orDefault(0) == 40u);

  // Omitted in one set inherits the other instead of becoming unlimited.
  auto c = minEffectiveResourceLimits(
      EffectiveResourceLimits{.subrequests = 7u}, EffectiveResourceLimits{.cpu = 5 * kj::SECONDS});
  KJ_EXPECT(c.cpu.orDefault(0 * kj::SECONDS) == 5 * kj::SECONDS);
  KJ_EXPECT(c.subrequests.orDefault(0) == 7u);
}

KJ_TEST("validateResourceLimits accepts in-range values and rejects the rest") {
  ResourceLimits valid{.cpuMs = 30000u, .subRequests = 10000u};
  auto parsed = validateResourceLimits(valid);
  KJ_EXPECT(parsed.cpu.orDefault(0 * kj::SECONDS) == 30 * kj::SECONDS);
  KJ_EXPECT(parsed.subrequests.orDefault(0) == 10000u);

  // Omitted values stay omitted (defaults are materialized by the loader host).
  ResourceLimits partial{.cpuMs = 1000u, .subRequests = kj::none};
  auto parsedPartial = validateResourceLimits(partial);
  KJ_EXPECT(parsedPartial.cpu != kj::none);
  KJ_EXPECT(parsedPartial.subrequests == kj::none);

  ResourceLimits zeroCpu{.cpuMs = 0u, .subRequests = kj::none};
  KJ_EXPECT_THROW_RECOVERABLE(FAILED, validateResourceLimits(zeroCpu));

  ResourceLimits overCpu{.cpuMs = 300001u, .subRequests = kj::none};
  KJ_EXPECT_THROW_RECOVERABLE(FAILED, validateResourceLimits(overCpu));

  ResourceLimits zeroSubrequests{.cpuMs = kj::none, .subRequests = 0u};
  KJ_EXPECT_THROW_RECOVERABLE(FAILED, validateResourceLimits(zeroSubrequests));

  ResourceLimits overSubrequests{.cpuMs = kj::none, .subRequests = 10000001u};
  KJ_EXPECT_THROW_RECOVERABLE(FAILED, validateResourceLimits(overSubrequests));
}

// ========================================================================================
// Watchdog sampling

KJ_TEST("watchdog condemns only after the CPU deadline is crossed") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  TestIsolate isolate;
  FakeClock fake;
  StandaloneLimitWatchdog watchdog(fake, false);

  auto token = watchdog.registerTurn(*isolate.state, &watchdog, 100 * kj::MILLISECONDS);
  (void)token;

  // Boundary-before: 99 ms consumed, still under budget.
  fake.now = 99 * kj::MILLISECONDS;
  watchdog.pollOnce();
  KJ_EXPECT(!isolate.state->isCondemned());
  KJ_EXPECT(isolate.evictions == 0);

  // Boundary: reaching the budget terminates (fail closed; the loop would otherwise cross it
  // before the next sample).
  fake.now = 100 * kj::MILLISECONDS;
  watchdog.pollOnce();
  KJ_EXPECT(isolate.state->isCondemned());
  KJ_EXPECT(isolate.state->violation() == StandaloneLimitViolation::CPU);

  // Watchdog condemnation is atomic-only; the KJ-side eviction happens when the workerd
  // thread observes the condemned isolate.
  auto exception = isolate.state->takeCondemnedException(StandaloneLimitViolation::CPU);
  KJ_EXPECT(exception.getDescription().contains("CPU time limit"));
  KJ_EXPECT(isolate.evictions == 1);
  KJ_EXPECT_THROW_RECOVERABLE(OVERLOADED, isolate.state->onLimitsExceeded().wait(waitScope));
}

KJ_TEST("watchdog ignores finished (disarmed) turns") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  TestIsolate isolate;
  FakeClock fake;
  StandaloneLimitWatchdog watchdog(fake, false);

  {
    auto token = watchdog.registerTurn(*isolate.state, &watchdog, 50 * kj::MILLISECONDS);
    watchdog.finishTurn(token, 10 * kj::MILLISECONDS);
  }
  // The turn was disarmed; later CPU consumption must not condemn anything.
  fake.now = 10 * kj::SECONDS;
  watchdog.pollOnce();
  KJ_EXPECT(!isolate.state->isCondemned());
  KJ_EXPECT(isolate.evictions == 0);
}

KJ_TEST("nested turns shift the enclosing deadline instead of double-billing") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  TestIsolate parent;
  TestIsolate child;
  FakeClock fake;
  StandaloneLimitWatchdog watchdog(fake, false);

  auto parentToken = watchdog.registerTurn(*parent.state, (void*)0x1, 100 * kj::MILLISECONDS);
  (void)parentToken;
  // The child turn consumes 60 ms of the same thread.
  auto childToken = watchdog.registerTurn(*child.state, (void*)0x2, 1000 * kj::MILLISECONDS);
  fake.now = 60 * kj::MILLISECONDS;
  watchdog.finishTurn(childToken, 60 * kj::MILLISECONDS);

  // Parent consumed nothing itself; the 60 ms nested turn must not count against it.
  fake.now = 90 * kj::MILLISECONDS;
  watchdog.pollOnce();
  KJ_EXPECT(!parent.state->isCondemned());
  KJ_EXPECT(!child.state->isCondemned());

  // Crossing the shifted deadline (100 ms budget + 60 ms nested) condemns the parent.
  fake.now = 161 * kj::MILLISECONDS;
  watchdog.pollOnce();
  KJ_EXPECT(parent.state->isCondemned());
}

KJ_TEST("turns without a CPU budget are never sampled") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  TestIsolate isolate;
  FakeClock fake;
  StandaloneLimitWatchdog watchdog(fake, false);

  auto token = watchdog.registerTurn(*isolate.state, &watchdog, kj::none);
  fake.now = 1000 * kj::SECONDS;
  watchdog.pollOnce();
  KJ_EXPECT(!isolate.state->isCondemned());
  watchdog.finishTurn(token, 0 * kj::SECONDS);
}

KJ_TEST("production watchdog samples at a fixed cadence") {
  // The cadence is an implementation constant; freeze it so it cannot drift silently.
  KJ_EXPECT(StandaloneLimitWatchdog::SAMPLE_INTERVAL == 5 * kj::MILLISECONDS);
}

// ========================================================================================
// Condemnation teardown

KJ_TEST("condemnation resolves once, evicts once, and returns the stable exception") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  TestIsolate isolate;

  // A late in-flight branch observes the condemnation.
  kj::Promise<void> lateBranch = isolate.state->onLimitsExceeded();

  auto first = isolate.state->takeCondemnedException(StandaloneLimitViolation::CPU);
  KJ_EXPECT(first.getDescription().contains("CPU time limit"));
  KJ_EXPECT(first.getDetail(CPU_LIMIT_DETAIL_ID) != kj::none);
  KJ_EXPECT(isolate.evictions == 1);

  auto second = isolate.state->takeCondemnedException(StandaloneLimitViolation::CPU);
  KJ_EXPECT(second.getDescription() == first.getDescription());
  KJ_EXPECT(isolate.evictions == 1);

  // The failure promise rejects for request branches registered before and after the
  // condemnation, settling every in-flight invocation of the isolate.
  KJ_EXPECT_THROW_RECOVERABLE(OVERLOADED, lateBranch.wait(waitScope));
  KJ_EXPECT_THROW_RECOVERABLE(OVERLOADED, isolate.state->onLimitsExceeded().wait(waitScope));
}

KJ_TEST("memory condemnation carries the memory detail") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  TestIsolate isolate;
  auto exception = isolate.state->takeCondemnedException(StandaloneLimitViolation::MEMORY);
  KJ_EXPECT(exception.getDescription().contains("memory limit"));
  KJ_EXPECT(exception.getDetail(MEMORY_LIMIT_DETAIL_ID) != kj::none);
}

// ========================================================================================
// Request enforcer (subrequests, outcomes, connections)

struct EnforcerFixture {
  FakeClock fake;
  StandaloneLimitWatchdog watchdog{fake, false};
  TestIsolate isolate;
};

KJ_TEST("subrequest budget allows exactly N and fails the N+1th before any side effect") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  (void)waitScope;
  EnforcerFixture fix;
  auto enforcer = newRequestLimitEnforcer(kj::addRef(*fix.isolate.state),
      EffectiveResourceLimits{.cpu = kj::none, .subrequests = 2u}, fix.watchdog);

  enforcer->newSubrequest(false);
  enforcer->newSubrequest(true);  // in-house flag does not bypass the tenant budget
  KJ_EXPECT_THROW_MESSAGE("Too many subrequests.", enforcer->newSubrequest(false));
  // The failed call must not have consumed anything; still exactly two accounted.
  KJ_EXPECT_THROW_MESSAGE("Too many subrequests.", enforcer->newSubrequest(false));
}

KJ_TEST("unlimited dimensions never reject") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  (void)waitScope;
  EnforcerFixture fix;
  auto enforcer = newRequestLimitEnforcer(
      kj::addRef(*fix.isolate.state), EffectiveResourceLimits{}, fix.watchdog);
  for (uint i = 0; i < 100; ++i) {
    enforcer->newSubrequest(false);
  }
  KJ_EXPECT(enforcer->getLimitsExceeded() == kj::none);
}

KJ_TEST("condemned isolate fails new subrequests and reports the matching outcome") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  EnforcerFixture fix;
  (void)waitScope;
  auto enforcer = newRequestLimitEnforcer(kj::addRef(*fix.isolate.state),
      EffectiveResourceLimits{.cpu = kj::none, .subrequests = kj::none}, fix.watchdog);

  fix.isolate.state->takeCondemnedException(StandaloneLimitViolation::CPU);
  KJ_EXPECT(enforcer->getLimitsExceeded() == EventOutcome::EXCEEDED_CPU);
  KJ_EXPECT_THROW_RECOVERABLE(OVERLOADED, enforcer->newSubrequest(false));
  KJ_EXPECT_THROW_RECOVERABLE(OVERLOADED, enforcer->requireLimitsNotExceeded());

  // Memory condemnation reports the memory outcome instead.
  TestIsolate memoryIsolate;
  auto memoryEnforcer = newRequestLimitEnforcer(
      kj::addRef(*memoryIsolate.state), EffectiveResourceLimits{}, fix.watchdog);
  memoryIsolate.state->takeCondemnedException(StandaloneLimitViolation::MEMORY);
  KJ_EXPECT(memoryEnforcer->getLimitsExceeded() == EventOutcome::EXCEEDED_MEMORY);
}

KJ_TEST("onLimitsExceeded joins the shared isolate failure promise") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  EnforcerFixture fix;
  auto enforcer = newRequestLimitEnforcer(
      kj::addRef(*fix.isolate.state), EffectiveResourceLimits{}, fix.watchdog);

  auto branch = enforcer->onLimitsExceeded();
  fix.isolate.state->takeCondemnedException(StandaloneLimitViolation::CPU);
  KJ_EXPECT_THROW_RECOVERABLE(OVERLOADED, branch.wait(waitScope));
}

KJ_TEST("six simultaneous outbound connections pass; the seventh queues") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  EnforcerFixture fix;
  auto enforcer = newRequestLimitEnforcer(
      kj::addRef(*fix.isolate.state), EffectiveResourceLimits{}, fix.watchdog);

  kj::Vector<kj::Own<LimitEnforcer::OutboundConnectionLease>> leases;
  for (uint i = 0; i < STANDARD_MAX_SIMULTANEOUS_OUTBOUND_CONNECTIONS; ++i) {
    leases.add(enforcer->newOutboundConnection().wait(waitScope));
  }

  // The seventh connect must queue without producing a side effect: no lease, no resolution.
  auto queued = enforcer->newOutboundConnection();
  KJ_EXPECT(!queued.poll(waitScope));

  // Releasing one connection resolves the queued waiter (slot transfer).
  kj::Own<LimitEnforcer::OutboundConnectionLease> released = kj::mv(leases[0]);
  released = nullptr;
  auto lease = queued.wait(waitScope);
  KJ_EXPECT(lease.get() != nullptr);

  // Dropping the lease releases its slot for the next caller.
  lease = nullptr;
  auto next = enforcer->newOutboundConnection().wait(waitScope);
  KJ_EXPECT(next.get() != nullptr);
}

KJ_TEST("connection queue resolves the oldest waiter and skips cancelled ones") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  EnforcerFixture fix;
  auto enforcer = newRequestLimitEnforcer(
      kj::addRef(*fix.isolate.state), EffectiveResourceLimits{}, fix.watchdog);

  kj::Vector<kj::Own<LimitEnforcer::OutboundConnectionLease>> leases;
  for (uint i = 0; i < STANDARD_MAX_SIMULTANEOUS_OUTBOUND_CONNECTIONS; ++i) {
    leases.add(enforcer->newOutboundConnection().wait(waitScope));
  }

  // Two queued waiters; the first is cancelled, the second must receive the freed slot.
  auto cancelled = enforcer->newOutboundConnection();
  auto secondWaiter = enforcer->newOutboundConnection();
  cancelled = nullptr;  // cancel the first queued waiter

  kj::Own<LimitEnforcer::OutboundConnectionLease> released = kj::mv(leases[0]);
  released = nullptr;
  auto lease = secondWaiter.wait(waitScope);
  KJ_EXPECT(lease.get() != nullptr);
}

}  // namespace
}  // namespace workerd
