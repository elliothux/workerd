// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Unit tests for the standalone isolate-level resource limits: the Standard memory and
// startup-CPU constants, the isolate creation constraints, and the bounded startup scope.
// Heap-limit behavior against a real V8 isolate is covered by the worker-loader wd-tests.

#include "standalone-isolate-limits.h"

#include <workerd/jsg/exception.h>
#include <workerd/util/exception.h>

#include <kj/test.h>

namespace workerd {
namespace {

// Mirrors the fake clock in standalone-resource-limits-test.c++.
struct FakeClock final: public ThreadCpuClock {
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

struct Fixture {
  FakeClock fake;
  StandaloneLimitWatchdog watchdog{fake, false};
  kj::Rc<StandaloneIsolateLimitState> state = kj::rc<StandaloneIsolateLimitState>();
  uint evictions = 0;

  Fixture() {
    state->setEvictionCallback([this]() { ++evictions; });
  }

  kj::Own<StandaloneIsolateLimitEnforcer> makeEnforcer() {
    return kj::refcounted<StandaloneIsolateLimitEnforcer>(kj::addRef(*state), watchdog);
  }
};

// ========================================================================================
// Standard constants

KJ_TEST("Standard isolate budgets are the documented constants") {
  KJ_EXPECT(STANDARD_ISOLATE_MEMORY_LIMIT == 128u * 1024 * 1024);
  KJ_EXPECT(STANDARD_STARTUP_CPU_LIMIT == 1 * kj::SECONDS);
  // Frozen so the limits cannot drift silently against the documented profile.
  KJ_EXPECT(STANDARD_MAX_SIMULTANEOUS_OUTBOUND_CONNECTIONS == 6);
  KJ_EXPECT(STANDARD_MAX_INVOCATION_CPU == 300 * kj::SECONDS);
  KJ_EXPECT(STANDARD_MAX_SUBREQUESTS == 10'000'000u);
}

KJ_TEST("isolate creation parameters cap the V8 heap at the Standard memory limit") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  (void)waitScope;
  Fixture fix;
  auto enforcer = fix.makeEnforcer();
  auto params = enforcer->getCreateParams();
  KJ_EXPECT(params.constraints.max_old_generation_size_in_bytes() == STANDARD_ISOLATE_MEMORY_LIMIT);
  KJ_EXPECT(!enforcer->hasExcessivelyExceededHeapLimit());
}

// ========================================================================================
// Bounded startup

KJ_TEST("startup within the CPU budget completes without error") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  (void)waitScope;
  Fixture fix;
  auto enforcer = fix.makeEnforcer();

  kj::OneOf<kj::Exception, kj::Duration> limitErrorOrTime;
  {
    auto scope = enforcer->enterBoundedStartup(limitErrorOrTime);
    fix.fake.now = 999 * kj::MILLISECONDS;  // under the 1 s budget
  }
  KJ_EXPECT(limitErrorOrTime.tryGet<kj::Exception>() == kj::none);
  KJ_EXPECT(!fix.state->isCondemned());
  KJ_EXPECT(fix.evictions == 0);
}

KJ_TEST("startup overrun condemns the isolate and reports the stable CPU exception") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  Fixture fix;
  auto enforcer = fix.makeEnforcer();

  kj::Promise<void> inFlight = fix.state->onLimitsExceeded();

  kj::OneOf<kj::Exception, kj::Duration> limitErrorOrTime;
  {
    auto scope = enforcer->enterBoundedStartup(limitErrorOrTime);
    // The watchdog would terminate the compile; simulate it sampling past the budget.
    fix.fake.now = 1001 * kj::MILLISECONDS;
    fix.state->condemnFromWatchdog(StandaloneLimitViolation::CPU);
  }
  KJ_EXPECT(limitErrorOrTime.tryGet<kj::Exception>() != kj::none);
  auto& exception = KJ_ASSERT_NONNULL(limitErrorOrTime.tryGet<kj::Exception>());
  KJ_EXPECT(exception.getDescription().contains("CPU time limit"));
  KJ_EXPECT(exception.getDetail(CPU_LIMIT_DETAIL_ID) != kj::none);
  KJ_EXPECT(fix.evictions == 1);
  KJ_EXPECT_THROW_RECOVERABLE(OVERLOADED, inFlight.wait(waitScope));
}

KJ_TEST("late watchdog events cannot condemn a finished startup") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  (void)waitScope;
  Fixture fix;
  auto enforcer = fix.makeEnforcer();

  kj::OneOf<kj::Exception, kj::Duration> limitErrorOrTime;
  { auto scope = enforcer->enterBoundedStartup(limitErrorOrTime); }
  // The turn is disarmed; a late sample (or a late V8 termination request) must not condemn.
  fix.fake.now = 10 * kj::SECONDS;
  fix.watchdog.pollOnce();
  KJ_EXPECT(!fix.state->isCondemned());
  KJ_EXPECT(limitErrorOrTime.tryGet<kj::Exception>() == kj::none);
  KJ_EXPECT(fix.evictions == 0);
}

}  // namespace
}  // namespace workerd
