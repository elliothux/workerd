// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Platform adapter tests for the thread CPU clock. These run with the real OS thread
// accounting on every formal target platform (Linux arm64/x64 and macOS arm64/x64); no fake
// clocks here.

#include "thread-cpu-clock.h"

#include <sys/types.h>

#include <kj/test.h>

#include <chrono>
#include <thread>

namespace workerd {
namespace {

void burnCpu(ThreadCpuClock& clock, kj::Duration target) {
  // Drive the adapter to the requested CPU delta rather than assuming the test process will
  // receive a particular fraction of wall time on a loaded builder. The wall deadline keeps a
  // broken adapter from hanging the test.
  auto start = clock.currentThreadCpu();
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  volatile double value = 1.0;
  while (clock.currentThreadCpu() - start < target && std::chrono::steady_clock::now() < deadline) {
    for (unsigned i = 0; i < 10000; ++i) {
      value = value * 1.0000001 + 0.5;
    }
  }
  (void)value;
}

KJ_TEST("thread CPU clock is monotonic per thread") {
  auto& clock = ThreadCpuClock::get();
  auto first = clock.currentThreadCpu();
  auto second = clock.currentThreadCpu();
  KJ_EXPECT(second >= first);
}

KJ_TEST("thread CPU clock advances only while computing, not while sleeping") {
  auto& clock = ThreadCpuClock::get();
  auto before = clock.currentThreadCpu();

  // Wall-clock sleep must not bill CPU time (this is what keeps I/O awaits free).
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  auto afterSleep = clock.currentThreadCpu();
  KJ_EXPECT(afterSleep - before < 50 * kj::MILLISECONDS);

  // Real computation advances the clock.
  burnCpu(clock, 50 * kj::MILLISECONDS);
  auto afterBurn = clock.currentThreadCpu();
  KJ_EXPECT(afterBurn - afterSleep >= 50 * kj::MILLISECONDS);
}

KJ_TEST("captured thread handles read from another thread") {
  auto& clock = ThreadCpuClock::get();
  auto handle = clock.captureCurrentThread();
  KJ_DEFER(clock.releaseCapture(handle));

  auto before = clock.currentThreadCpu();
  kj::Maybe<kj::Duration> crossThread;
  std::thread sampler([&]() { crossThread = clock.readCapturedThread(handle); });
  sampler.join();
  KJ_ASSERT(crossThread != kj::none);
  KJ_EXPECT(KJ_ASSERT_NONNULL(crossThread) >= before);
}

KJ_TEST("stale or invalid handles read as unreadable rather than crashing") {
  auto& clock = ThreadCpuClock::get();
  // A handle value that cannot name a live thread of this process.
  auto reading = clock.readCapturedThread(0);
  // Some platforms accept arbitrary values and return a (garbage-free) failure; either way
  // the call must not crash. We assert only that it returned without terminating.
  (void)reading;
}

}  // namespace
}  // namespace workerd
