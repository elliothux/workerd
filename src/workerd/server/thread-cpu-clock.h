// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/time.h>

namespace workerd {

// Measures the CPU time actually consumed by the thread executing tenant JS/Wasm. Wall-clock
// time is never an acceptable substitute: I/O awaits, queueing, and upstream waits must not
// be billed to an invocation's CPU budget.
//
// The platform adapters live entirely in thread-cpu-clock.c++; Linux reads the pthread CPU
// clock and macOS reads Mach thread accounting. Test code injects a fake clock into the
// resource-limit watchdog instead of sleeping for real budgets.
class ThreadCpuClock {
 public:
  virtual ~ThreadCpuClock() noexcept(false);

  // CPU time consumed by the calling thread since the thread (or process) started. The value
  // is monotonic per thread and independent across threads; two threads may observe very
  // different values at the same instant.
  virtual kj::Duration currentThreadCpu() const = 0;

  // Reads the CPU time of a thread captured earlier by `captureCurrentThread()` on that
  // thread. This is the watchdog's cross-thread sampling path: the returned value follows the
  // same per-thread monotonic timeline as `currentThreadCpu()` on the captured thread.
  //
  // Returns kj::none if the captured thread no longer exists or its CPU time cannot be read.
  // Implementations must be safe to call concurrently from a non-JS thread while the captured
  // thread is running JS; a stale capture that outlived its turn simply reads nothing.
  virtual kj::Maybe<kj::Duration> readCapturedThread(uint64_t handle) const = 0;

  // Captures an opaque handle for the calling thread that `readCapturedThread()` can later
  // resolve. The handle is only valid for reads while the calling thread is alive; turns are
  // unregistered before their thread can exit, so handles never outlive their thread in
  // production paths.
  virtual uint64_t captureCurrentThread() const = 0;

  // Releases resources taken by `captureCurrentThread()`. Called on the owning thread when
  // the turn is unregistered; a no-op for clock implementations whose captures are plain
  // values.
  virtual void releaseCapture(uint64_t handle) const = 0;

  // The process-wide production clock. Watchdog and enforcement code obtain their clock here
  // so tests can substitute a deterministic implementation before constructing them.
  static ThreadCpuClock& get();
};

// Installs `clock` as the process-wide CPU clock, overriding ThreadCpuClock::get(). Test-only:
// unit tests call this before constructing the resource-limit watchdog so budgets can be
// driven deterministically instead of sleeping for real CPU time. Production code never calls
// it, and it must not be called after watchdog threads have started sampling.
void setProcessThreadCpuClockForTest(ThreadCpuClock* clock);

}  // namespace workerd
