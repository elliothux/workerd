// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "thread-cpu-clock.h"

#include <pthread.h>
#include <time.h>

#include <kj/debug.h>

#if __APPLE__
#include <mach/mach.h>
#include <mach/mach_init.h>
#include <mach/mach_port.h>
#include <mach/thread_act.h>
#include <mach/thread_info.h>
#endif

namespace workerd {
namespace {

#if __APPLE__
// Mach thread accounting. `thread_info(THREAD_BASIC_INFO)` reports user and system CPU time
// for any live thread send right, including one captured by another thread.
class MachCpuClock final {
 public:
  kj::Duration currentThreadCpu() const {
    mach_port_t self = mach_thread_self();
    KJ_DEFER(mach_port_deallocate(mach_task_self(), self));
    auto reading = read(self);
    return reading.orDefault(0 * kj::SECONDS);
  }

  kj::Maybe<kj::Duration> readCapturedThread(uint64_t handle) const {
    return read(static_cast<mach_port_t>(handle));
  }

  uint64_t captureCurrentThread() const {
    // mach_thread_self() returns a send right carrying an extra reference. The watchdog
    // releases it through releaseCapture() when the turn is unregistered, which happens on
    // the owning thread under the registry mutex, so the port can never be sampled after its
    // reference is dropped.
    return static_cast<uint64_t>(mach_thread_self());
  }

  void releaseCapture(uint64_t handle) const {
    mach_port_deallocate(mach_task_self(), static_cast<mach_port_t>(handle));
  }

 private:
  static kj::Maybe<kj::Duration> read(mach_port_t port) {
    thread_basic_info_data_t info;
    mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
    if (thread_info(port, THREAD_BASIC_INFO, reinterpret_cast<thread_info_t>(&info), &count) !=
        KERN_SUCCESS) {
      // The thread is gone or the port is stale; treat as unreadable.
      return kj::none;
    }
    return kj::Duration(info.user_time.seconds * kj::SECONDS +
        info.user_time.microseconds * kj::MICROSECONDS + info.system_time.seconds * kj::SECONDS +
        info.system_time.microseconds * kj::MICROSECONDS);
  }
};
#else
// Monotonic per-thread CPU time via POSIX pthread CPU clocks. pthread_getcpuclockid() is the
// documented way to obtain another (live) thread's CPU clock, which is what the watchdog's
// cross-thread sampling path needs. Turn registrations are removed on their owning thread
// before that thread can exit, so the captured pthread_t is alive whenever the watchdog can
// observe it through the registry.
class PthreadCpuClock final {
 public:
  kj::Duration currentThreadCpu() const {
    struct timespec ts;
    KJ_SYSCALL(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts),
        "clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    return durationFromTimespec(ts);
  }

  kj::Maybe<kj::Duration> readCapturedThread(uint64_t handle) const {
    pthread_t thread = reconstruct(handle);
    clockid_t clockId;
    if (pthread_getcpuclockid(thread, &clockId) != 0) return kj::none;
    struct timespec ts;
    if (clock_gettime(clockId, &ts) != 0) return kj::none;
    return durationFromTimespec(ts);
  }

  uint64_t captureCurrentThread() const {
    static_assert(sizeof(pthread_t) <= sizeof(uint64_t), "pthread_t larger than handle");
    uint64_t handle = 0;
    pthread_t self = pthread_self();
    memcpy(&handle, &self, sizeof(self));
    return handle;
  }

  void releaseCapture(uint64_t) const {}

 private:
  static pthread_t reconstruct(uint64_t handle) {
    pthread_t thread;
    memcpy(&thread, &handle, sizeof(thread));
    return thread;
  }

  static kj::Duration durationFromTimespec(const struct timespec& ts) {
    return kj::Duration(ts.tv_sec * kj::SECONDS + ts.tv_nsec * kj::NANOSECONDS);
  }
};
#endif

class RealThreadCpuClock final: public ThreadCpuClock {
 public:
  kj::Duration currentThreadCpu() const override {
    return impl.currentThreadCpu();
  }
  kj::Maybe<kj::Duration> readCapturedThread(uint64_t handle) const override {
    return impl.readCapturedThread(handle);
  }
  uint64_t captureCurrentThread() const override {
    return impl.captureCurrentThread();
  }
  void releaseCapture(uint64_t handle) const override {
    impl.releaseCapture(handle);
  }

 private:
#if __APPLE__
  MachCpuClock impl;
#else
  PthreadCpuClock impl;
#endif
};

// All CPU-limit code resolves its clock through this pointer. Production never mutates it;
// test code installs a fake before constructing the watchdog.
ThreadCpuClock* globalThreadCpuClock = nullptr;

}  // namespace

ThreadCpuClock::~ThreadCpuClock() noexcept(false) = default;

ThreadCpuClock& ThreadCpuClock::get() {
  static RealThreadCpuClock realClock;
  return globalThreadCpuClock != nullptr ? *globalThreadCpuClock : realClock;
}

void setProcessThreadCpuClockForTest(ThreadCpuClock* clock) {
  globalThreadCpuClock = clock;
}

}  // namespace workerd
