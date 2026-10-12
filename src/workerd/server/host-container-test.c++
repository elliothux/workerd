#include "container-auth.h"
#include "host-container.h"

#include <workerd/util/completion-membrane.h>

#include <kj/async-io.h>
#include <kj/test.h>
#include <kj/vector.h>

#include <limits>

namespace workerd::server {
namespace {

kj::Promise<void> rejectNativeEgress(
    bool, uint16_t, kj::String, kj::Own<IoChannelFactory::SubrequestChannel>) {
  KJ_FAIL_REQUIRE("Unexpected native egress registration");
}

kj::Promise<HostContainerSnapshot> rejectSnapshotCapture(kj::String, kj::Maybe<kj::String>) {
  KJ_FAIL_REQUIRE("Unexpected host snapshot capture");
}

void rejectInactivity(int64_t) {
  KJ_FAIL_REQUIRE("Unexpected inactivity configuration");
}

void rejectPrestartEgress(uint16_t, capnp::AnyPointer::Reader) {
  KJ_FAIL_ASSERT("Unexpected pre-start egress registration");
}

KJ_TEST("Container callback credentials reject missing duplicate stale and malformed headers") {
  kj::HttpHeaderTable table;
  kj::Maybe<kj::String> token =
      kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  kj::HttpHeaders headers(table);
  KJ_EXPECT(!containerCallbackAuthorized(token, headers));
  headers.add(CONTAINER_CALLBACK_HEADER, KJ_ASSERT_NONNULL(token));
  KJ_EXPECT(containerCallbackAuthorized(token, headers));
  KJ_EXPECT(!containerCallbackAuthorized(kj::none, headers));
  KJ_EXPECT(!containerTokenMatches(KJ_ASSERT_NONNULL(token), "short"));
  KJ_EXPECT(!containerTokenMatches(KJ_ASSERT_NONNULL(token),
      "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
  headers.add(CONTAINER_CALLBACK_HEADER, KJ_ASSERT_NONNULL(token));
  KJ_EXPECT(!containerCallbackAuthorized(token, headers));
}

KJ_TEST("Host Container status and destroy do not reserve an absent lease") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  uint calls = 0;
  capnp::Capability::Client capability(kj::refcounted<HostContainerRpc>(
      [&calls](bool start, bool stop, bool completed, kj::String startup,
          kj::String expectedLeaseKey, bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    KJ_EXPECT(!start && !completed && startup.size() == 0);
    KJ_EXPECT(stop == (calls == 1));
    ++calls;
    return kj::Maybe<HostContainerRpc::Resolved>(kj::none);
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  auto container = capability.castAs<rpc::Container>();
  KJ_EXPECT(!container.statusRequest().send().wait(wait).getRunning());
  container.destroyRequest().send().wait(wait);
  KJ_EXPECT(calls == 2);
  KJ_EXPECT_THROW_MESSAGE(
      "Container operation failed", container.getTcpPortRequest().send().wait(wait));
}

KJ_TEST("Host Container inspect returns none only for an authorized absent lease") {
  for (bool stale: {false, true}) {
    kj::EventLoop loop;
    kj::WaitScope wait(loop);
    uint lookups = 0;
    capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
        [&](bool start, bool stop, bool completed, kj::String startup, kj::String expected,
            bool monitor) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      KJ_EXPECT(
          !start && !stop && !completed && !monitor && startup.size() == 0 && expected.size() == 0);
      ++lookups;
      KJ_REQUIRE(!stale, "Private stale route details");
      return kj::Maybe<HostContainerRpc::Resolved>(kj::none);
    },
        kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
        rejectSnapshotCapture, rejectInactivity));
    auto container = grant.castAs<rpc::Container>();
    if (stale) {
      KJ_EXPECT_THROW_MESSAGE(
          "Container operation failed", container.inspectRequest().send().wait(wait));
    } else {
      KJ_EXPECT(container.inspectRequest().send().wait(wait).getInfo().isNone());
      // setLabels is a mutation, not another absent-container query.
      KJ_EXPECT_THROW_MESSAGE(
          "Container operation failed", container.setLabelsRequest().send().wait(wait));
    }
    KJ_EXPECT(lookups == (stale ? 1 : 2));
  }
}

class InspectEngine final: public rpc::Container::Server {
 public:
  bool running = true;
  kj::Maybe<kj::Promise<void>> pause;
  kj::Promise<void> inspect(InspectContext context) override {
    KJ_IF_SOME(pending, pause) {
      co_await kj::mv(pending);
    }
    auto info = context.getResults().initInfo();
    if (!running) {
      info.setNone();
      co_return;
    }
    auto started = info.initStarted();
    started.setImage("sha256:internal-config-not-a-public-image");
    auto labels = started.initLabels(1);
    labels[0].setName("engine");
    labels[0].setValue("internal");
  }
};

HostContainerInspection inspection(kj::StringPtr image, kj::StringPtr value) {
  auto labels = kj::heapArrayBuilder<HostContainerLabel>(1);
  labels.add(HostContainerLabel{kj::str("purpose"), kj::str(value)});
  return {kj::str(image), labels.finish()};
}

KJ_TEST("Host inspect uses the current lease presentation on running and reconnected clients") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto first = kj::heap<InspectEngine>();
  auto second = kj::heap<InspectEngine>();
  auto& secondEngine = *second;
  rpc::Container::Client firstClient(kj::mv(first));
  rpc::Container::Client secondClient(kj::mv(second));
  bool reconnect = false;
  uint lookups = 0;
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&](bool start, bool stop, bool completed, kj::String startup, kj::String expected,
          bool monitor) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    KJ_EXPECT(!start && !stop && !completed && !monitor && startup.size() == 0);
    KJ_EXPECT(expected.size() == 0 || expected == "same-lease");
    ++lookups;
    return kj::Maybe<HostContainerRpc::Resolved>(HostContainerRpc::Resolved{
      rpc::Container::Client(reconnect ? secondClient : firstClient), kj::str("same-lease"), {},
      inspection(
          "registry/public@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          reconnect ? "reconnected" : "initial")});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  auto container = grant.castAs<rpc::Container>();
  for (bool next: {false, true}) {
    reconnect = next;
    auto response = container.inspectRequest().send().wait(wait);
    auto info = response.getInfo();
    KJ_ASSERT(info.isStarted());
    KJ_EXPECT(info.getStarted().getImage() ==
        "registry/public@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    auto labels = info.getStarted().getLabels();
    KJ_ASSERT(labels.size() == 1);
    KJ_EXPECT(labels[0].getName() == "purpose");
    KJ_EXPECT(labels[0].getValue() == (next ? "reconnected" : "initial"));
  }
  KJ_EXPECT(lookups == 4);
  secondEngine.running = false;
  KJ_EXPECT(container.inspectRequest().send().wait(wait).getInfo().isNone());
  KJ_EXPECT(lookups == 5);
}

KJ_TEST("Host inspect rejects replacement while the native status response is pending") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto ready = kj::newPromiseAndFulfiller<void>();
  auto engine = kj::heap<InspectEngine>();
  engine->pause = kj::mv(ready.promise);
  rpc::Container::Client native(kj::mv(engine));
  bool replaced = false;
  uint lookups = 0;
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&](bool, bool, bool, kj::String, kj::String expected,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    ++lookups;
    KJ_REQUIRE(expected.size() == 0 || (!replaced && expected == "old-lease"),
        "Private stale lease details");
    return kj::Maybe<HostContainerRpc::Resolved>(HostContainerRpc::Resolved{
      rpc::Container::Client(native), kj::str(replaced ? "new-lease" : "old-lease"), {},
      inspection("registry/public:immutable", "initial")});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  auto pending = grant.castAs<rpc::Container>().inspectRequest().send().dropPipeline();
  KJ_EXPECT(!pending.poll(wait) && lookups == 1);
  replaced = true;
  ready.fulfiller->fulfill();
  KJ_EXPECT_THROW_MESSAGE("Container operation failed", pending.wait(wait));
  KJ_EXPECT(lookups == 2);
}

KJ_TEST("Host inactivity accepts constructor configuration without reserving or starting a lease") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());
  uint lookups = 0;
  uint stopped = 0;
  HostContainerInactivity inactivity(timer, true, [&]() -> kj::Promise<void> {
    ++stopped;
    return kj::READY_NOW;
  });
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&](bool start, bool stop, bool completed, kj::String startup, kj::String expected,
          bool monitor) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    KJ_EXPECT(!start && !stop && !completed && !monitor);
    KJ_EXPECT(startup.size() == 0 && expected.size() == 0);
    ++lookups;
    return kj::Maybe<HostContainerRpc::Resolved>(kj::none);
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, [&](int64_t ms) { inactivity.configure(ms); }));
  auto container = grant.castAs<rpc::Container>();
  auto request = container.setInactivityTimeoutRequest();
  request.setDurationMs(6 * 60 * 60 * 1000);
  request.send().wait(wait);
  KJ_EXPECT(lookups == 1 && stopped == 0 && inactivity.retainsOwner());
  auto poll = kj::Promise<void>(kj::NEVER_DONE);
  timer.advanceTo(timer.now() + 7 * (60 * kj::MINUTES));
  KJ_EXPECT(!poll.poll(wait) && stopped == 0);
  inactivity.inactive();
  timer.advanceTo(timer.now() + 6 * (60 * kj::MINUTES));
  KJ_EXPECT(!poll.poll(wait) && stopped == 1 && !inactivity.retainsOwner());
  for (int64_t invalid: {-1, 0, 6 * 60 * 60 * 1000 + 1}) {
    auto invalidRequest = container.setInactivityTimeoutRequest();
    invalidRequest.setDurationMs(invalid);
    KJ_EXPECT_THROW_MESSAGE("Container operation failed", invalidRequest.send().wait(wait));
  }
}

KJ_TEST("Host inactivity starts at inactive and resets after real activity") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());
  uint stops = 0;
  HostContainerInactivity inactivity(timer, true, [&]() -> kj::Promise<void> {
    ++stops;
    return kj::READY_NOW;
  });
  auto poll = kj::Promise<void>(kj::NEVER_DONE);
  auto advance = [&](kj::Duration elapsed) {
    timer.advanceTo(timer.now() + elapsed);
    KJ_EXPECT(!poll.poll(wait));
  };
  inactivity.configure(1000);
  advance(10 * kj::SECONDS);
  KJ_EXPECT(stops == 0);
  inactivity.inactive();
  advance(999 * kj::MILLISECONDS);
  KJ_EXPECT(stops == 0);
  inactivity.active();
  advance(10 * kj::SECONDS);
  KJ_EXPECT(stops == 0);
  inactivity.inactive();
  advance(999 * kj::MILLISECONDS);
  KJ_EXPECT(stops == 0);
  advance(kj::MILLISECONDS);
  KJ_EXPECT(stops == 1 && !inactivity.retainsOwner());
  inactivity.active();
  inactivity.inactive();
  advance(10 * kj::SECONDS);
  KJ_EXPECT(stops == 1);
}

KJ_TEST("Retired host inactivity cancels its deadline and never stops a new epoch") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());
  uint oldStops = 0;
  uint newStops = 0;
  auto poll = kj::Promise<void>(kj::NEVER_DONE);
  {
    HostContainerInactivity old(timer, false, [&]() -> kj::Promise<void> {
      ++oldStops;
      return kj::READY_NOW;
    });
    old.configure(1000);
    timer.advanceTo(timer.now() + 500 * kj::MILLISECONDS);
    KJ_EXPECT(!poll.poll(wait));
  }
  HostContainerInactivity next(timer, false, [&]() -> kj::Promise<void> {
    ++newStops;
    return kj::READY_NOW;
  });
  timer.advanceTo(timer.now() + 10 * kj::SECONDS);
  KJ_EXPECT(!poll.poll(wait) && oldStops == 0 && newStops == 0);
  next.configure(1);
  timer.advanceTo(timer.now() + kj::MILLISECONDS);
  KJ_EXPECT(!poll.poll(wait) && oldStops == 0 && newStops == 1);
}

KJ_TEST("Inactivity cleanup keeps control authority until positive stop or failure settles") {
  for (bool failure: {false, true}) {
    kj::EventLoop loop;
    kj::WaitScope wait(loop);
    kj::TimerImpl timer(kj::origin<kj::TimePoint>());
    auto completion = kj::newPromiseAndFulfiller<void>();
    bool began = false;
    bool completed = false;
    HostContainerInactivity inactivity(timer, false, [&]() -> kj::Promise<void> {
      began = true;
      co_await kj::mv(completion.promise);
      completed = true;
    });
    auto poll = kj::Promise<void>(kj::NEVER_DONE);
    inactivity.configure(1000);
    timer.advanceTo(timer.now() + kj::SECONDS);
    KJ_EXPECT(!poll.poll(wait) && began && !completed && inactivity.retainsOwner());
    inactivity.active();
    inactivity.inactive();
    // New activity cannot cancel cleanup after stop intent has been persisted.
    KJ_EXPECT(inactivity.retainsOwner());
    if (failure) {
      KJ_EXPECT_LOG(ERROR, "Container inactivity cleanup failed.");
      completion.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "private provider details"));
      KJ_EXPECT(!poll.poll(wait) && !completed);
    } else {
      completion.fulfiller->fulfill();
      KJ_EXPECT(!poll.poll(wait) && completed);
    }
    KJ_EXPECT(!inactivity.retainsOwner());
  }
}

class ActivityHooks final: public RequestTracker::Hooks {
 public:
  uint activeCount = 0;
  uint inactiveCount = 0;
  void active() override {
    ++activeCount;
  }
  void inactive() override {
    ++inactiveCount;
  }
};

KJ_TEST("Each operation expires independently without completing its backing promise") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());
  ActivityHooks hooks;
  auto tracker = kj::refcounted<RequestTracker>(hooks);
  auto monitor = kj::refcounted<HostContainerActivity>(*tracker, timer);
  auto pending = kj::newPromiseAndFulfiller<void>();
  bool settled = false;
  auto task = pending.promise.then([&] { settled = true; }).eagerlyEvaluate(nullptr);
  auto poll = kj::Promise<void>(kj::NEVER_DONE);
  timer.advanceTo(timer.now() + 14 * kj::MINUTES);
  KJ_EXPECT(!poll.poll(wait) && tracker->isActive());
  kj::Maybe<kj::Own<HostContainerActivity>> operation =
      kj::refcounted<HostContainerActivity>(*tracker, timer);
  timer.advanceTo(timer.now() + kj::MINUTES);
  KJ_EXPECT(!poll.poll(wait) && tracker->isActive() && hooks.inactiveCount == 0 && !settled);
  timer.advanceTo(timer.now() + 14 * kj::MINUTES);
  KJ_EXPECT(!poll.poll(wait) && !tracker->isActive() && hooks.inactiveCount == 1 && !settled);
  pending.fulfiller->fulfill();
  KJ_EXPECT(!poll.poll(wait) && settled);
  auto resumed = kj::refcounted<HostContainerActivity>(*tracker, timer);
  resumed = nullptr;
  KJ_EXPECT(!tracker->isActive() && hooks.inactiveCount == 2 && hooks.activeCount == 2);
}

class Engine final: public rpc::Container::Server {
 public:
  uint started = 0;
  uint destroyed = 0;
  uint snapshots = 0;

  kj::Promise<void> start(StartContext) override {
    ++started;
    return kj::READY_NOW;
  }
  kj::Promise<void> destroy(DestroyContext) override {
    ++destroyed;
    return kj::READY_NOW;
  }
  kj::Promise<void> snapshotContainer(SnapshotContainerContext) override {
    ++snapshots;
    KJ_FAIL_REQUIRE("Snapshot capture reached the Docker client");
  }
};

KJ_TEST("Host snapshot capture returns only metadata for the freshly resolved exact lease") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto engine = kj::heap<Engine>();
  auto& state = *engine;
  rpc::Container::Client native(kj::mv(engine));
  uint lookups = 0;
  uint captures = 0;
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&](bool start, bool stop, bool completed, kj::String startup, kj::String expected,
          bool monitor) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    KJ_EXPECT(
        !start && !stop && !completed && !monitor && startup.size() == 0 && expected.size() == 0);
    ++lookups;
    return kj::Maybe<HostContainerRpc::Resolved>(
        HostContainerRpc::Resolved{rpc::Container::Client(native),
          kj::str(lookups == 1 ? "first-lease" : "next-lease"), {}, {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      [&](kj::String expected, kj::Maybe<kj::String> name) -> kj::Promise<HostContainerSnapshot> {
    KJ_EXPECT(expected == (captures == 0 ? "first-lease" : "next-lease"));
    if (captures == 0) {
      KJ_EXPECT(KJ_ASSERT_NONNULL(name) == "checkpoint");
    } else {
      KJ_EXPECT(name == kj::none);
    }
    ++captures;
    return HostContainerSnapshot{kj::str("ee2acc27-21e4-48a7-96f4-9790eb855d78"),
      static_cast<double>(jsg::MAX_SAFE_INTEGER), kj::mv(name)};
  }, rejectInactivity));
  auto container = grant.castAs<rpc::Container>();
  auto request = container.snapshotContainerRequest();
  request.setName("checkpoint");
  auto response = request.send().wait(wait);
  auto snapshot = response.getSnapshot();
  KJ_EXPECT(snapshot.getId() == "ee2acc27-21e4-48a7-96f4-9790eb855d78");
  KJ_EXPECT(snapshot.getSize() == jsg::MAX_SAFE_INTEGER && snapshot.getName() == "checkpoint");
  auto unnamed = container.snapshotContainerRequest().send().wait(wait);
  KJ_EXPECT(unnamed.getSnapshot().getName().size() == 0);
  KJ_EXPECT(lookups == 2 && captures == 2 && state.snapshots == 0 && state.started == 0);
  KJ_EXPECT_THROW_MESSAGE(
      "Container operation failed", container.snapshotDirectoryRequest().send().wait(wait));
  KJ_EXPECT(lookups == 2 && captures == 2);
}

KJ_TEST("Host snapshot capture rejects missing and stale authority before any capture") {
  for (bool stale: {false, true}) {
    kj::EventLoop loop;
    kj::WaitScope wait(loop);
    capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
        [stale](bool, bool, bool, kj::String, kj::String,
            bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      KJ_REQUIRE(!stale, "Private stale route details");
      return kj::Maybe<HostContainerRpc::Resolved>(kj::none);
    },
        kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
        rejectSnapshotCapture, rejectInactivity));
    KJ_EXPECT_THROW_MESSAGE("Container operation failed",
        grant.castAs<rpc::Container>().snapshotContainerRequest().send().wait(wait));
  }
}

KJ_TEST("Host snapshot capture rejects metadata that cannot cross the public number boundary") {
  for (uint invalid = 0; invalid < 6; ++invalid) {
    kj::EventLoop loop;
    kj::WaitScope wait(loop);
    rpc::Container::Client native(kj::heap<Engine>());
    capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
        [&](bool, bool, bool, kj::String, kj::String,
            bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      return kj::Maybe<HostContainerRpc::Resolved>(
          HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}, {}});
    },
        kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
        [invalid](kj::String, kj::Maybe<kj::String>) -> kj::Promise<HostContainerSnapshot> {
      constexpr double sizes[] = {-1, 0.5, std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(), 9007199254740992.0, 0};
      return HostContainerSnapshot{kj::str(invalid == 5 ? "" : "snapshot"), sizes[invalid], {}};
    }, rejectInactivity));
    KJ_EXPECT_THROW_MESSAGE("Container operation failed",
        grant.castAs<rpc::Container>().snapshotContainerRequest().send().wait(wait));
  }
}

KJ_TEST("Host snapshot revocation cancels a pending capture and rejects later calls") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  rpc::Container::Client native(kj::heap<Engine>());
  bool capturing = false;
  uint captures = 0;
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&](bool, bool, bool, kj::String, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    return kj::Maybe<HostContainerRpc::Resolved>(
        HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}, {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      [&](kj::String, kj::Maybe<kj::String>) -> kj::Promise<HostContainerSnapshot> {
    ++captures;
    capturing = true;
    return kj::Promise<HostContainerSnapshot>(kj::NEVER_DONE).attach(kj::defer([&] {
      capturing = false;
    }));
  }, rejectInactivity));
  auto revoked = kj::newPromiseAndFulfiller<void>();
  auto container = capnp::membrane(
      grant.castAs<rpc::Container>(), kj::refcounted<RevokerMembrane>(kj::mv(revoked.promise)));
  auto pending = container.snapshotContainerRequest().send().dropPipeline();
  KJ_EXPECT(!pending.poll(wait) && capturing && captures == 1);
  revoked.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "Container attachment revoked."));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", pending.wait(wait));
  KJ_EXPECT(!capturing);
  KJ_EXPECT_THROW_MESSAGE(
      "Container attachment revoked", container.snapshotContainerRequest().send().wait(wait));
  KJ_EXPECT(captures == 1);
}

KJ_TEST("Host Container mutation callbacks bracket the original native engine operation") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto engine = kj::heap<Engine>();
  auto& inspect = *engine;
  rpc::Container::Client native(kj::mv(engine));
  uint calls = 0;
  capnp::Capability::Client capability(kj::refcounted<HostContainerRpc>(
      [&calls, &inspect, &native](bool start, bool stop, bool completed, kj::String startup,
          kj::String expectedLeaseKey, bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    switch (calls++) {
      case 0:
        KJ_EXPECT(start && !stop && !completed && inspect.started == 0);
        break;
      case 1:
        KJ_EXPECT(!start && !stop && completed && inspect.started == 1);
        break;
      case 2:
        KJ_EXPECT(!start && stop && !completed && inspect.destroyed == 0);
        break;
      case 3:
        KJ_EXPECT(!start && stop && completed && inspect.destroyed == 1);
        break;
      default:
        KJ_FAIL_ASSERT("Unexpected Container callback");
    }
    return kj::Maybe<HostContainerRpc::Resolved>(
        HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}, {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  auto container = capability.castAs<rpc::Container>();
  container.startRequest().send().wait(wait);
  container.destroyRequest().send().wait(wait);
  KJ_EXPECT(calls == 4);
}

KJ_TEST("Host Container mutation queue preserves ordering after a waiting operation is canceled") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto queue = kj::refcounted<HostContainerRpc::MutationQueue>();
  auto first = queue->take();
  first.promise.wait(wait);
  { auto canceled = queue->take(); }
  auto last = queue->take();
  KJ_EXPECT(!last.promise.poll(wait));
  first.fulfiller->fulfill();
  last.promise.wait(wait);
  last.fulfiller->fulfill();
}

class StartingEngine final: public rpc::Container::Server {
 public:
  kj::PromiseFulfillerPair<void> startup = kj::newPromiseAndFulfiller<void>();
  kj::PromiseFulfillerPair<int32_t> exit = kj::newPromiseAndFulfiller<int32_t>();
  bool started = false;
  bool monitoring = false;
  bool portRequested = false;
  bool destroyed = false;

  kj::Promise<void> start(StartContext) override {
    co_await kj::mv(startup.promise);
    started = true;
  }
  kj::Promise<void> monitor(MonitorContext context) override {
    KJ_REQUIRE(started);
    monitoring = true;
    context.getResults().setExitCode(co_await kj::mv(exit.promise));
  }
  kj::Promise<void> getTcpPort(GetTcpPortContext context) override {
    KJ_REQUIRE(started);
    portRequested = true;
    context.getResults().setPort(rpc::Container::Port::Client(nullptr));
    return kj::READY_NOW;
  }
  kj::Promise<void> destroy(DestroyContext) override {
    destroyed = true;
    exit.fulfiller->fulfill(0);
    return kj::READY_NOW;
  }
};

KJ_TEST("Host snapshot capture waits for startup and serializes destruction until capture ends") {
  for (bool fail: {false, true}) {
    kj::EventLoop loop;
    kj::WaitScope wait(loop);
    auto engine = kj::heap<StartingEngine>();
    auto& state = *engine;
    rpc::Container::Client native(kj::mv(engine));
    auto captured = kj::newPromiseAndFulfiller<HostContainerSnapshot>();
    uint captures = 0;
    capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
        [&](bool, bool, bool, kj::String, kj::String,
            bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      return kj::Maybe<HostContainerRpc::Resolved>(
          HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}, {}});
    },
        kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
        [&](kj::String expected, kj::Maybe<kj::String>) -> kj::Promise<HostContainerSnapshot> {
      KJ_EXPECT(expected == "lease" && state.started && !state.destroyed);
      ++captures;
      return kj::mv(captured.promise);
    }, rejectInactivity));
    auto container = grant.castAs<rpc::Container>();
    auto start = container.startRequest().send().dropPipeline();
    auto snapshot = container.snapshotContainerRequest().send().dropPipeline();
    auto destroy = container.destroyRequest().send().dropPipeline();
    KJ_EXPECT(!start.poll(wait) && !snapshot.poll(wait) && !destroy.poll(wait));
    KJ_EXPECT(captures == 0 && !state.destroyed);
    state.startup.fulfiller->fulfill();
    start.wait(wait);
    KJ_EXPECT(!snapshot.poll(wait) && !destroy.poll(wait));
    KJ_EXPECT(captures == 1 && !state.destroyed);
    if (fail) {
      captured.fulfiller->reject(KJ_EXCEPTION(FAILED, "Private archive path and credentials"));
      auto error = kj::runCatchingExceptions([&] { snapshot.wait(wait); });
      auto description = KJ_ASSERT_NONNULL(error).getDescription();
      KJ_EXPECT(description.contains("Container operation failed"));
      KJ_EXPECT(!description.contains("Private archive"));
    } else {
      captured.fulfiller->fulfill(HostContainerSnapshot{kj::str("snapshot"), 123, {}});
      KJ_EXPECT(snapshot.wait(wait).getSnapshot().getSize() == 123);
    }
    destroy.wait(wait);
    KJ_EXPECT(state.destroyed && captures == 1);
  }
}

KJ_TEST(
    "Container monitor and port await reservation and startup without locking their lifetimes") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto engine = kj::heap<StartingEngine>();
  auto& state = *engine;
  rpc::Container::Client native(kj::mv(engine));
  auto reserved = kj::newPromiseAndFulfiller<void>();
  uint lookups = 0;
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&native, &reserved, &lookups](bool start, bool, bool, kj::String, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    ++lookups;
    if (start) co_await kj::mv(reserved.promise);
    co_return kj::Maybe<HostContainerRpc::Resolved>(
        HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}, {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  auto container = grant.castAs<rpc::Container>();
  auto start = container.startRequest().send().dropPipeline();
  auto monitor = container.monitorRequest().send().dropPipeline();
  auto port = container.getTcpPortRequest().send().dropPipeline();
  KJ_EXPECT(!start.poll(wait) && !monitor.poll(wait) && !port.poll(wait));
  KJ_EXPECT(lookups == 1 && !state.started && !state.monitoring && !state.portRequested);
  reserved.fulfiller->fulfill();
  KJ_EXPECT(!start.poll(wait) && !monitor.poll(wait) && !port.poll(wait));
  KJ_EXPECT(lookups == 1);
  state.startup.fulfiller->fulfill();
  start.wait(wait);
  port.wait(wait);
  KJ_EXPECT(!monitor.poll(wait) && state.monitoring && state.portRequested);
  // A running monitor must not block the destroy mutation that will finish it.
  container.destroyRequest().send().wait(wait);
  KJ_EXPECT(monitor.wait(wait).getExitCode() == 0);
}

KJ_TEST(
    "Failed Container reservation releases waiting monitor and port without reaching the engine") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto reserved = kj::newPromiseAndFulfiller<void>();
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&reserved](bool start, bool, bool, kj::String, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    if (start) co_await kj::mv(reserved.promise);
    co_return kj::none;
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  auto container = grant.castAs<rpc::Container>();
  auto start = container.startRequest().send().dropPipeline();
  auto monitor = container.monitorRequest().send().dropPipeline();
  auto port = container.getTcpPortRequest().send().dropPipeline();
  KJ_EXPECT(!monitor.poll(wait) && !port.poll(wait));
  reserved.fulfiller->reject(KJ_EXCEPTION(FAILED, "Reservation failed."));
  KJ_EXPECT_THROW_MESSAGE("Container operation failed", start.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container operation failed", monitor.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container operation failed", port.wait(wait));
}

KJ_TEST("Container epoch revocation interrupts pending reservation and queued monitor and port") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto reserved = kj::newPromiseAndFulfiller<void>();
  auto revocation = kj::newPromiseAndFulfiller<void>();
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&reserved](bool start, bool, bool, kj::String, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    if (start) co_await kj::mv(reserved.promise);
    co_return kj::none;
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  auto membrane = kj::refcounted<RevokerMembrane>(kj::mv(revocation.promise));
  auto container = capnp::membrane(grant.castAs<rpc::Container>(), kj::mv(membrane));
  auto start = container.startRequest().send().dropPipeline();
  auto monitor = container.monitorRequest().send().dropPipeline();
  auto port = container.getTcpPortRequest().send().dropPipeline();
  KJ_EXPECT(!start.poll(wait) && !monitor.poll(wait) && !port.poll(wait));
  revocation.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "Container attachment revoked."));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", start.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", monitor.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", port.wait(wait));
}

class OwnerActivity final: public RequestTracker::Hooks {
 public:
  uint entered = 0;
  uint left = 0;
  void active() override {
    ++entered;
  }
  void inactive() override {
    ++left;
  }
};

KJ_TEST("Host Container owner activity follows monitor completion failure and revocation") {
  for (uint outcome = 0; outcome < 3; ++outcome) {
    kj::EventLoop loop;
    kj::WaitScope wait(loop);
    OwnerActivity hooks;
    auto tracker = kj::refcounted<RequestTracker>(hooks);
    auto engine = kj::heap<StartingEngine>();
    auto& native = *engine;
    native.started = true;
    rpc::Container::Client client(kj::mv(engine));
    capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
        [&tracker, &client](bool, bool, bool, kj::String, kj::String,
            bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      return kj::Maybe<HostContainerRpc::Resolved>(
          HostContainerRpc::Resolved{rpc::Container::Client(client), kj::str("lease"),
            kj::heap<RequestTracker::ActiveRequest>(tracker->startRequest()), {}});
    },
        kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
        rejectSnapshotCapture, rejectInactivity));
    auto revocation = kj::newPromiseAndFulfiller<void>();
    auto fenced =
        capnp::membrane(kj::mv(grant), kj::refcounted<RevokerMembrane>(kj::mv(revocation.promise)));
    auto monitor = fenced.castAs<rpc::Container>().monitorRequest().send().dropPipeline();
    KJ_EXPECT(!monitor.poll(wait) && native.monitoring && tracker->isActive());
    if (outcome == 0) {
      native.exit.fulfiller->fulfill(0);
      KJ_EXPECT(monitor.wait(wait).getExitCode() == 0);
    } else if (outcome == 1) {
      native.exit.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "Unknown process state."));
      KJ_EXPECT_THROW_MESSAGE("Container operation failed", monitor.wait(wait));
    } else {
      revocation.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "Container attachment revoked."));
      KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", monitor.wait(wait));
    }
    kj::evalLater([] {}).wait(wait);
    KJ_EXPECT(!tracker->isActive() && hooks.entered == hooks.left);
  }
}

KJ_TEST("Delayed monitor cleanup retains its original lease key after replacement startup") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto oldEngine = kj::heap<StartingEngine>();
  auto& old = *oldEngine;
  old.started = true;
  rpc::Container::Client oldClient(kj::mv(oldEngine));
  auto replacementEngine = kj::heap<Engine>();
  auto& replacement = *replacementEngine;
  rpc::Container::Client replacementClient(kj::mv(replacementEngine));
  kj::StringPtr current = "old";
  uint stops = 0;
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&oldClient, &replacementClient, &current, &stops](bool start, bool stop, bool, kj::String,
          kj::String expected, bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    if (start) current = "replacement";
    KJ_REQUIRE(expected.size() == 0 || expected == current, "Lease generation changed.");
    if (stop) ++stops;
    return kj::Maybe<HostContainerRpc::Resolved>(HostContainerRpc::Resolved{
      rpc::Container::Client(current == "old" ? oldClient : replacementClient), kj::str(current),
      {}, {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  auto container = grant.castAs<rpc::Container>();
  auto monitor = container.monitorRequest().send().dropPipeline();
  KJ_EXPECT(!monitor.poll(wait) && old.monitoring);
  container.startRequest().send().wait(wait);
  KJ_EXPECT(replacement.started == 1);
  old.exit.fulfiller->fulfill(0);
  KJ_EXPECT_THROW_MESSAGE("Container operation failed", monitor.wait(wait));
  KJ_EXPECT(stops == 0 && replacement.destroyed == 0);
}

class ExitedEngine final: public rpc::Container::Server {
 public:
  explicit ExitedEngine(int32_t code, bool disconnected): code(code), disconnected(disconnected) {}
  kj::Promise<void> start(StartContext) override {
    return kj::READY_NOW;
  }
  kj::Promise<void> monitor(MonitorContext context) override {
    KJ_REQUIRE(!disconnected, "Docker wait transport disconnected.");
    context.getResults().setExitCode(code);
    return kj::READY_NOW;
  }

 private:
  int32_t code;
  bool disconnected;
};

KJ_TEST(
    "Positive native exit codes persist stop intent and cleanup before returning monitor result") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  for (int32_t code: {0, 7, 137}) {
    rpc::Container::Client native(kj::heap<ExitedEngine>(code, false));
    uint stage = 0;
    capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
        [&native, &stage](bool start, bool stop, bool completed, kj::String,
            kj::String expectedLeaseKey,
            bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      KJ_EXPECT(!start);
      KJ_EXPECT(expectedLeaseKey == (stage == 0 ? "" : "lease"));
      KJ_EXPECT(stop == (stage != 0));
      KJ_EXPECT(completed == (stage == 2));
      ++stage;
      return kj::Maybe<HostContainerRpc::Resolved>(
          HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}, {}});
    },
        kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
        rejectSnapshotCapture, rejectInactivity));
    auto result = grant.castAs<rpc::Container>().monitorRequest().send().wait(wait);
    KJ_EXPECT(result.getExitCode() == code && stage == 3);
  }
}

KJ_TEST("Unknown native monitor transport failure never requests stop or releases its lease") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  rpc::Container::Client native(kj::heap<ExitedEngine>(0, true));
  uint lookups = 0;
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&native, &lookups](bool start, bool stop, bool completed, kj::String, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    KJ_EXPECT(!start && !stop && !completed);
    ++lookups;
    return kj::Maybe<HostContainerRpc::Resolved>(
        HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}, {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  KJ_EXPECT_THROW_MESSAGE("Container operation failed",
      grant.castAs<rpc::Container>().monitorRequest().send().wait(wait));
  KJ_EXPECT(lookups == 1);
}

KJ_TEST("Monitor lookup after a proved start can wait an exited lease without Running evidence") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  for (int32_t code: {0, 7, 137}) {
    rpc::Container::Client native(kj::heap<ExitedEngine>(code, false));
    uint stage = 0;
    capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
        [&native, &stage](bool start, bool stop, bool completed, kj::String, kj::String expected,
            bool monitor) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      KJ_EXPECT(start == (stage == 0));
      KJ_EXPECT(monitor == (stage == 2));
      KJ_EXPECT(stop == (stage >= 3));
      KJ_EXPECT(completed == (stage == 1 || stage == 4));
      KJ_EXPECT(expected == (stage == 0 ? "" : "lease"));
      ++stage;
      return kj::Maybe<HostContainerRpc::Resolved>(
          HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}, {}});
    },
        kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
        rejectSnapshotCapture, rejectInactivity));
    auto container = grant.castAs<rpc::Container>();
    container.startRequest().send().wait(wait);
    KJ_EXPECT(stage == 2);
    KJ_EXPECT(container.monitorRequest().send().wait(wait).getExitCode() == code);
    KJ_EXPECT(stage == 5);
  }
}

KJ_TEST("Monitor lookup cannot bind a replacement after its original start was proved") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  rpc::Container::Client native(kj::heap<ExitedEngine>(0, false));
  uint stops = 0;
  capnp::Capability::Client grant(kj::refcounted<HostContainerRpc>(
      [&native, &stops](bool, bool stop, bool, kj::String, kj::String expected,
          bool monitor) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    if (stop) ++stops;
    if (monitor) KJ_EXPECT(expected == "lease");
    return kj::Maybe<HostContainerRpc::Resolved>(HostContainerRpc::Resolved{
      rpc::Container::Client(native), kj::str(monitor ? "replacement" : "lease"), {}, {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(), rejectPrestartEgress, rejectNativeEgress,
      rejectSnapshotCapture, rejectInactivity));
  auto container = grant.castAs<rpc::Container>();
  container.startRequest().send().wait(wait);
  KJ_EXPECT_THROW_MESSAGE(
      "Container operation failed", container.monitorRequest().send().wait(wait));
  KJ_EXPECT(stops == 0);
}

class PendingCapability final: public capnp::Capability::Server {
 public:
  explicit PendingCapability(bool& active): active(active) {}

  DispatchCallResult dispatchCall(uint64_t,
      uint16_t,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    active = true;
    return {
      kj::Promise<void>(kj::NEVER_DONE).attach(kj::defer([this] { active = false; })), false, true};
  }

 private:
  bool& active;
};

class DerivedCapabilities final: public rpc::Container::Server {
 public:
  bool portActive = false;
  bool processActive = false;
  capnp::ByteStream::Client writer = nullptr;

  kj::Promise<void> getTcpPort(GetTcpPortContext context) override {
    capnp::Capability::Client pending(kj::heap<PendingCapability>(portActive));
    context.getResults().setPort(pending.castAs<rpc::Container::Port>());
    return kj::READY_NOW;
  }
  kj::Promise<void> exec(ExecContext context) override {
    writer = context.getParams().getStdoutWriter();
    capnp::Capability::Client pending(kj::heap<PendingCapability>(processActive));
    context.getResults().initProcess().setHandle(pending.castAs<rpc::Container::ProcessHandle>());
    return kj::READY_NOW;
  }
};

KJ_TEST("Container grant revocation cancels already-started derived port and process operations") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto derived = kj::heap<DerivedCapabilities>();
  auto& inspect = *derived;
  rpc::Container::Client native(kj::mv(derived));
  auto revocation = kj::newPromiseAndFulfiller<void>();
  auto membrane = kj::refcounted<RevokerMembrane>(kj::mv(revocation.promise));
  auto container = capnp::membrane(native, kj::mv(membrane));
  auto port = container.getTcpPortRequest().send().wait(wait).getPort();
  auto portCall = port.connectRequest().send().dropPipeline();
  KJ_EXPECT(!portCall.poll(wait) && inspect.portActive);
  auto process = container.execRequest().send().wait(wait).getProcess().getHandle();
  auto processCall = process.waitRequest().send().dropPipeline();
  KJ_EXPECT(!processCall.poll(wait) && inspect.processActive);
  revocation.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "Container attachment revoked."));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", portCall.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", processCall.wait(wait));
  KJ_EXPECT(!inspect.portActive && !inspect.processActive);
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", port.connectRequest().send().wait(wait));
}

class PendingCallback final: public kj::HttpService {
 public:
  explicit PendingCallback(kj::HttpHeaderTable& table): table(table) {}
  uint active = 0;
  kj::Maybe<kj::String> token =
      kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  kj::Promise<void> request(kj::HttpMethod,
      kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncInputStream&,
      Response& response) override {
    return response.sendError(405, "Method Not Allowed", table);
  }
  kj::Promise<void> connect(kj::StringPtr,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream&,
      ConnectResponse& response,
      kj::HttpConnectSettings) override {
    KJ_REQUIRE(containerCallbackAuthorized(token, headers));
    ++active;
    KJ_DEFER(--active);
    kj::HttpHeaders result(table);
    response.accept(202, "Accepted", result);
    co_await kj::Promise<void>(kj::NEVER_DONE);
  }

 private:
  kj::HttpHeaderTable& table;
};

KJ_TEST("Container reverse callback revocation closes a started HTTP CONNECT stream") {
  auto io = kj::setupAsyncIo();
  kj::HttpHeaderTable table;
  PendingCallback callback(table);
  kj::HttpServer server(io.provider->getTimer(), table, callback);
  auto pipe = kj::newTwoWayPipe();
  kj::Canceler revocation;
  auto serving =
      revocation.wrap(server.listenHttp(kj::mv(pipe.ends[0]))).eagerlyEvaluate([](kj::Exception&&) {
  });
  auto client = kj::newHttpClient(table, *pipe.ends[1]);
  kj::HttpHeaders headers(table);
  headers.add(CONTAINER_CALLBACK_HEADER, KJ_ASSERT_NONNULL(callback.token));
  auto request = client->connect("example.com:443", headers, {});
  KJ_EXPECT(request.status.wait(io.waitScope).statusCode == 202 && callback.active);
  revocation.cancel("Container attachment revoked.");
  KJ_EXPECT(!callback.active);
  kj::byte byte;
  KJ_EXPECT(request.connection->tryRead(&byte, 1, 1).wait(io.waitScope) == 0);
  serving.wait(io.waitScope);
}

KJ_TEST("Container reverse callback streams exceed the idle pool and all revoke together") {
  auto io = kj::setupAsyncIo();
  kj::HttpHeaderTable table;
  PendingCallback callback(table);
  kj::HttpServer server(io.provider->getTimer(), table, callback);
  kj::Canceler revocation;
  kj::Vector<kj::Promise<void>> serving;
  kj::Vector<kj::Own<kj::AsyncIoStream>> clients;
  kj::Vector<kj::Own<kj::HttpClient>> httpClients;
  kj::Vector<kj::Own<kj::AsyncIoStream>> tunnels;
  for (uint i = 0; i < 32; ++i) {
    auto pipe = kj::newTwoWayPipe();
    serving.add(revocation.wrap(server.listenHttp(kj::mv(pipe.ends[0])))
            .eagerlyEvaluate([](kj::Exception&&) {}));
    clients.add(kj::mv(pipe.ends[1]));
    auto client = kj::newHttpClient(table, *clients.back());
    kj::HttpHeaders headers(table);
    headers.add(CONTAINER_CALLBACK_HEADER, KJ_ASSERT_NONNULL(callback.token));
    auto request = client->connect("example.com:443", headers, {});
    KJ_EXPECT(request.status.wait(io.waitScope).statusCode == 202);
    tunnels.add(kj::mv(request.connection));
    httpClients.add(kj::mv(client));
  }
  KJ_EXPECT(callback.active == 32);
  revocation.cancel("Container attachment revoked.");
  KJ_EXPECT(callback.active == 0);
  kj::byte byte;
  for (auto& tunnel: tunnels) KJ_EXPECT(tunnel->tryRead(&byte, 1, 1).wait(io.waitScope) == 0);
  kj::joinPromises(serving.releaseAsArray()).wait(io.waitScope);
}

class PrestartEngine final: public rpc::Container::Server {
 public:
  explicit PrestartEngine(uint& staged): staged(staged) {}
  uint starts = 0;
  uint runningRegistrations = 0;
  kj::Promise<void> start(StartContext) override {
    KJ_EXPECT(staged == 3);
    ++starts;
    return kj::READY_NOW;
  }
  kj::Promise<void> setEgressHttp(SetEgressHttpContext) override {
    ++runningRegistrations;
    return kj::READY_NOW;
  }

 private:
  uint& staged;
};

KJ_TEST("Host interception registers before a lease without reserving or starting the Engine") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  uint staged = 0;
  uint reservations = 0;
  bool admitted = false;
  bool authority = true;
  auto engine = kj::heap<PrestartEngine>(staged);
  auto& inspect = *engine;
  rpc::Container::Client native(kj::mv(engine));
  capnp::Capability::Client capability(kj::refcounted<HostContainerRpc>(
      [&](bool start, bool stop, bool, kj::String startup, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    KJ_REQUIRE(authority, "Frozen authority has ended");
    KJ_EXPECT(!stop);
    if (start) {
      admitted = true;
      ++reservations;
    } else {
      KJ_EXPECT(startup.size() == 0);
    }
    if (!admitted) return kj::Maybe<HostContainerRpc::Resolved>(kj::none);
    return kj::Maybe<HostContainerRpc::Resolved>(
        HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}, {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>(),
      [&](uint16_t method, capnp::AnyPointer::Reader params) {
    KJ_EXPECT(inspect.starts == 0 && reservations == 0);
    if (method == 8) {
      KJ_EXPECT(
          params.getAs<rpc::Container::SetEgressHttpParams>().getHostPort() == "http.invalid");
    } else if (method == 9) {
      KJ_EXPECT(
          params.getAs<rpc::Container::SetEgressHttpsParams>().getHostPort() == "https.invalid");
    } else {
      KJ_EXPECT(method == 13 &&
          params.getAs<rpc::Container::SetEgressTcpParams>().getHostPort() == "tcp.invalid");
    }
    ++staged;
  }, rejectNativeEgress, rejectSnapshotCapture, rejectInactivity));
  auto container = capability.castAs<rpc::Container>();
  auto http = container.setEgressHttpRequest();
  http.setHostPort("http.invalid");
  http.send().wait(wait);
  auto https = container.setEgressHttpsRequest();
  https.setHostPort("https.invalid");
  https.send().wait(wait);
  auto tcp = container.setEgressTcpRequest();
  tcp.setHostPort("tcp.invalid");
  tcp.send().wait(wait);
  KJ_EXPECT(staged == 3 && reservations == 0 && inspect.starts == 0);
  KJ_EXPECT(!container.statusRequest().send().wait(wait).getRunning());
  authority = false;
  KJ_EXPECT_THROW_MESSAGE(
      "Container operation failed", container.setEgressHttpRequest().send().wait(wait));
  KJ_EXPECT(staged == 3 && reservations == 0 && inspect.starts == 0);
  authority = true;
  container.startRequest().send().wait(wait);
  KJ_EXPECT(reservations == 1 && inspect.starts == 1);
  container.setEgressHttpRequest().send().wait(wait);
  KJ_EXPECT(staged == 3 && inspect.runningRegistrations == 1);
}

}  // namespace
}  // namespace workerd::server
