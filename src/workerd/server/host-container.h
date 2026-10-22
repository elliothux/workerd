#pragma once

#include <workerd/io/container.capnp.h>
#include <workerd/io/io-channels.h>
#include <workerd/io/request-tracker.h>

#include <capnp/compat/json.h>
#include <kj/timer.h>

namespace workerd::server {

// The timer belongs to the host owner, not to a tenant capability or an engine client.
class HostContainerInactivity final {
 public:
  HostContainerInactivity(kj::Timer& timer, bool active, kj::Function<kj::Promise<void>()> stop)
      : timer(timer),
        activeNow(active),
        stop(kj::mv(stop)) {}

  static void requireValid(int64_t durationMs) {
    JSG_REQUIRE(durationMs > 0, TypeError,
        "setInactivityTimeout() cannot be called with a durationMs <= 0");
    JSG_REQUIRE(
        durationMs <= 6 * 60 * 60 * 1000, Error, "setInactivityTimeout() cannot exceed 6 hours");
  }

  void configure(int64_t durationMs) {
    requireValid(durationMs);
    duration = durationMs * kj::MILLISECONDS;
    if (!activeNow) inactive();
  }

  void reset() {
    duration = kj::none;
    if (!stopping) task = kj::none;
  }

  void active() {
    activeNow = true;
    if (!stopping) task = kj::none;
  }

  void inactive() {
    activeNow = false;
    if (stopping) return;
    KJ_IF_SOME(timeout, duration) {
      task = timer.afterDelay(timeout)
                 .then([this]() -> kj::Promise<void> {
        duration = kj::none;
        stopping = true;
        KJ_DEFER(stopping = false);
        co_await stop();
      }).eagerlyEvaluate([](kj::Exception&&) {
        // The persisted stop intent and capacity remain owned by platform reconciliation.
        KJ_LOG(ERROR, "Container inactivity cleanup failed.");
      });
    }
  }

  bool retainsOwner() const {
    return duration != kj::none || stopping;
  }

 private:
  kj::Timer& timer;
  bool activeNow;
  bool stopping = false;
  kj::Maybe<kj::Duration> duration;
  kj::Function<kj::Promise<void>()> stop;
  kj::Maybe<kj::Promise<void>> task;
};

// Each operation retains the control owner for at most the documented 15-minute window.
class HostContainerActivity final: public kj::Refcounted {
 public:
  HostContainerActivity(RequestTracker& tracker, kj::Timer& timer)
      : request(kj::heap<RequestTracker::ActiveRequest>(tracker.startRequest())) {
    timeout = timer.afterDelay(15 * kj::MINUTES).then([this] {
      request = kj::none;
    }).eagerlyEvaluate(nullptr);
  }

 private:
  kj::Maybe<kj::Own<RequestTracker::ActiveRequest>> request;
  kj::Maybe<kj::Promise<void>> timeout;
};

class HostContainerAuthority final: public kj::Refcounted {
 public:
  HostContainerAuthority(ResolveHostContainerLease resolve, CaptureHostContainerSnapshot capture)
      : resolve(kj::mv(resolve)),
        capture(kj::mv(capture)) {}
  ResolveHostContainerLease resolve;
  CaptureHostContainerSnapshot capture;
};

// Admission stays outside the engine client; returned native capabilities retain their RPC brands.
class HostContainerRpc final: public capnp::Capability::Server,
                              public IoChannelFactory::HostContainerEgressRegistrar {
 public:
  class MutationQueue final: public kj::Refcounted {
   public:
    kj::PromiseFulfillerPair<void> take() {
      auto next = kj::newPromiseAndFulfiller<void>();
      auto previous = kj::mv(queue);
      queue = previous.addBranch()
                  .then([done = kj::mv(next.promise)]() mutable {
        return done.catch_([](kj::Exception&&) {});
      }).fork();
      return {previous.addBranch(), kj::mv(next.fulfiller)};
    }

   private:
    kj::ForkedPromise<void> queue = kj::Promise<void>(kj::READY_NOW).fork();
  };
  struct Resolved {
    rpc::Container::Client client;
    kj::String leaseKey;
    kj::Own<void> ownerActivity;
    HostContainerInspection inspection;
  };
  using Resolve = kj::Function<kj::Promise<kj::Maybe<Resolved>>(
      bool, bool, bool, kj::String, kj::String, bool)>;
  using StageEgress = kj::Function<void(uint16_t, capnp::AnyPointer::Reader)>;
  using RegisterNativeEgress = kj::Function<kj::Promise<void>(
      bool, uint16_t, kj::String, kj::Own<IoChannelFactory::SubrequestChannel>)>;
  HostContainerRpc(Resolve resolve,
      kj::Own<MutationQueue> mutations,
      StageEgress stageEgress,
      RegisterNativeEgress registerNativeEgress,
      CaptureHostContainerSnapshot capture,
      kj::Function<void(int64_t)> configureInactivity)
      : resolve(kj::mv(resolve)),
        mutations(kj::mv(mutations)),
        stageEgress(kj::mv(stageEgress)),
        registerNativeEgress(kj::mv(registerNativeEgress)),
        capture(kj::mv(capture)),
        configureInactivity(kj::mv(configureInactivity)) {}

  kj::Promise<void> registerEgress(uint16_t methodId,
      kj::String hostPort,
      kj::Own<IoChannelFactory::SubrequestChannel> channel) override {
    KJ_REQUIRE(methodId == 8 || methodId == 9 || methodId == 13);
    auto turn = mutations->take();
    KJ_DEFER(turn.fulfiller->fulfill());
    co_await turn.promise;
    auto target = co_await resolve(false, false, false, kj::str(""), kj::str(""), false);
    co_await registerNativeEgress(target != kj::none, methodId, kj::mv(hostPort), kj::mv(channel));
  }

  DispatchCallResult dispatchCall(uint64_t interfaceId,
      uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    KJ_REQUIRE(interfaceId == capnp::typeId<rpc::Container>() && methodId <= 15,
        "Invalid Container capability method.");
    return {forward(methodId, kj::mv(context)).catch_([](kj::Exception&&) {
      JSG_FAIL_REQUIRE(Error, "Container operation failed.");
    }),
      false, true};
  }

 private:
  kj::Promise<void> forward(
      uint16_t methodId, capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) {
    auto validDirectory = [](kj::StringPtr directory) {
      return directory.size() > 0 && directory.size() <= 4096 && directory.startsWith("/") &&
          directory.findFirst('\0') == kj::none;
    };
    if (methodId == 10) {
      JSG_REQUIRE(
          validDirectory(
              context.getParams().getAs<rpc::Container::SnapshotDirectoryParams>().getDir()),
          Error, "Invalid Container snapshot directory.");
    }
    kj::Maybe<kj::Own<kj::PromiseFulfiller<void>>> mutation;
    KJ_DEFER(if (mutation != kj::none) KJ_ASSERT_NONNULL(mutation)->fulfill());
    bool egress = methodId == 8 || methodId == 9 || methodId == 13;
    if (methodId == 1 || methodId == 3 || methodId == 10 || methodId == 11 || methodId == 7 ||
        egress) {
      auto turn = mutations->take();
      mutation = kj::mv(turn.fulfiller);
      co_await turn.promise;
    } else {
      // start() queues its RPC before the automatic monitor. Do not resolve the lease until that
      // mutation has finished, but release this barrier before a long-lived monitor or stream.
      auto turn = mutations->take();
      co_await turn.promise;
      turn.fulfiller->fulfill();
    }
    kj::String startup = kj::str("");
    if (methodId == 1) {
      capnp::JsonCodec codec;
      codec.setHasMode(capnp::HasMode::NON_DEFAULT);
      startup = codec.encode(context.getParams().getAs<rpc::Container::StartParams>());
    }
    auto client = co_await resolve(methodId == 1, methodId == 3, false, kj::mv(startup),
        methodId == 2
            ? startedLeaseKey.map([](auto& key) { return kj::str(key); }).orDefault(kj::str(""))
            : kj::str(""),
        methodId == 2);
    if (methodId == 7) {
      auto durationMs =
          context.getParams().getAs<rpc::Container::SetInactivityTimeoutParams>().getDurationMs();
      configureInactivity(durationMs);
      context.getResults().initAs<rpc::Container::SetInactivityTimeoutResults>();
      co_return;
    }
    if (client == kj::none && (methodId == 0 || methodId == 3 || methodId == 14)) {
      if (methodId == 0)
        context.getResults().initAs<rpc::Container::StatusResults>().setRunning(false);
      else if (methodId == 3)
        context.getResults().initAs<rpc::Container::DestroyResults>();
      else
        context.getResults().initAs<rpc::Container::InspectResults>().initInfo().setNone();
      co_return;
    }
    if (client == kj::none && egress) {
      // Official interception registration precedes start(). Authority was checked above, but
      // no lease or engine object exists yet. Register in the owner's native egress state.
      stageEgress(methodId, context.getParams());
      if (methodId == 8)
        context.getResults().initAs<rpc::Container::SetEgressHttpResults>();
      else if (methodId == 9)
        context.getResults().initAs<rpc::Container::SetEgressHttpsResults>();
      else
        context.getResults().initAs<rpc::Container::SetEgressTcpResults>();
      co_return;
    }
    auto& target = JSG_REQUIRE_NONNULL(client, Error, "The Container is not running.");
    if (methodId == 2) {
      KJ_IF_SOME(expected, startedLeaseKey) {
        JSG_REQUIRE(expected == target.leaseKey, Error, "Container execution authority changed.");
      }
    }
    auto leaseKey = kj::str(target.leaseKey);
    if (methodId == 10 || methodId == 11) {
      kj::Maybe<kj::String> name;
      kj::Maybe<kj::String> directory;
      if (methodId == 10) {
        auto params = context.getParams().getAs<rpc::Container::SnapshotDirectoryParams>();
        directory = kj::str(params.getDir());
        if (params.hasName()) name = kj::str(params.getName());
      } else {
        auto params = context.getParams().getAs<rpc::Container::SnapshotContainerParams>();
        if (params.hasName()) name = kj::str(params.getName());
      }
      auto snapshot = co_await capture(kj::mv(leaseKey), kj::mv(name), kj::mv(directory));
      JSG_REQUIRE(snapshot.id.size() > 0 && snapshot.id.size() <= 256 &&
              snapshot.id.findFirst('\0') == kj::none && snapshot.size >= 0 &&
              snapshot.size <= jsg::MAX_SAFE_INTEGER &&
              snapshot.size == static_cast<uint64_t>(snapshot.size) &&
              (snapshot.dir != kj::none) == (methodId == 10),
          Error, "Invalid Container snapshot metadata.");
      if (methodId == 10) {
        auto& directory =
            JSG_REQUIRE_NONNULL(snapshot.dir, Error, "Invalid Container snapshot metadata.");
        JSG_REQUIRE(validDirectory(directory), Error, "Invalid Container snapshot metadata.");
        auto result =
            context.getResults().initAs<rpc::Container::SnapshotDirectoryResults>().initSnapshot();
        result.setId(snapshot.id);
        result.setSize(static_cast<uint64_t>(snapshot.size));
        result.setDir(directory);
        KJ_IF_SOME(name, snapshot.name) {
          result.setName(name);
        }
      } else {
        auto result =
            context.getResults().initAs<rpc::Container::SnapshotContainerResults>().initSnapshot();
        result.setId(snapshot.id);
        result.setSize(static_cast<uint64_t>(snapshot.size));
        KJ_IF_SOME(name, snapshot.name) {
          result.setName(name);
        }
      }
      co_return;
    }
    auto request =
        target.client.typelessRequest(capnp::typeId<rpc::Container>(), methodId, kj::none, {});
    request.setAs<capnp::AnyPointer>(context.getParams());
    if (methodId == 14) {
      // Engine status is authoritative for running/null; presentation belongs to this exact lease.
      auto response = co_await request.send();
      auto info = response.getAs<rpc::Container::InspectResults>().getInfo();
      auto result = context.getResults().initAs<rpc::Container::InspectResults>().initInfo();
      if (info.isStarted()) {
        auto current = co_await resolve(false, false, false, kj::str(""), kj::str(leaseKey), false);
        auto& presentation =
            JSG_REQUIRE_NONNULL(current, Error, "Container execution authority changed.");
        JSG_REQUIRE(
            presentation.leaseKey == leaseKey, Error, "Container execution authority changed.");
        auto started = result.initStarted();
        started.setImage(presentation.inspection.image);
        auto labels = started.initLabels(presentation.inspection.labels.size());
        for (auto i: kj::indices(presentation.inspection.labels)) {
          labels[i].setName(presentation.inspection.labels[i].name);
          labels[i].setValue(presentation.inspection.labels[i].value);
        }
      } else {
        result.setNone();
      }
      co_return;
    }
    if (methodId == 1 || methodId == 2 || methodId == 3) {
      auto response = co_await request.send();
      if (methodId == 2) {
        // The native monitor RPC succeeds with an exact exit code, including signal/nonzero exits.
        // Transport failures reject above and must retain unknown leases. Persist a stop intent
        // before the provider proves process fencing and cleanup.
        co_await resolve(false, true, false, kj::str(""), kj::str(leaseKey), false);
      }
      co_await resolve(false, methodId != 1, true, kj::str(""), kj::str(leaseKey), false);
      target.ownerActivity = {};
      if (methodId == 1) startedLeaseKey = kj::str(leaseKey);
      if (methodId == 3) startedLeaseKey = kj::none;
      context.getResults().setAs<capnp::AnyPointer>(response);
    } else {
      co_await context.tailCall(kj::mv(request));
    }
  }

  kj::Maybe<kj::String> startedLeaseKey;
  Resolve resolve;
  kj::Own<MutationQueue> mutations;
  StageEgress stageEgress;
  RegisterNativeEgress registerNativeEgress;
  CaptureHostContainerSnapshot capture;
  kj::Function<void(int64_t)> configureInactivity;
};

}  // namespace workerd::server
