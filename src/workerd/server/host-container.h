#pragma once

#include <workerd/io/container.capnp.h>
#include <workerd/io/io-channels.h>
#include <workerd/io/request-tracker.h>

#include <capnp/compat/json.h>

namespace workerd::server {

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
    kj::Own<RequestTracker::ActiveRequest> ownerActivity;
  };
  using Resolve = kj::Function<kj::Promise<kj::Maybe<Resolved>>(
      bool, bool, bool, kj::String, kj::String, bool)>;
  using StageEgress = kj::Function<void(uint16_t, capnp::AnyPointer::Reader)>;
  using RegisterNativeEgress = kj::Function<kj::Promise<void>(
      bool, uint16_t, kj::String, kj::Own<IoChannelFactory::SubrequestChannel>)>;
  HostContainerRpc(Resolve resolve,
      kj::Own<MutationQueue> mutations,
      StageEgress stageEgress,
      RegisterNativeEgress registerNativeEgress)
      : resolve(kj::mv(resolve)),
        mutations(kj::mv(mutations)),
        stageEgress(kj::mv(stageEgress)),
        registerNativeEgress(kj::mv(registerNativeEgress)) {}

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
    JSG_REQUIRE(methodId != 10 && methodId != 11, Error, "Container snapshots are unavailable.");
    kj::Maybe<kj::Own<kj::PromiseFulfiller<void>>> mutation;
    KJ_DEFER(if (mutation != kj::none) KJ_ASSERT_NONNULL(mutation)->fulfill());
    bool egress = methodId == 8 || methodId == 9 || methodId == 13;
    if (methodId == 1 || methodId == 3 || egress) {
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
    if (client == kj::none && (methodId == 0 || methodId == 3)) {
      if (methodId == 0)
        context.getResults().initAs<rpc::Container::StatusResults>().setRunning(false);
      else
        context.getResults().initAs<rpc::Container::DestroyResults>();
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
    auto request =
        target.client.typelessRequest(capnp::typeId<rpc::Container>(), methodId, kj::none, {});
    request.setAs<capnp::AnyPointer>(context.getParams());
    if (methodId == 1 || methodId == 2 || methodId == 3) {
      auto response = co_await request.send();
      if (methodId == 2) {
        // The native monitor RPC succeeds with an exact exit code, including signal/nonzero exits.
        // Transport failures reject above and must retain unknown leases. Persist a stop intent
        // before the provider proves process fencing and cleanup.
        co_await resolve(false, true, false, kj::str(""), kj::str(leaseKey), false);
      }
      co_await resolve(false, methodId != 1, true, kj::str(""), kj::str(leaseKey), false);
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
};

}  // namespace workerd::server
