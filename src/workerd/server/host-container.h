#pragma once

#include <workerd/io/container.capnp.h>
#include <workerd/io/io-channels.h>

#include <capnp/compat/json.h>

namespace workerd::server {

// Admission stays outside the engine client; returned native capabilities retain their RPC brands.
class HostContainerRpc final: public capnp::Capability::Server {
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
  using Resolve =
      kj::Function<kj::Promise<kj::Maybe<rpc::Container::Client>>(bool, bool, bool, kj::String)>;
  HostContainerRpc(Resolve resolve, kj::Own<MutationQueue> mutations)
      : resolve(kj::mv(resolve)),
        mutations(kj::mv(mutations)) {}

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
    if (methodId == 1 || methodId == 3) {
      auto turn = mutations->take();
      mutation = kj::mv(turn.fulfiller);
      co_await turn.promise;
    }
    kj::String startup = kj::str("");
    if (methodId == 1) {
      capnp::JsonCodec codec;
      codec.setHasMode(capnp::HasMode::NON_DEFAULT);
      startup = codec.encode(context.getParams().getAs<rpc::Container::StartParams>());
    }
    auto client = co_await resolve(methodId == 1, methodId == 3, false, kj::mv(startup));
    if (client == kj::none && (methodId == 0 || methodId == 3)) {
      if (methodId == 0)
        context.getResults().initAs<rpc::Container::StatusResults>().setRunning(false);
      else
        context.getResults().initAs<rpc::Container::DestroyResults>();
      co_return;
    }
    auto& target = JSG_REQUIRE_NONNULL(client, Error, "The Container is not running.");
    auto request = target.typelessRequest(capnp::typeId<rpc::Container>(), methodId, kj::none, {});
    request.setAs<capnp::AnyPointer>(context.getParams());
    if (methodId == 1 || methodId == 3) {
      auto response = co_await request.send();
      co_await resolve(false, methodId == 3, true, kj::str(""));
      context.getResults().setAs<capnp::AnyPointer>(response);
    } else {
      co_await context.tailCall(kj::mv(request));
    }
  }

  Resolve resolve;
  kj::Own<MutationQueue> mutations;
};

}  // namespace workerd::server
