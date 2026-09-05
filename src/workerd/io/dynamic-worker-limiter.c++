// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "dynamic-worker-limiter.h"

#include <workerd/io/io-channels.h>
#include <workerd/io/worker-interface.h>
#include <workerd/jsg/exception.h>

namespace workerd {

class DynamicWorkerLimiter::Lease {
 public:
  Lease(kj::Rc<DynamicWorkerLimiter> owner, const void* identity)
      : owner(kj::mv(owner)),
        identity(identity) {}

  ~Lease() noexcept(false) {
    auto& count = KJ_ASSERT_NONNULL(owner->active.find(identity));
    if (--count == 0) owner->active.erase(identity);
  }

 private:
  kj::Rc<DynamicWorkerLimiter> owner;
  const void* identity;
};

kj::Own<void> DynamicWorkerLimiter::acquire(const void* identity) {
  auto& count = active.findOrCreate(identity, [&]() -> decltype(active)::Entry {
    JSG_REQUIRE(active.size() < limit, Error, "Too many distinct Dynamic Workers in flight.");
    return {identity, 0};
  });
  ++count;
  return kj::heap<Lease>(addRefToThis(), identity);
}

namespace {

class LimitedWorkerStub final: public WorkerStubChannel {
 public:
  LimitedWorkerStub(kj::Own<WorkerStubChannel> inner, kj::Rc<DynamicWorkerLimiter> limiter)
      : inner(kj::mv(inner)),
        limiter(kj::mv(limiter)) {}

  kj::Own<IoChannelFactory::SubrequestChannel> getEntrypointResolved(
      kj::Maybe<kj::String> name, Frankenvalue props, kj::Maybe<ResourceLimits> limits) override {
    auto channel = inner->getEntrypoint(kj::mv(name), kj::mv(props), limits);
    return kj::refcounted<Entrypoint>(kj::mv(channel), kj::addRef(*inner), limiter.addRef());
  }

  kj::Own<IoChannelFactory::ActorClassChannel> getActorClassResolved(
      kj::Maybe<kj::String> name, Frankenvalue props, kj::Maybe<ResourceLimits> limits) override {
    return inner->getActorClass(kj::mv(name), kj::mv(props), limits);
  }

 private:
  kj::Own<WorkerStubChannel> inner;
  kj::Rc<DynamicWorkerLimiter> limiter;

  class Entrypoint final: public IoChannelFactory::SubrequestChannel {
   public:
    Entrypoint(kj::Own<IoChannelFactory::SubrequestChannel> inner,
        kj::Own<WorkerStubChannel> identity,
        kj::Rc<DynamicWorkerLimiter> limiter)
        : inner(kj::mv(inner)),
          identity(kj::mv(identity)),
          limiter(kj::mv(limiter)) {}

    kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata metadata) override {
      auto lease = limiter->acquire(identity.get());
      return inner->startRequest(kj::mv(metadata)).attach(kj::addRef(*this), kj::mv(lease));
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
    kj::Own<WorkerStubChannel> identity;
    kj::Rc<DynamicWorkerLimiter> limiter;
  };
};

}  // namespace

kj::Own<WorkerStubChannel> DynamicWorkerLimiter::wrap(kj::Own<WorkerStubChannel> inner) {
  return kj::refcounted<LimitedWorkerStub>(kj::mv(inner), addRefToThis());
}

}  // namespace workerd
