// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/api/actor.h>
#include <workerd/api/open-compute-service.h>

namespace workerd::api::open_compute::durable_objects {

// Native DO stub. Identity stays in the native base; only dispatch and ordering use private policy.
class DurableObject final: public api::DurableObject {
 public:
  using Fetch = open_compute::Fetcher::Fetch;
  using Connect = open_compute::Fetcher::Connect;

  DurableObject(jsg::Lock& js,
      jsg::Ref<api::DurableObjectId> id,
      jsg::Ref<api::Fetcher> transport,
      jsg::JsObject policy)
      : api::DurableObject(kj::mv(id), newPolicyOnlyFactory("DO_RPC_UNSUPPORTED"_kj)),
        transport(kj::mv(transport)),
        policy(js, policy) {}

  jsg::Promise<jsg::Ref<Response>> fetch(jsg::Lock& js,
      jsg::Value input,
      jsg::Optional<jsg::Value> init,
      const jsg::TypeHandler<Fetch>& handler);
  jsg::Ref<Socket> connect(jsg::Lock& js,
      jsg::Value address,
      jsg::Optional<jsg::Value> options,
      const jsg::TypeHandler<Connect>& handler);
  kj::Maybe<jsg::JsValue> getRpcMethod(jsg::Lock& js, kj::String name) override;
  ClientForOneCall getClientForOneCall(
      jsg::Lock& js, kj::Maybe<ActorCallRetryState::Attempt> attempt) override;

  JSG_RESOURCE_TYPE(DurableObject) {
    JSG_INHERIT(api::DurableObject);
    JSG_METHOD(fetch);
    JSG_METHOD(connect);
  }
  void serialize(jsg::Lock& js, jsg::Serializer& serializer);
  JSG_ONEWAY_SERIALIZABLE(rpc::SerializationTag::OPEN_COMPUTE_DURABLE_OBJECT_STUB);

 private:
  jsg::Ref<api::Fetcher> transport;
  jsg::JsRef<jsg::JsObject> policy;
  void visitForGc(jsg::GcVisitor& visitor) {
    visitor.visit(transport, policy);
  }
};

// Transferred DO stubs are ServiceStubs: native id/name metadata does not transfer.
class ServiceStub final: public open_compute::Fetcher {
 public:
  ServiceStub(jsg::Lock& js, jsg::Ref<api::Fetcher> transport, jsg::JsObject policy, kj::String id)
      : open_compute::Fetcher(js, kj::mv(transport), policy),
        id(kj::mv(id)) {}

  JSG_RESOURCE_TYPE(ServiceStub) {
    JSG_INHERIT(open_compute::Fetcher);
  }
  void serialize(jsg::Lock& js, jsg::Serializer& serializer);
  static jsg::Ref<ServiceStub> deserialize(jsg::Lock& js,
      rpc::SerializationTag tag,
      jsg::Deserializer& deserializer,
      const jsg::TypeHandler<jsg::Ref<api::Fetcher>>& innerHandler);
  JSG_SERIALIZABLE(rpc::SerializationTag::OPEN_COMPUTE_DURABLE_OBJECT_STUB);

 private:
  kj::String id;
};

}  // namespace workerd::api::open_compute::durable_objects
