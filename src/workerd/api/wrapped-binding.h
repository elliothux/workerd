// Copyright (c) 2017-2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/api/actor.h>
#include <workerd/api/http.h>
#include <workerd/api/open-compute-durable-object.h>
#include <workerd/api/open-compute-service.h>
#include <workerd/api/queue.h>
#include <workerd/io/io-context.h>
#include <workerd/io/worker-interface.capnp.h>
#include <workerd/jsg/jsg.h>
#include <workerd/jsg/modules-new.h>
#include <workerd/jsg/url.h>

namespace workerd::api {

constexpr kj::StringPtr WRAPPED_BINDING_INNER_NAME = "fetcher"_kj;

// Native base for an application-level binding implemented by an internal JavaScript module around
// one service stub. Wrapper classes inherit this type, so their instances carry JSG's internal
// fields and use the normal JSG resource serialization path.
class WrappedBinding final: public jsg::Object {
 public:
  explicit WrappedBinding(jsg::LenientOptional<jsg::Ref<Fetcher>> maybeFetcher) {
    KJ_IF_SOME(fetcher, maybeFetcher) {
      this->fetcher = kj::mv(fetcher);
    }
  }

  static jsg::Ref<WrappedBinding> constructor(
      jsg::Lock& js, jsg::LenientOptional<jsg::Ref<Fetcher>> fetcher) {
    return js.alloc<WrappedBinding>(kj::mv(fetcher));
  }

  JSG_RESOURCE_TYPE(WrappedBinding) {}

  void serialize(jsg::Lock& js, jsg::Serializer& serializer);
  static jsg::Ref<WrappedBinding> deserialize(jsg::Lock& js,
      rpc::SerializationTag tag,
      jsg::Deserializer& deserializer,
      const jsg::TypeHandler<jsg::Ref<Fetcher>>& innerHandler,
      const jsg::TypeHandler<jsg::Ref<WrappedBinding>>& selfHandler);

  JSG_SERIALIZABLE(rpc::SerializationTag::WRAPPED_BINDING);

  void setWrapperModule(kj::String moduleName) {
    KJ_REQUIRE(fetcher != kj::none, "serializable wrapped binding has no fetcher");
    KJ_REQUIRE(wrapperModule == kj::none, "wrapped binding module was already set");
    wrapperModule = kj::mv(moduleName);
  }

 private:
  kj::Maybe<jsg::Ref<Fetcher>> fetcher;
  kj::Maybe<kj::String> wrapperModule;

  void visitForGc(jsg::GcVisitor& visitor) {
    KJ_IF_SOME(f, fetcher) {
      visitor.visit(f);
    }
  }
};

class WrappedBindingModule final: public jsg::Object {
 public:
  WrappedBindingModule() = default;
  WrappedBindingModule(jsg::Lock&, const jsg::Url&) {}

  jsg::Ref<Fetcher> createPrivateTransport(jsg::Lock& js, jsg::Ref<Fetcher> transport) {
    return js.alloc<Fetcher>(kj::mv(transport));
  }

  // Public policy dispatch shares the existing native quota authority. Private lifecycle
  // operations use a control transport, so acquiring a lease is not a second user operation.
  void admitSubrequest() {
    IoContext::current().getLimitEnforcer().newSubrequest(false);
  }

  // The private namespace policy validates the persisted namespace prefix and authentication tag.
  // This factory only gives that validated identity the existing native DurableObjectId API.
  jsg::Ref<DurableObjectId> createDurableObjectId(jsg::Lock& js,
      kj::String value,
      jsg::Optional<kj::String> name,
      jsg::Optional<kj::String> jurisdiction);

  jsg::Ref<DurableObjectNamespace> createDurableObjectNamespace(
      jsg::Lock& js, jsg::JsObject policy) {
    return js.alloc<DurableObjectNamespace>(js, policy);
  }

  jsg::Ref<open_compute::durable_objects::DurableObject> createDurableObjectStub(jsg::Lock& js,
      jsg::Ref<DurableObjectId> id,
      jsg::Ref<Fetcher> transport,
      jsg::JsObject policy) {
    return js.alloc<open_compute::durable_objects::DurableObject>(
        js, kj::mv(id), kj::mv(transport), policy);
  }

  jsg::Ref<JsRpcStub> createServiceRpcStub(jsg::Lock& js, jsg::JsObject policy) {
    auto result = JsRpcStub::constructor(js, policy);
    result->setServicePolicy(js, policy);
    return result;
  }

  bool isRpcStub(
      jsg::Lock& js, jsg::JsValue value, const jsg::TypeHandler<jsg::Ref<JsRpcStub>>& handler) {
    return handler.tryUnwrap(js, value) != kj::none;
  }

  void installQueuePolicy(jsg::Lock& js, jsg::Ref<WorkerQueue> queue, jsg::JsObject policy) {
    queue->setPublicationPolicy(js, policy);
  }

  jsg::JsValue decodeQueueV8(jsg::Lock& js, jsg::JsBufferSource bytes) {
    return js.alloc<QueueWireCodec>()->decodeV8(js, kj::mv(bytes));
  }

  JSG_RESOURCE_TYPE(WrappedBindingModule) {
    JSG_NESTED_TYPE(WrappedBinding);
    JSG_METHOD(createPrivateTransport);
    JSG_METHOD(admitSubrequest);
    JSG_METHOD(createDurableObjectId);
    JSG_METHOD(createDurableObjectNamespace);
    JSG_METHOD(createDurableObjectStub);
    JSG_METHOD(createServiceRpcStub);
    JSG_METHOD(isRpcStub);
    JSG_METHOD(installQueuePolicy);
    JSG_METHOD(decodeQueueV8);
  }
};

template <class Registry>
void registerWrappedBindingModule(Registry& registry, auto featureFlags) {
  registry.template addBuiltinModule<WrappedBindingModule>(
      "cloudflare-internal:wrapped-binding", workerd::jsg::ModuleRegistry::Type::INTERNAL);
}

template <typename TypeWrapper>
kj::Own<jsg::modules::ModuleBundle> getInternalWrappedBindingModuleBundle(auto featureFlags) {
  jsg::modules::ModuleBundle::BuiltinBuilder builder(
      jsg::modules::ModuleBundle::BuiltinBuilder::Type::BUILTIN_ONLY);
  static const auto kSpecifier = "cloudflare-internal:wrapped-binding"_url;
  builder.addObject<WrappedBindingModule, TypeWrapper>(kSpecifier);
  return builder.finish();
}

#define EW_WRAPPED_BINDING_ISOLATE_TYPES                                                           \
  api::WrappedBinding, api::WrappedBindingModule, api::open_compute::Fetcher,                      \
      api::open_compute::durable_objects::DurableObject,                                           \
      api::open_compute::durable_objects::ServiceStub

}  // namespace workerd::api
