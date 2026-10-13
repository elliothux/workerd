// Copyright (c) 2017-2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/api/actor.h>
#include <workerd/api/http.h>
#include <workerd/api/open-compute-durable-object.h>
#include <workerd/api/open-compute-service.h>
#include <workerd/api/queue.h>
#include <workerd/api/sockets.h>
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

  void validatePublicTransport(jsg::Ref<Fetcher> transport) {
    requirePublicTransport(transport);
  }

  // Public binding dispatch uses the original receiver and its normal subrequest budget.
  jsg::Promise<jsg::Ref<Response>> fetchPublicTransport(
      jsg::Lock& js, jsg::Ref<Fetcher> transport, jsg::Ref<Request> request) {
    requirePublicTransport(transport);
    return transport->api::Fetcher::fetch(js, kj::mv(request), kj::none);
  }

  jsg::Ref<Socket> connectPublicTransport(jsg::Lock& js,
      jsg::Ref<Fetcher> transport,
      AnySocketAddress address,
      jsg::Optional<SocketOptions> options) {
    requirePublicTransport(transport);
    return transport->api::Fetcher::connect(js, kj::mv(address), kj::mv(options));
  }

  // These INTERNAL operations select native methods without consulting a public prototype.
  kj::StringPtr rpcTransportKind(jsg::Lock& js, jsg::JsValue value) {
    auto object = value.tryCast<jsg::JsObject>();
    if (object == kj::none) return ""_kj;
    auto& receiver = KJ_ASSERT_NONNULL(object);
    KJ_IF_SOME(fetcher, receiver.tryUnwrapAs<Fetcher>(js)) {
      requireRawTransport(fetcher);
      return "fetcher"_kj;
    }
    if (receiver.tryUnwrapAs<JsRpcStub>(js) != kj::none) return "stub"_kj;
    if (receiver.tryUnwrapAs<JsRpcPromise>(js) != kj::none) return "promise"_kj;
    if (receiver.tryUnwrapAs<JsRpcProperty>(js) != kj::none) return "property"_kj;
    return ""_kj;
  }

  jsg::JsValue getRpcTransportMember(
      jsg::Lock& js, jsg::JsObject receiver, kj::String name, kj::String resultKind) {
    auto kind = resultKind == "value"_kj ? JsRpcResultKind::PUBLIC
        : resultKind == "stub"_kj        ? JsRpcResultKind::PRIVATE_STUB
        : resultKind == "stream"_kj      ? JsRpcResultKind::PRIVATE_STREAM
                                         : JsRpcResultKind::PRIVATE_DATA;
    JSG_REQUIRE(resultKind == "value"_kj || resultKind == "stub"_kj || resultKind == "stream"_kj ||
            resultKind == "data"_kj,
        TypeError, "SERVICE_BINDING_DENIED");
    auto mark = [&](jsg::JsValue value) {
      KJ_IF_SOME(object, value.tryCast<jsg::JsObject>()) {
        auto property = JSG_REQUIRE_NONNULL(
            object.tryUnwrapAs<JsRpcProperty>(js), TypeError, "SERVICE_BINDING_DENIED");
        property->setInternalResultKind(js, kind);
      }
      return value;
    };
    KJ_IF_SOME(fetcher, receiver.tryUnwrapAs<Fetcher>(js)) {
      requireRawTransport(fetcher);
      return mark(fetcher->api::Fetcher::getRpcMethod(js, kj::mv(name)).orDefault(js.undefined()));
    }
    KJ_IF_SOME(stub, receiver.tryUnwrapAs<JsRpcStub>(js)) {
      JSG_REQUIRE(kind == JsRpcResultKind::PUBLIC || stub->isNativeInternalTransport(), TypeError,
          "SERVICE_BINDING_DENIED");
      return mark(stub->getRpcMethod(js, kj::mv(name)).orDefault(js.undefined()));
    }
    const auto& handler = KJ_REQUIRE_NONNULL(js.tryGetTypeHandler<jsg::Ref<JsRpcProperty>>());
    KJ_IF_SOME(promise, receiver.tryUnwrapAs<JsRpcPromise>(js)) {
      JSG_REQUIRE(kind == JsRpcResultKind::PUBLIC || promise->isNativeInternalTransport(),
          TypeError, "SERVICE_BINDING_DENIED");
      KJ_IF_SOME(property, promise->getProperty(js, kj::mv(name))) {
        property->setInternalResultKind(js, kind);
        return jsg::JsValue(handler.wrap(js, kj::mv(property)));
      }
      return js.undefined();
    }
    KJ_IF_SOME(parent, receiver.tryUnwrapAs<JsRpcProperty>(js)) {
      JSG_REQUIRE(kind == JsRpcResultKind::PUBLIC || parent->isNativeInternalTransport(), TypeError,
          "SERVICE_BINDING_DENIED");
      KJ_IF_SOME(property, parent->getProperty(js, kj::mv(name))) {
        property->setInternalResultKind(js, kind);
        return jsg::JsValue(handler.wrap(js, kj::mv(property)));
      }
      return js.undefined();
    }
    JSG_FAIL_REQUIRE(TypeError, "SERVICE_BINDING_DENIED");
  }

  jsg::JsValue thenRpcTransport(jsg::Lock& js,
      jsg::JsObject receiver,
      v8::Local<v8::Function> fulfilled,
      jsg::Optional<v8::Local<v8::Function>> rejected) {
    KJ_IF_SOME(promise, receiver.tryUnwrapAs<JsRpcPromise>(js)) {
      return promise->then(js, fulfilled, rejected);
    }
    KJ_IF_SOME(property, receiver.tryUnwrapAs<JsRpcProperty>(js)) {
      return property->then(js, fulfilled, rejected);
    }
    JSG_FAIL_REQUIRE(TypeError, "SERVICE_BINDING_DENIED");
  }

  void disposeRpcTransport(jsg::Lock& js, jsg::JsObject receiver) {
    KJ_IF_SOME(stub, receiver.tryUnwrapAs<JsRpcStub>(js)) {
      stub->disposeForJs(js);
      return;
    }
    KJ_IF_SOME(promise, receiver.tryUnwrapAs<JsRpcPromise>(js)) {
      if (promise->getResultKindForInternalTransport() == JsRpcResultKind::PUBLIC)
        promise->dispose(js);
      else
        promise->disposeForInternalTransport(js);
      return;
    }
    JSG_FAIL_REQUIRE(TypeError, "SERVICE_BINDING_DENIED");
  }

  jsg::Ref<JsRpcStub> duplicateRpcTransport(
      jsg::Lock& js, jsg::Ref<JsRpcStub> receiver, kj::String exposure) {
    JSG_REQUIRE(
        exposure == "private"_kj || exposure == "value"_kj, TypeError, "SERVICE_BINDING_DENIED");
    auto duplicate = receiver->dup(js);
    if (exposure == "value"_kj) return duplicate;
    auto& handler = KJ_REQUIRE_NONNULL(js.tryGetTypeHandler<jsg::Ref<JsRpcStub>>());
    auto value = handler.wrap(js, duplicate.addRef()).As<v8::Object>();
    auto attributes =
        static_cast<v8::PropertyAttribute>(v8::ReadOnly | v8::DontDelete | v8::DontEnum);
    v8::Local<v8::String> thenName = js.str("then"_kj);
    v8::Local<v8::Value> undefined = js.undefined();
    JSG_REQUIRE(
        jsg::check(value->DefineOwnProperty(js.v8Context(), thenName, undefined, attributes)),
        TypeError, "SERVICE_BINDING_DENIED");
    return duplicate;
  }

  jsg::Ref<Fetcher> createPrivateTransport(jsg::Lock& js, jsg::Ref<Fetcher> transport) {
    requirePublicTransport(transport);
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
    JSG_METHOD(rpcTransportKind);
    JSG_METHOD(getRpcTransportMember);
    JSG_METHOD(thenRpcTransport);
    JSG_METHOD(disposeRpcTransport);
    JSG_METHOD(duplicateRpcTransport);
    JSG_METHOD(validatePublicTransport);
    JSG_METHOD(fetchPublicTransport);
    JSG_METHOD(connectPublicTransport);
    JSG_METHOD(admitSubrequest);
    JSG_METHOD(createDurableObjectId);
    JSG_METHOD(createDurableObjectNamespace);
    JSG_METHOD(createDurableObjectStub);
    JSG_METHOD(createServiceRpcStub);
    JSG_METHOD(isRpcStub);
    JSG_METHOD(installQueuePolicy);
    JSG_METHOD(decodeQueueV8);
  }

 private:
  static void requireRawTransport(jsg::Ref<Fetcher>& transport) {
    JSG_REQUIRE(dynamic_cast<open_compute::Fetcher*>(&*transport) == nullptr &&
            dynamic_cast<open_compute::durable_objects::DurableObject*>(&*transport) == nullptr,
        TypeError, "SERVICE_BINDING_DENIED");
  }

  static void requirePublicTransport(jsg::Ref<Fetcher>& transport) {
    requireRawTransport(transport);
    JSG_REQUIRE(!transport->channelOrClientFactory.is<jsg::Ref<Fetcher>>(), TypeError,
        "SERVICE_BINDING_DENIED");
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
