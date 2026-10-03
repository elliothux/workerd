// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "open-compute-service.h"

#include <workerd/jsg/setup.h>

namespace workerd::api::open_compute {
namespace {

// Inherited native handlers and channel extraction must never expose the private transport.
class PolicyOnlyFactory final: public api::Fetcher::CrossContextOutgoingFactory {
 public:
  explicit PolicyOnlyFactory(kj::StringPtr code): code(kj::str(code)) {}
  api::Fetcher::OutgoingFactory::Result newSingleUseClient(
      IoContext&, kj::Maybe<kj::String>, api::Fetcher::MakeUserSpanParent) override {
    JSG_FAIL_REQUIRE(Error, code);
  }

  kj::Own<IoChannelFactory::SubrequestChannel> getSubrequestChannel(IoContext&) override {
    JSG_FAIL_REQUIRE(Error, code);
  }

 private:
  kj::String code;
};

}  // namespace

kj::Own<api::Fetcher::CrossContextOutgoingFactory> newPolicyOnlyFactory(kj::StringPtr code) {
  return kj::heap<PolicyOnlyFactory>(code);
}

Fetcher::Fetcher(jsg::Lock& js, jsg::Ref<api::Fetcher> transport, jsg::JsObject policy)
    : api::Fetcher(newPolicyOnlyFactory("SERVICE_BINDING_DENIED"_kj), RequiresHostAndProtocol::YES),
      transport(kj::mv(transport)),
      policy(js, policy) {}

jsg::Promise<jsg::Ref<Response>> Fetcher::fetch(jsg::Lock& js,
    jsg::Value input,
    jsg::Optional<jsg::Value> init,
    const jsg::TypeHandler<Fetch>& handler) {
  auto fn = JSG_REQUIRE_NONNULL(handler.tryUnwrap(js, policy.getHandle(js).get(js, "fetch"_kj)),
      Error, "SERVICE_BINDING_DENIED");
  return fn(js, kj::mv(input), kj::mv(init));
}

jsg::Ref<Socket> Fetcher::connect(jsg::Lock& js,
    jsg::Value address,
    jsg::Optional<jsg::Value> options,
    const jsg::TypeHandler<Connect>& handler) {
  auto fn = JSG_REQUIRE_NONNULL(handler.tryUnwrap(js, policy.getHandle(js).get(js, "connect"_kj)),
      Error, "SERVICE_BINDING_DENIED");
  return fn(js, kj::mv(address), kj::mv(options));
}

kj::Maybe<jsg::JsValue> Fetcher::getRpcMethod(jsg::Lock& js, kj::String name) {
  if (name == "then"_kj) return kj::none;
  auto member = policy.getHandle(js).get(js, name);
  return newRpcPolicyProperty(js, JSG_THIS, kj::mv(name), member);
}

api::Fetcher::ClientForOneCall Fetcher::getClientForOneCall(
    jsg::Lock&, kj::Maybe<ActorCallRetryState::Attempt>) {
  JSG_FAIL_REQUIRE(Error, "SERVICE_BINDING_DENIED");
}

void Fetcher::serialize(jsg::Lock& js, jsg::Serializer& serializer) {
  transport->serialize(js, serializer);
}

jsg::Ref<Fetcher> Fetcher::deserialize(jsg::Lock& js,
    rpc::SerializationTag,
    jsg::Deserializer& deserializer,
    const jsg::TypeHandler<jsg::Ref<api::Fetcher>>& innerHandler,
    const jsg::TypeHandler<jsg::Ref<Fetcher>>& selfHandler) {
  auto transport = api::Fetcher::deserializeForWrappedBinding(js, deserializer);
  auto env = js.obj();
  env.createDataProperty(
      js, js.str("fetcher"_kj), jsg::JsValue(innerHandler.wrap(js, transport.addRef())));

  // The fixed host policy and its scope registry are shared with entrypoint dispatch.
  v8::Isolate::AllowJavascriptExecutionScope allowJs(js.v8Isolate);
  auto module = KJ_REQUIRE_NONNULL(
      js.resolveInternalModule("cloudflare-internal:open-compute-host-policy"_kj),
      "Service host policy is unavailable");
  auto create =
      KJ_REQUIRE_NONNULL(module.get(js, "createServiceBinding"_kj).tryCast<jsg::JsFunction>(),
          "Service host policy factory is unavailable");
  auto policy = KJ_REQUIRE_NONNULL(create.callNoReceiver(js, env).tryCast<jsg::JsObject>(),
      "Service host policy did not return an object");
  auto result = js.alloc<Fetcher>(js, kj::mv(transport), policy);
  auto registerBinding =
      KJ_REQUIRE_NONNULL(module.get(js, "registerServiceBinding"_kj).tryCast<jsg::JsFunction>(),
          "Service host policy registry is unavailable");
  registerBinding.call(js, js.null(), jsg::JsValue(selfHandler.wrap(js, result.addRef())), policy);
  return result;
}

}  // namespace workerd::api::open_compute
