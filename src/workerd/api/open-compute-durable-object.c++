// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "open-compute-durable-object.h"

#include <workerd/jsg/setup.h>

namespace workerd::api::open_compute::durable_objects {

jsg::Promise<jsg::Ref<Response>> DurableObject::fetch(jsg::Lock& js,
    jsg::Value input,
    jsg::Optional<jsg::Value> init,
    const jsg::TypeHandler<Fetch>& handler) {
  auto fn = JSG_REQUIRE_NONNULL(
      handler.tryUnwrap(js, policy.getHandle(js).get(js, "fetch"_kj)), Error, "DO_RPC_UNSUPPORTED");
  return fn(js, kj::mv(input), kj::mv(init));
}

jsg::Ref<Socket> DurableObject::connect(jsg::Lock& js,
    jsg::Value address,
    jsg::Optional<jsg::Value> options,
    const jsg::TypeHandler<Connect>& handler) {
  auto fn = JSG_REQUIRE_NONNULL(handler.tryUnwrap(js, policy.getHandle(js).get(js, "connect"_kj)),
      Error, "DO_RPC_UNSUPPORTED");
  return fn(js, kj::mv(address), kj::mv(options));
}

kj::Maybe<jsg::JsValue> DurableObject::getRpcMethod(jsg::Lock& js, kj::String name) {
  if (name == "then"_kj) return kj::none;
  auto member = policy.getHandle(js).get(js, name);
  return newRpcPolicyProperty(js, JSG_THIS, kj::mv(name), member);
}

kj::Own<IoChannelFactory::SubrequestChannel> DurableObject::getSubrequestChannel(
    IoContext& context) {
  return newPolicySubrequestChannel(
      context, createPolicyChannelCallbacks(context, policy.getHandle(context.getCurrentLock())));
}

api::Fetcher::ClientForOneCall DurableObject::getClientForOneCall(
    jsg::Lock&, kj::Maybe<ActorCallRetryState::Attempt>) {
  JSG_FAIL_REQUIRE(Error, "DO_RPC_UNSUPPORTED");
}

void DurableObject::serialize(jsg::Lock& js, jsg::Serializer& serializer) {
  transport->serialize(js, serializer);
  serializer.writeRawBytes(getId()->getInner().toString().asBytes());
}

void ServiceStub::serialize(jsg::Lock& js, jsg::Serializer& serializer) {
  open_compute::Fetcher::serialize(js, serializer);
  serializer.writeRawBytes(id.asBytes());
}

jsg::Ref<ServiceStub> ServiceStub::deserialize(jsg::Lock& js,
    rpc::SerializationTag,
    jsg::Deserializer& deserializer,
    const jsg::TypeHandler<jsg::Ref<api::Fetcher>>& innerHandler) {
  auto transport = api::Fetcher::deserializeForWrappedBinding(js, deserializer);
  auto id = deserializer.readRawString(64);
  for (char digit: id) {
    JSG_REQUIRE((digit >= '0' && digit <= '9') || (digit >= 'a' && digit <= 'f'), TypeError,
        "DO_ID_INVALID");
  }
  auto env = js.obj();
  env.createDataProperty(
      js, js.str("fetcher"_kj), jsg::JsValue(innerHandler.wrap(js, transport.addRef())));
  env.createDataProperty(js, js.str("id"_kj), js.str(id));
  v8::Isolate::AllowJavascriptExecutionScope allowJs(js.v8Isolate);
  auto module = KJ_REQUIRE_NONNULL(
      js.resolveInternalModule("cloudflare-internal:open-compute-host-policy"_kj),
      "Durable Object host policy is unavailable");
  auto create = KJ_REQUIRE_NONNULL(
      module.get(js, "createDurableObjectStubPolicy"_kj).tryCast<jsg::JsFunction>(),
      "Durable Object host policy factory is unavailable");
  auto policy = KJ_REQUIRE_NONNULL(create.callNoReceiver(js, env).tryCast<jsg::JsObject>(),
      "Durable Object host policy did not return an object");
  return js.alloc<ServiceStub>(js, kj::mv(transport), policy, kj::mv(id));
}

}  // namespace workerd::api::open_compute::durable_objects
