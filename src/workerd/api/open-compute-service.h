// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/api/http.h>
#include <workerd/api/sockets.h>

namespace workerd::api::open_compute {

// Shared fail-closed RPC boundary for bindings whose private policy owns dispatch.
kj::Own<api::Fetcher::CrossContextOutgoingFactory> newPolicyOnlyFactory(kj::StringPtr code);

// HTTP and TCP callbacks re-enter their original actor and use the same private public-binding policy.
jsg::JsObject createPolicyChannelCallbacks(IoContext& context, jsg::JsObject policy);
kj::Own<IoChannelFactory::SubrequestChannel> newPolicySubrequestChannel(
    IoContext& context, jsg::JsObject callbacks);

// A native Fetcher whose private policy owns Service authority and lifecycle. The native type
// retains the public Fetcher name and ancestry; the policy never enters the tenant environment.
class Fetcher: public api::Fetcher {
 public:
  using Fetch =
      jsg::Function<jsg::Promise<jsg::Ref<Response>>(jsg::Value, jsg::Optional<jsg::Value>)>;
  using Connect = jsg::Function<jsg::Ref<Socket>(jsg::Value, jsg::Optional<jsg::Value>)>;

  Fetcher(jsg::Lock& js, jsg::Ref<api::Fetcher> transport, jsg::JsObject policy);

  jsg::Promise<jsg::Ref<Response>> fetch(jsg::Lock& js,
      jsg::Value input,
      jsg::Optional<jsg::Value> init,
      const jsg::TypeHandler<Fetch>& handler);
  jsg::Ref<Socket> connect(jsg::Lock& js,
      jsg::Value address,
      jsg::Optional<jsg::Value> options,
      const jsg::TypeHandler<Connect>& handler);
  kj::Maybe<jsg::JsValue> getRpcMethod(jsg::Lock& js, kj::String name) override;

  kj::Own<IoChannelFactory::SubrequestChannel> getSubrequestChannel(IoContext& context) override;

  ClientForOneCall getClientForOneCall(
      jsg::Lock& js, kj::Maybe<ActorCallRetryState::Attempt> attempt) override;

  JSG_RESOURCE_TYPE(Fetcher) {
    JSG_INHERIT(api::Fetcher);
    JSG_METHOD(fetch);
    JSG_METHOD(connect);
  }

  void serialize(jsg::Lock& js, jsg::Serializer& serializer);
  static jsg::Ref<Fetcher> deserialize(jsg::Lock& js,
      rpc::SerializationTag tag,
      jsg::Deserializer& deserializer,
      const jsg::TypeHandler<jsg::Ref<api::Fetcher>>& innerHandler,
      const jsg::TypeHandler<jsg::Ref<Fetcher>>& selfHandler);
  JSG_SERIALIZABLE(rpc::SerializationTag::OPEN_COMPUTE_SERVICE_FETCHER);

 private:
  jsg::Ref<api::Fetcher> transport;
  jsg::JsRef<jsg::JsObject> policy;

  void visitForGc(jsg::GcVisitor& visitor) {
    visitor.visit(transport, policy);
  }
};

}  // namespace workerd::api::open_compute
