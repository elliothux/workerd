// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "open-compute-service.h"

#include "global-scope.h"
#include "system-streams.h"

#include <workerd/io/frankenvalue.h>
#include <workerd/jsg/setup.h>
#include <workerd/util/stream-utils.h>

namespace workerd::api::open_compute {
namespace {

struct PolicyCallback {
  jsg::JsRef<jsg::JsObject> handlers;
};

class PolicyChannelState final: public kj::Refcounted {
 public:
  PolicyChannelState(IoContext& context, jsg::JsObject handlers)
      : origin(context.getId()),
        live(context.getWeakRef()),
        loopback(JSG_REQUIRE_NONNULL(
            context.getActor(), Error, "Public binding interception requires a Durable Object.")
                .getLoopback()),
        callback(context.addObjectReverse(kj::heap<PolicyCallback>(
            PolicyCallback{jsg::JsRef<jsg::JsObject>(context.getCurrentLock(), handlers)}))) {}

  void requireLive() {
    auto& context = JSG_REQUIRE_NONNULL(
        live->tryGet(), Error, "The public binding callback owner is no longer running.");
    JSG_REQUIRE(context.getAbortReason() == kj::none, Error,
        "The public binding callback owner is no longer running.");
  }

  IoContext::Id origin;
  kj::Own<IoContext::WeakRef> live;
  kj::Own<Worker::Actor::Loopback> loopback;
  ReverseIoOwn<PolicyCallback> callback;
};

// The private ownership response carries no data. EOF or any byte means ownership was lost.
class PolicyLeaseSink final: public WritableStreamSink {
 public:
  kj::Promise<void> write(kj::ArrayPtr<const byte> bytes) override {
    if (bytes.size() == 0) return kj::READY_NOW;
    return KJ_EXCEPTION(DISCONNECTED, "Public binding WebSocket owner disconnected.");
  }
  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
    for (auto piece: pieces) {
      if (piece.size() != 0) {
        return KJ_EXCEPTION(DISCONNECTED, "Public binding WebSocket owner disconnected.");
      }
    }
    return kj::READY_NOW;
  }
  kj::Promise<void> end() override {
    return KJ_EXCEPTION(DISCONNECTED, "Public binding WebSocket owner disconnected.");
  }
  void abort(kj::Exception) override {}
};

class PolicyFetchEvent final: public WorkerInterface::CustomEvent {
 public:
  PolicyFetchEvent(kj::Own<PolicyChannelState> state,
      kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body,
      kj::HttpService::Response& response)
      : state(kj::mv(state)),
        method(method),
        url(kj::str(url)),
        headers(headers),
        body(body),
        response(response) {}

  kj::Promise<Result> run(kj::Own<IoContext_IncomingRequest> incoming,
      kj::Maybe<kj::StringPtr>,
      kj::Maybe<Worker_VersionInfo>,
      Frankenvalue,
      kj::TaskSet& tasks,
      bool) override {
    auto& context = incoming->getContext();
    JSG_REQUIRE(
        state->origin == context.getId(), Error, "The public binding callback owner was replaced.");
    state->requireLive();
    incoming->delivered();
    KJ_DEFER(incoming->drain(tasks, kj::mv(incoming)));
    auto input = newNeuterableInputStream(body);
    KJ_DEFER(input->neuter(KJ_EXCEPTION(DISCONNECTED, "Public binding callback input closed.")));
    auto canceled = kj::rc<bool>(false);
    kj::Maybe<jsg::Ref<AbortController>> abortController;
    auto completed = false;
    KJ_DEFER({
      *canceled = true;
      if (!completed) {
        if (abortController != kj::none) {
          auto& controller = KJ_REQUIRE_NONNULL(abortController);
          context.addWaitUntil(
              context.run([controller = controller.addRef()](Worker::Lock& lock) mutable {
            controller->getSignal()->triggerAbort(
                lock, JSG_KJ_EXCEPTION(DISCONNECTED, DOMAbortError, "The client has disconnected"));
          }));
        }
      }
    });
    KJ_TRY {
      auto send = co_await context.run(
          [state = kj::addRef(*state), method = method, url = kj::str(url),
              headers = headers.clone(), input = kj::addRef(*input), response = &response,
              canceled = canceled.addRef(),
              &abortController](Worker::Lock& lock, IoContext& context) mutable {
        jsg::Lock& js = lock;
        JSG_REQUIRE(state->origin == context.getId(), Error,
            "The public binding callback owner was replaced.");
        auto& callback = JSG_REQUIRE_NONNULL(state->callback.tryGet(), Error,
            "The public binding callback owner is no longer running.");
        kj::Maybe<Body::ExtractedBody> requestBody;
        auto jsHeaders = js.alloc<Headers>(js, headers, Headers::Guard::REQUEST);
        if (headers.get(kj::HttpHeaderId::CONTENT_LENGTH) != kj::none ||
            headers.get(kj::HttpHeaderId::TRANSFER_ENCODING) != kj::none ||
            input->tryGetLength().orDefault(1) > 0) {
          auto stream = JsReadableStream::create(
              js, context, newSystemStream(kj::addRef(*input), StreamEncoding::IDENTITY));
          requestBody = Body::ExtractedBody(js, kj::mv(stream));
          if (!jsHeaders->hasCommon(capnp::CommonHeaderName::CONTENT_LENGTH) &&
              !jsHeaders->hasCommon(capnp::CommonHeaderName::TRANSFER_ENCODING)) {
            KJ_IF_SOME(length, input->tryGetLength()) {
              jsHeaders->setCommon(capnp::CommonHeaderName::CONTENT_LENGTH, kj::str(length));
            } else {
              jsHeaders->setCommon(capnp::CommonHeaderName::TRANSFER_ENCODING, kj::str("chunked"));
            }
          }
        }
        kj::Maybe<jsg::Ref<AbortSignal>> signal;
        auto flags = FeatureFlags::get(js);
        if (flags.getEnableRequestSignal()) {
          signal.emplace(abortController
                  .emplace(js.alloc<AbortController>(js,
                      flags.getRequestSignalPassthrough()
                          ? AbortSignal::Flag::NONE
                          : AbortSignal::Flag::IGNORE_FOR_SUBREQUESTS))
                  ->getSignal());
        }
        auto request = js.alloc<Request>(js, method, url, Request::Redirect::MANUAL,
            kj::mv(jsHeaders),
            js.alloc<api::Fetcher>(
                IoContext::NEXT_CLIENT_CHANNEL, api::Fetcher::RequiresHostAndProtocol::YES),
            kj::mv(signal), CfProperty(), kj::mv(requestBody), kj::none, Request::CacheMode::NONE);
        auto& requestHandler = KJ_REQUIRE_NONNULL(js.tryGetTypeHandler<jsg::Ref<Request>>());
        auto wrapped = KJ_REQUIRE_NONNULL(
            jsg::JsValue(requestHandler.wrap(js, kj::mv(request))).tryCast<jsg::JsObject>());
        auto handlers = callback.handlers.getHandle(js);
        auto fetch = JSG_REQUIRE_NONNULL(handlers.get(js, "fetch"_kj).tryCast<jsg::JsFunction>(),
            TypeError, "Public binding fetch callback is unavailable.");
        auto result = js.toPromise(fetch.call(js, handlers, wrapped));
        return context.awaitJs(js,
            result.then(js,
                context.addFunctor(
                    [response, headers = kj::mv(headers), canceled = canceled.addRef(),
                        state = kj::addRef(*state)](jsg::Lock& js, IoContext& context,
                        jsg::Value value) mutable -> IoOwn<kj::Promise<DeferredProxy<void>>> {
          auto packet =
              JSG_REQUIRE_NONNULL(jsg::JsValue(value.getHandle(js)).tryCast<jsg::JsObject>(),
                  TypeError, "Public binding fetch did not return a private response packet.");
          auto object = JSG_REQUIRE_NONNULL(packet.get(js, "response"_kj).tryCast<jsg::JsObject>(),
              TypeError, "Public binding fetch did not return a Response.");
          auto inner = JSG_REQUIRE_NONNULL(object.tryUnwrapAs<api::Response>(js), TypeError,
              "Public binding fetch did not return a Response.");
          JSG_REQUIRE(inner->getType() != "error"_kj, TypeError,
              "Public binding fetch returned an error Response.");
          auto errorCode = packet.get(js, "errorCode"_kj);
          JSG_REQUIRE(errorCode.isNull() ||
                  (errorCode.isString() && errorCode.toString(js) == "SERVICE_UNAVAILABLE"_kj),
              TypeError, "Public binding fetch returned an invalid private error.");
          auto leaseValue = packet.get(js, "leaseBody"_kj);
          kj::Own<void> releaseLease;
          kj::Promise<void> lostLease = kj::NEVER_DONE;
          if (!leaseValue.isNull()) {
            auto lease = JSG_REQUIRE_NONNULL(JsReadableStream::tryUnwrap(js, leaseValue), TypeError,
                "Public binding fetch returned an invalid private lease body.");
            JSG_REQUIRE(inner->getWebSocket(js) != kj::none, TypeError,
                "Public binding ownership requires a WebSocket response.");
            if (*canceled || !errorCode.isNull()) {
              lease.forceCancel(js, kj::none).markAsHandled(js);
            } else {
              lostLease = context.waitForDeferredProxy(
                  lease.pumpTo(js, kj::heap<PolicyLeaseSink>(), EndStream::YES));
              auto ownedLease = context.addObject(kj::heap(lease.addRef(js)));
              releaseLease = kj::heap(
                  kj::defer([state = kj::addRef(*state), lease = kj::mv(ownedLease)]() mutable {
                KJ_IF_SOME(context, state->live->tryGet()) {
                  if (context.hasCurrentIncomingRequest()) {
                    context.addWaitUntil(context.run(
                        [lease = kj::mv(lease)](Worker::Lock& lock, IoContext& context) mutable {
                      return context.awaitJs(lock, lease->forceCancel(lock, kj::none));
                    }));
                  }
                }
              }));
            }
          }
          if (*canceled || !errorCode.isNull()) {
            KJ_IF_SOME(ws, inner->getWebSocket(js)) {
              if (!ws->isReleased()) {
                if (!ws->isAccepted() && ws->getUrl() == kj::none) ws->accept(js, kj::none);
                ws->close(js, 1000, kj::none);
              }
            }
            KJ_IF_SOME(body, inner->getBody(js)) {
              body.forceCancel(js, kj::none).markAsHandled(js);
            }
            JSG_REQUIRE(errorCode.isNull(), Error, "SERVICE_UNAVAILABLE");
            return context.addObject(kj::heap(addNoopDeferredProxy(kj::READY_NOW)));
          }
          // A locally terminated WebSocket can keep the outer send pending until it closes.
          // The same lease must therefore protect both outer delivery and deferred proxying.
          auto leaseWatch = lostLease.fork();
          auto outerLease = addNoopDeferredProxy(leaseWatch.addBranch());
          auto innerLease = leaseWatch.addBranch();
          auto send = inner->send(js, *response, {.allowWebSocket = headers.isWebSocket()}, headers)
                          .exclusiveJoin(kj::mv(outerLease));
          return context.addObject(kj::heap(
              send.then([releaseLease = kj::mv(releaseLease), lostLease = kj::mv(innerLease)](
                            DeferredProxy<void> transmission) mutable {
            transmission.proxyTask = transmission.proxyTask.exclusiveJoin(kj::mv(lostLease))
                                         .attach(kj::mv(releaseLease));
            return transmission;
          })));
        })));
      });
      co_await send.proxyTask.exclusiveJoin(context.onAbort());
      completed = true;
      co_return Result{.outcome = EventOutcome::OK};
    }
    KJ_CATCH(error) {
      markExceptionAsDelivered(error);
      kj::throwFatalException(kj::mv(error));
    }
  }

  kj::Promise<Result> sendRpc(capnp::HttpOverCapnpFactory&,
      capnp::ByteStreamFactory&,
      FrankenvalueHandler&,
      rpc::EventDispatcher::Client) override {
    JSG_FAIL_REQUIRE(Error, "Public binding callbacks are local only.");
  }
  kj::Promise<Result> notSupported() override {
    JSG_FAIL_REQUIRE(Error, "Public binding callback is unavailable.");
  }
  uint16_t getType() override {
    return 0;
  }
  tracing::EventInfo getEventInfo() const override {
    return tracing::CustomEventInfo();
  }

 private:
  kj::Own<PolicyChannelState> state;
  kj::HttpMethod method;
  kj::String url;
  const kj::HttpHeaders& headers;
  kj::AsyncInputStream& body;
  kj::HttpService::Response& response;
};

struct PolicyConnectOwnership {
  bool canceled = false;
  kj::Maybe<IoOwn<jsg::Ref<Socket>>> socket;
  kj::Own<void> connectionAbortHandle;
};

class PolicyConnectEvent final: public WorkerInterface::CustomEvent {
 public:
  PolicyConnectEvent(kj::Own<PolicyChannelState> state,
      kj::StringPtr address,
      kj::AsyncIoStream& connection,
      kj::HttpService::ConnectResponse& response,
      const kj::HttpHeaderTable& table)
      : state(kj::mv(state)),
        address(kj::str(address)),
        connection(connection),
        response(response),
        table(table) {}

  kj::Promise<Result> run(kj::Own<IoContext_IncomingRequest> incoming,
      kj::Maybe<kj::StringPtr>,
      kj::Maybe<Worker_VersionInfo>,
      Frankenvalue,
      kj::TaskSet& tasks,
      bool) override {
    auto& context = incoming->getContext();
    JSG_REQUIRE(
        state->origin == context.getId(), Error, "The public binding callback owner was replaced.");
    state->requireLive();
    incoming->delivered();
    KJ_DEFER(incoming->drain(tasks, kj::mv(incoming)));
    auto tunnel = newNeuterableIoStream(connection);
    KJ_DEFER(tunnel->neuter(KJ_EXCEPTION(DISCONNECTED, "Public binding callback tunnel closed.")));
    auto ownership = kj::rc<PolicyConnectOwnership>();
    KJ_DEFER({
      ownership->canceled = true;
      // Revoke native ownership before any cleanup that needs a live JavaScript context.
      ownership->connectionAbortHandle = nullptr;
      if (ownership->socket != kj::none) {
        auto& socket = KJ_ASSERT_NONNULL(ownership->socket);
        if (context.getAbortReason() != kj::none) return;
        context.addWaitUntil(
            context.run([socket = kj::mv(socket)](Worker::Lock& lock, IoContext& context) mutable {
          jsg::Lock& js = lock;
          auto readable = (*socket)->getReadable(js);
          auto writable = (*socket)->getWritable(js);
          readable.forceCancel(js, kj::none).markAsHandled(js);
          writable.forceAbort(js, kj::none).markAsHandled(js);
          return context.awaitJs(js, (*socket)->close(js));
        }));
      }
    });
    KJ_TRY {
      co_await context.run(
          [state = kj::addRef(*state), address = kj::str(address), ownership = ownership.addRef()](
              Worker::Lock& lock, IoContext& context) mutable {
        jsg::Lock& js = lock;
        auto& callback = JSG_REQUIRE_NONNULL(state->callback.tryGet(), Error,
            "The public binding callback owner is no longer running.");
        auto handlers = callback.handlers.getHandle(js);
        auto connect =
            JSG_REQUIRE_NONNULL(handlers.get(js, "connect"_kj).tryCast<jsg::JsFunction>(),
                TypeError, "Public binding connect callback is unavailable.");
        return context.awaitJs(js,
            js.toPromise(connect.call(js, handlers, js.str(address)))
                .then(js,
                    context.addFunctor([ownership = ownership.addRef()](jsg::Lock& js,
                                           IoContext& context, jsg::Value value) mutable {
          auto object =
              JSG_REQUIRE_NONNULL(jsg::JsValue(value.getHandle(js)).tryCast<jsg::JsObject>(),
                  TypeError, "Public binding connect did not return a Socket.");
          auto socket = JSG_REQUIRE_NONNULL(object.tryUnwrapAs<Socket>(js), TypeError,
              "Public binding connect did not return a Socket.");
          if (ownership->canceled) {
            socket->close(js).markAsHandled(js);
            return;
          }
          ownership->connectionAbortHandle = socket->getConnectionAbortHandle(js);
          ownership->socket.emplace(context.addObject(kj::heap(kj::mv(socket))));
        })));
      });
      kj::HttpHeaders headers(table);
      response.accept(200, "OK", headers);
      co_await context.run([ownership = ownership.addRef(), tunnel = tunnel.addRef()](
                               Worker::Lock& lock, IoContext& context) mutable {
        jsg::Lock& js = lock;
        auto& socket = *KJ_REQUIRE_NONNULL(ownership->socket);
        kj::Own<kj::AsyncInputStream> input = tunnel.addRef().toOwn();
        kj::Own<kj::AsyncOutputStream> output = tunnel.addRef().toOwn();
        auto incoming = JsReadableStream::create(
            js, context, newSystemStream(kj::mv(input), StreamEncoding::IDENTITY));
        auto outgoing = JsWritableStream::create(
            js, context, newSystemStream(kj::mv(output), StreamEncoding::IDENTITY), kj::none);
        auto writable = socket->getWritable(js);
        auto readable = socket->getReadable(js);
        auto toSocket = context.awaitJs(js, incoming.pipeTo(js, writable));
        auto fromSocket = context.awaitJs(js, readable.pipeTo(js, outgoing));
        return kj::joinPromisesFailFast(kj::arr(kj::mv(toSocket), kj::mv(fromSocket)));
      });
      co_await context.run(
          [ownership = ownership.addRef()](Worker::Lock& lock, IoContext& context) mutable {
        auto& socket = KJ_REQUIRE_NONNULL(ownership->socket);
        return context.awaitJs(lock, (*socket)->close(lock));
      });
      ownership->socket = kj::none;
      co_return Result{.outcome = EventOutcome::OK};
    }
    KJ_CATCH(error) {
      markExceptionAsDelivered(error);
      kj::throwFatalException(kj::mv(error));
    }
  }
  kj::Promise<Result> sendRpc(capnp::HttpOverCapnpFactory&,
      capnp::ByteStreamFactory&,
      FrankenvalueHandler&,
      rpc::EventDispatcher::Client) override {
    JSG_FAIL_REQUIRE(Error, "Public binding callbacks are local only.");
  }
  kj::Promise<Result> notSupported() override {
    JSG_FAIL_REQUIRE(Error, "Public binding callback is unavailable.");
  }
  uint16_t getType() override {
    return 0;
  }
  tracing::EventInfo getEventInfo() const override {
    return tracing::CustomEventInfo();
  }

 private:
  kj::Own<PolicyChannelState> state;
  kj::String address;
  kj::AsyncIoStream& connection;
  kj::HttpService::ConnectResponse& response;
  const kj::HttpHeaderTable& table;
};

class PolicyWorker final: public WorkerInterface {
 public:
  PolicyWorker(kj::Own<PolicyChannelState> state, IoChannelFactory::SubrequestMetadata metadata)
      : state(kj::mv(state)),
        metadata(kj::mv(metadata)) {}
  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body,
      kj::HttpService::Response& response) override {
    state->requireLive();
    auto worker = state->loopback->getWorker(kj::mv(metadata));
    return worker
        ->customEvent(
            kj::heap<PolicyFetchEvent>(kj::addRef(*state), method, url, headers, body, response))
        .ignoreResult()
        .attach(kj::mv(worker));
  }
  kj::Promise<void> connect(kj::StringPtr address,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings) override {
    state->requireLive();
    auto worker = state->loopback->getWorker(kj::mv(metadata));
    return worker
        ->customEvent(kj::heap<PolicyConnectEvent>(kj::addRef(*state), address, connection,
            response, KJ_REQUIRE_NONNULL(state->live->tryGet()).getHeaderTable()))
        .ignoreResult()
        .attach(kj::mv(worker));
  }
  kj::Promise<void> prewarm(kj::StringPtr) override {
    return kj::READY_NOW;
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date, kj::StringPtr) override {
    JSG_FAIL_REQUIRE(Error, "Public binding callback only dispatches fetch and connect.");
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date, uint32_t) override {
    JSG_FAIL_REQUIRE(Error, "Public binding callback only dispatches fetch and connect.");
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent>) override {
    JSG_FAIL_REQUIRE(Error, "Public binding callback only dispatches fetch and connect.");
  }

 private:
  kj::Own<PolicyChannelState> state;
  IoChannelFactory::SubrequestMetadata metadata;
};

class PolicySubrequestChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  explicit PolicySubrequestChannel(kj::Own<PolicyChannelState> state): state(kj::mv(state)) {}
  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata metadata) override {
    state->requireLive();
    return kj::heap<PolicyWorker>(kj::addRef(*state), kj::mv(metadata));
  }
  void requireAllowsTransfer() override {
    JSG_FAIL_REQUIRE(DOMDataCloneError, "Public binding callback channels are local only.");
  }
  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage) override {
    JSG_FAIL_REQUIRE(DOMDataCloneError, "Public binding callback channels are local only.");
  }

 private:
  kj::Own<PolicyChannelState> state;
};

// Inherited native RPC and transport handlers must never expose the private transport.
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

jsg::JsObject createPolicyChannelCallbacks(IoContext& context, jsg::JsObject policy) {
  JSG_REQUIRE(context.getActor() != kj::none, Error,
      "Public binding interception requires a Durable Object.");
  jsg::Lock& js = context.getCurrentLock();
  auto module = KJ_REQUIRE_NONNULL(
      js.resolveInternalModule("cloudflare-internal:open-compute-host-policy"_kj),
      "Public binding host policy is unavailable");
  auto factory = KJ_REQUIRE_NONNULL(
      module.get(js, "createBindingChannelCallbacks"_kj).tryCast<jsg::JsFunction>(),
      "Public binding callback factory is unavailable");
  auto callbacks = JSG_REQUIRE_NONNULL(factory.callNoReceiver(js, policy).tryCast<jsg::JsObject>(),
      TypeError, "Public binding callback factory did not return an object.");
  return callbacks;
}

kj::Own<IoChannelFactory::SubrequestChannel> newPolicySubrequestChannel(
    IoContext& context, jsg::JsObject callbacks) {
  return kj::refcounted<PolicySubrequestChannel>(
      kj::refcounted<PolicyChannelState>(context, callbacks));
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

kj::Own<IoChannelFactory::SubrequestChannel> Fetcher::getSubrequestChannel(IoContext& context) {
  return newPolicySubrequestChannel(
      context, createPolicyChannelCallbacks(context, policy.getHandle(context.getCurrentLock())));
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
