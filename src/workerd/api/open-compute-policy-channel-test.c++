// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "open-compute-service.h"

#include <workerd/api/streams/readable.h>
#include <workerd/io/compatibility-date.h>
#include <workerd/io/frankenvalue.h>
#include <workerd/jsg/script.h>
#include <workerd/tests/test-fixture.h>

#include <strings.h>

#include <capnp/message.h>
#include <kj/test.h>

namespace workerd::api {
namespace {

// TestFixture accepts a raw reader; compile the same stable date as the maintained tenant fixture.
void compilePolicyTestFlags(
    CompatibilityFlags::Builder flags, kj::ArrayPtr<const kj::String> explicitFlags = nullptr) {
  SimpleWorkerErrorReporter errors;
  compileCompatibilityFlags("2026-09-08", explicitFlags, flags, errors, false,
      CompatibilityDateValidation::CODE_VERSION, nullptr);
  KJ_ASSERT(errors.errors.empty(), kj::strArray(errors.errors, "\n"));
  KJ_ASSERT(flags.getInternalWritableStreamAbortClearsQueue());
  KJ_ASSERT(flags.getStreamsJavaScriptControllers());
}

class TestConnectResponse final: public kj::HttpService::ConnectResponse {
 public:
  void accept(uint, kj::StringPtr, const kj::HttpHeaders&) override {
    accepted = true;
    acceptance.fulfiller->fulfill();
  }
  kj::Own<kj::AsyncOutputStream> reject(
      uint, kj::StringPtr, const kj::HttpHeaders&, kj::Maybe<uint64_t>) override {
    KJ_UNREACHABLE;
  }
  bool accepted = false;
  kj::PromiseFulfillerPair<void> acceptance = kj::newPromiseAndFulfiller<void>();
};

// Loopback invokes the real CustomEvent on a newly delivered IncomingRequest, without retaining
// an active request while the channel is idle.
struct PolicyDispatchState: kj::TaskSet::ErrorHandler {
  TestFixture* fixture = nullptr;
  IoContext* context = nullptr;
  uint calls = 0;
  kj::Maybe<kj::TaskSet&> tasks;
  void taskFailed(kj::Exception&& exception) override {
    KJ_FAIL_EXPECT(exception);
  }
};

class PolicyDispatchWorker final: public WorkerInterface {
 public:
  explicit PolicyDispatchWorker(PolicyDispatchState& state): state(state) {}
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    ++state.calls;
    return event
        ->run(state.fixture->newUndeliveredIncomingRequest(*state.context), kj::none, kj::none,
            Frankenvalue(), KJ_REQUIRE_NONNULL(state.tasks))
        .attach(kj::mv(event));
  }
  kj::Promise<void> request(kj::HttpMethod,
      kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncInputStream&,
      kj::HttpService::Response&) override {
    KJ_FAIL_REQUIRE("Unexpected public policy test dispatch.");
  }
  kj::Promise<void> connect(kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncIoStream&,
      ConnectResponse&,
      kj::HttpConnectSettings) override {
    KJ_FAIL_REQUIRE("Unexpected public policy test dispatch.");
  }
  kj::Promise<void> prewarm(kj::StringPtr) override {
    KJ_FAIL_REQUIRE("Unexpected public policy test dispatch.");
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date, kj::StringPtr) override {
    KJ_FAIL_REQUIRE("Unexpected public policy test dispatch.");
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date, uint32_t) override {
    KJ_FAIL_REQUIRE("Unexpected public policy test dispatch.");
  }

 private:
  PolicyDispatchState& state;
};

class PolicyDispatchLoopback final: public Worker::Actor::Loopback, public kj::Refcounted {
 public:
  explicit PolicyDispatchLoopback(PolicyDispatchState& state): state(state) {}
  kj::Own<WorkerInterface> getWorker(IoChannelFactory::SubrequestMetadata) override {
    return kj::heap<PolicyDispatchWorker>(state);
  }
  kj::Own<Worker::Actor::Loopback> addRef() override {
    return kj::addRef(*this);
  }

 private:
  PolicyDispatchState& state;
};

jsg::JsValue policyScript(const TestFixture::Environment& env, kj::StringPtr source) {
  return jsg::NonModuleScript::compile(env.js, source, "policy-channel-test.js"_kj)
      .runAndReturn(env.js);
}

enum class PolicyFetchReply { RESPONSE, PACKET };

kj::Own<IoChannelFactory::SubrequestChannel> policyChannel(TestFixture& fixture,
    IoContext& context,
    kj::StringPtr source,
    PolicyFetchReply reply = PolicyFetchReply::RESPONSE) {
  kj::Own<IoChannelFactory::SubrequestChannel> channel;
  auto request = fixture.newIncomingRequest(context);
  fixture.enterContext(*request, [&](const TestFixture::Environment& env) {
    auto script = reply == PolicyFetchReply::RESPONSE
        ? kj::str(
              "(callbacks => { if (callbacks.fetch) { const fetch = callbacks.fetch; callbacks.fetch = async request => ({response: await fetch(request), leaseBody: null, errorCode: null}); } return callbacks; })(",
              source, ")")
        : kj::str(source);
    auto callbacks = KJ_ASSERT_NONNULL(policyScript(env, script).tryCast<jsg::JsObject>());
    channel = open_compute::newPolicySubrequestChannel(env.context, callbacks);
  });
  fixture.drainAndDestroy(kj::mv(request));
  return channel;
}

class PolicyResponse final: public kj::HttpService::Response {
 public:
  class Output final: public kj::AsyncOutputStream {
   public:
    explicit Output(kj::Vector<char>& body): body(body) {}
    kj::Promise<void> write(kj::ArrayPtr<const byte> data) override {
      body.addAll(data.asChars());
      return kj::READY_NOW;
    }
    kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
      for (auto piece: pieces) body.addAll(piece.asChars());
      return kj::READY_NOW;
    }
    kj::Promise<void> whenWriteDisconnected() override {
      return kj::NEVER_DONE;
    }

   private:
    kj::Vector<char>& body;
  };
  kj::Own<kj::AsyncOutputStream> send(
      uint code, kj::StringPtr, const kj::HttpHeaders& headers, kj::Maybe<uint64_t>) override {
    status = code;
    contentType = kj::str(headers.get(kj::HttpHeaderId::CONTENT_TYPE).orDefault(""_kj));
    return kj::heap<Output>(body);
  }
  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders&) override {
    KJ_UNREACHABLE;
  }
  uint status = 0;
  kj::String contentType;
  kj::Vector<char> body;
};

KJ_TEST("public policy channel streams a fresh HTTP event and rejects replaced owners") {
  PolicyDispatchState state;
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  compilePolicyTestFlags(flags);
  TestFixture fixture({.featureFlags = flags.asReader(),
    .actorId = Worker::Actor::Id(kj::str("policy-channel")),
    .actorLoopback = kj::refcounted<PolicyDispatchLoopback>(state)});
  KJ_DEFER(state.tasks = kj::none);
  kj::TaskSet dispatchTasks(state);
  state.tasks = dispatchTasks;
  state.fixture = &fixture;
  auto context = fixture.newIoContext();
  state.context = context.get();
  auto channel = policyChannel(fixture, *context, R"JS(({
    async fetch(request) {
      if (request.url !== "https://public/" || request.headers.get("content-type") !== "text/plain")
        throw Error("request identity missing");
      return new Response(request.body, {status: 201, headers: {"content-type": "text/plain"}});
    }
  }))JS"_kj);
  KJ_EXPECT(state.calls == 0);
  auto client = channel->startRequest({});
  auto pipe = kj::newOneWayPipe();
  // Close EOF eagerly because joinPromises defers lazy continuations.
  auto write = pipe.out->write("streamed-body"_kj.asBytes()).then([&]() {
    pipe.out = nullptr;
  }).eagerlyEvaluate(nullptr);
  kj::HttpHeaders headers(context->getHeaderTable());
  headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "text/plain");
  PolicyResponse response;
  auto request =
      client->request(kj::HttpMethod::POST, "https://public/", headers, *pipe.in, response);
  kj::joinPromises(kj::arr(kj::mv(write), kj::mv(request))).wait(fixture.getWaitScope());
  KJ_EXPECT(response.status == 201, response.contentType == "text/plain");
  KJ_EXPECT(kj::StringPtr(response.body.asPtr().begin(), response.body.size()) == "streamed-body");
  auto replacement = fixture.newIoContext();
  state.context = replacement.get();
  kj::NullStream empty;
  auto stale = channel->startRequest({});
  KJ_EXPECT_THROW_MESSAGE("owner was replaced",
      stale->request(kj::HttpMethod::GET, "https://public/", headers, empty, response)
          .wait(fixture.getWaitScope()));
  channel = nullptr;
  client = nullptr;
  stale = nullptr;
  dispatchTasks.onEmpty().wait(fixture.getWaitScope());
  context = nullptr;
  fixture.pollEventLoop();
}

KJ_TEST(
    "public policy channel cancels late response bodies and rejects dead owners before dispatch") {
  PolicyDispatchState state;
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  kj::String explicitFlags[] = {kj::str("enable_request_signal")};
  compilePolicyTestFlags(flags, explicitFlags);
  TestFixture fixture({.featureFlags = flags.asReader(),
    .actorId = Worker::Actor::Id(kj::str("policy-cancel")),
    .actorLoopback = kj::refcounted<PolicyDispatchLoopback>(state)});
  KJ_DEFER(state.tasks = kj::none);
  kj::TaskSet dispatchTasks(state);
  state.tasks = dispatchTasks;
  state.fixture = &fixture;
  auto context = fixture.newIoContext();
  state.context = context.get();
  auto channel = policyChannel(fixture, *context, R"JS(({
    async fetch(request) {
      globalThis.entered = true;
      globalThis.aborted = false;
      request.signal.addEventListener("abort", () => { globalThis.aborted = true; });
      await new Promise(resolve => { globalThis.release = resolve; });
      return new Response(new ReadableStream({cancel() { globalThis.bodyCanceled = true; }}));
    }
  }))JS"_kj);
  auto client = channel->startRequest({});
  kj::HttpHeaders headers(context->getHeaderTable());
  kj::NullStream empty;
  PolicyResponse response;
  auto pending = client->request(kj::HttpMethod::GET, "https://public/", headers, empty, response)
                     .eagerlyEvaluate(nullptr);
  fixture.pollEventLoop();
  auto inspect = fixture.newIncomingRequest(*context);
  fixture.enterContext(*inspect, [&](const TestFixture::Environment& env) {
    KJ_EXPECT(policyScript(env, "entered"_kj).isTrue());
  });
  pending = nullptr;
  fixture.pollEventLoop();
  fixture.enterContext(
      *inspect, [&](const TestFixture::Environment& env) { policyScript(env, "release()"_kj); });
  fixture.pollEventLoop();
  fixture.enterContext(*inspect, [&](const TestFixture::Environment& env) {
    KJ_EXPECT(policyScript(env, "bodyCanceled"_kj).isTrue());
    KJ_EXPECT(policyScript(env, "aborted"_kj).isTrue());
  });
  KJ_EXPECT(response.status == 0);
  fixture.drainAndDestroy(kj::mv(inspect));
  context = nullptr;
  fixture.pollEventLoop();
  auto calls = state.calls;
  KJ_EXPECT_THROW_MESSAGE("owner is no longer running", channel->startRequest({}));
  KJ_EXPECT(state.calls == calls);
  dispatchTasks.onEmpty().wait(fixture.getWaitScope());
}

enum class PolicyTunnelEnd { HALF_CLOSE, CANCEL, OWNER_ABORT };

void policyTunnelTest(PolicyTunnelEnd end) {
  PolicyDispatchState state;
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  compilePolicyTestFlags(flags);
  TestFixture fixture({.featureFlags = flags.asReader(),
    .actorId = Worker::Actor::Id(kj::str("policy-tunnel")),
    .actorLoopback = kj::refcounted<PolicyDispatchLoopback>(state)});
  KJ_DEFER(state.tasks = kj::none);
  kj::TaskSet dispatchTasks(state);
  state.tasks = dispatchTasks;
  state.fixture = &fixture;
  auto context = fixture.newIoContext();
  state.context = context.get();
  auto socketPipe = kj::newTwoWayPipe();
  kj::Own<IoChannelFactory::SubrequestChannel> channel;
  jsg::WeakRef<Socket> originalSocket(nullptr);
  kj::Maybe<jsg::V8Ref<v8::Promise>> rawClosed;
  jsg::JsRef<jsg::JsObject> reactionState;
  auto registerRequest = fixture.newIncomingRequest(*context);
  fixture.enterContext(*registerRequest, [&](const TestFixture::Environment& env) {
    auto opened = env.js.newPromiseAndResolver<SocketInfo>();
    opened.resolver.resolve(env.js, SocketInfo{});
    auto socket = setupSocket(env.js, kj::mv(socketPipe.ends[0]), kj::none, kj::none,
        SocketOptions{.allowHalfOpen = true}, kj::heap<kj::TlsStarterCallback>(),
        SecureTransportKind::OFF, kj::none, false, kj::mv(opened));
    if (end == PolicyTunnelEnd::OWNER_ABORT) originalSocket = socket.getWeakRef(env.js);
    auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<Socket>>());
    jsg::JsObject(env.js.v8Context()->Global())
        .set(env.js, "issuedSocket"_kj, jsg::JsValue(handler.wrap(env.js, kj::mv(socket))));
    policyScript(env,
        "globalThis.socketClosed = false; issuedSocket.closed.then(() => { socketClosed = true; }, () => { socketClosed = true; });"_kj);
    auto callbacks = KJ_ASSERT_NONNULL(policyScript(env, R"JS(({
      async connect(address) {
        if (address !== "public:443") throw Error("unexpected address");
        await issuedSocket.opened;
        const socket = issuedSocket;
        globalThis.issuedSocket = undefined;
        return socket;
      }
    }))JS"_kj)
            .tryCast<jsg::JsObject>());
    channel = open_compute::newPolicySubrequestChannel(env.context, callbacks);
  });
  auto client = channel->startRequest({});
  auto tunnel = kj::newTwoWayPipe();
  kj::HttpHeaders headers(context->getHeaderTable());
  TestConnectResponse response;
  auto pending = client->connect("public:443", headers, *tunnel.ends[0], response, {})
                     .eagerlyEvaluate(nullptr);
  {
    // Await accept or failure, then release the fork so pending remains the sole cancel owner.
    auto ready = pending.fork();
    pending = ready.addBranch();
    kj::mv(response.acceptance.promise)
        .exclusiveJoin(ready.addBranch().then([]() {
      KJ_FAIL_REQUIRE("The policy tunnel completed before acceptance.");
    })).wait(fixture.getWaitScope());
  }
  KJ_EXPECT(response.accepted);
  auto inspect = fixture.newIncomingRequest(*context);
  fixture.enterContext(*inspect, [&](const TestFixture::Environment& env) {
    // The callback no longer keeps a JS reference. Only the active bridge owns the Socket.
    env.isolate->LowMemoryNotification();
    KJ_EXPECT(!policyScript(env, "socketClosed"_kj).isTrue());
    if (end == PolicyTunnelEnd::OWNER_ABORT) {
      auto socket = KJ_ASSERT_NONNULL(originalSocket.tryAddRef(env.js));
      auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<Socket>>());
      auto object = KJ_ASSERT_NONNULL(
          jsg::JsValue(handler.wrap(env.js, kj::mv(socket))).tryCast<jsg::JsObject>());
      v8::Local<v8::Value> closed = object.get(env.js, "closed"_kj);
      KJ_ASSERT(closed->IsPromise());
      rawClosed.emplace(env.js.v8Ref(closed.As<v8::Promise>()));
      KJ_EXPECT(KJ_ASSERT_NONNULL(rawClosed).getHandle(env.js)->State() == v8::Promise::kPending);
      auto sentinel = env.js.obj();
      sentinel.set(env.js, "called"_kj, env.js.boolean(false));
      reactionState = jsg::JsRef<jsg::JsObject>(env.js, sentinel);
      auto global = jsg::JsObject(env.js.v8Context()->Global());
      global.set(env.js, "closedReactionState"_kj, sentinel);
      global.set(env.js, "observedClosed"_kj, jsg::JsValue(closed));
      policyScript(env,
          "observedClosed.then(() => { closedReactionState.called = true; }, () => { closedReactionState.called = true; });"_kj);
    }
  });
  if (end == PolicyTunnelEnd::HALF_CLOSE) {
    // Close EOF eagerly because joinPromises defers lazy continuations.
    auto write = tunnel.ends[1]
                     ->write("request"_kj.asBytes())
                     .then([&]() {
      tunnel.ends[1]->shutdownWrite();
    }).eagerlyEvaluate(nullptr);
    auto exchange = socketPipe.ends[1]
                        ->readAllText()
                        .then([&](kj::String body) {
      KJ_EXPECT(body == "request");
      return socketPipe.ends[1]->write("response"_kj.asBytes()).then([&]() {
        socketPipe.ends[1]->shutdownWrite();
      });
    }).eagerlyEvaluate(nullptr);
    auto read =
        tunnel.ends[1]->readAllText().then([](kj::String body) { KJ_EXPECT(body == "response"); });
    kj::joinPromises(kj::arr(kj::mv(write), kj::mv(exchange), kj::mv(read), kj::mv(pending)))
        .wait(fixture.getWaitScope());
  } else {
    if (end == PolicyTunnelEnd::OWNER_ABORT) {

      context->abort(KJ_EXCEPTION(DISCONNECTED, "policy owner aborted"));

      auto error = KJ_ASSERT_NONNULL(
          kj::runCatchingExceptions([&]() { pending.wait(fixture.getWaitScope()); }));

      KJ_EXPECT(error.getDetail(WORKER_REQUEST_DELIVERED_DETAIL_ID) != kj::none);

      KJ_EXPECT_THROW_MESSAGE("owner is no longer running", channel->startRequest({}));

    } else {
      pending = nullptr;
    }
    fixture.pollEventLoop();

    KJ_EXPECT(socketPipe.ends[1]->readAllText().wait(fixture.getWaitScope()) == "");
  }
  fixture.pollEventLoop();

  if (end == PolicyTunnelEnd::OWNER_ABORT) {
    // Dead-owner Promise settlement remains deferred; cleanup must not execute its reactions.
    fixture.enterWorkerLock([&](Worker::Lock& lock) {
      JSG_WITHIN_CONTEXT_SCOPE(lock, lock.getContext(), [&](jsg::Lock& js) {
        jsg::DisallowJavaScriptScope noJs(js);
        KJ_EXPECT(!IoContext::hasCurrent());
        KJ_EXPECT(KJ_ASSERT_NONNULL(rawClosed).getHandle(js)->State() == v8::Promise::kPending);
        KJ_EXPECT(!reactionState.getHandle(js).get(js, "called"_kj).isTrue());
      });
    });
    bool entered = false;
    KJ_EXPECT_THROW_MESSAGE("policy owner aborted",
        context->run([&](Worker::Lock&) { entered = true; }).wait(fixture.getWaitScope()));
    KJ_EXPECT(!entered);
  } else {
    fixture.enterContext(*inspect, [&](const TestFixture::Environment& env) {
      KJ_EXPECT(policyScript(env, "socketClosed"_kj).isTrue());
    });
  }

  // Socket.closed settles actor I/O, so drain registration only after the tunnel closes.

  fixture.drainAndDestroy(kj::mv(registerRequest));

  fixture.drainAndDestroy(kj::mv(inspect));

  dispatchTasks.onEmpty().wait(fixture.getWaitScope());
}

KJ_TEST("public policy TCP channel preserves half-close and Socket ownership until real EOF") {
  policyTunnelTest(PolicyTunnelEnd::HALF_CLOSE);
}

KJ_TEST("public policy TCP channel cancellation closes both Socket directions") {
  policyTunnelTest(PolicyTunnelEnd::CANCEL);
}

KJ_TEST("public policy TCP opening cancellation closes a late Socket without accepting") {
  PolicyDispatchState state;
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  compilePolicyTestFlags(flags);
  TestFixture fixture({.featureFlags = flags.asReader(),
    .actorId = Worker::Actor::Id(kj::str("policy-late-socket")),
    .actorLoopback = kj::refcounted<PolicyDispatchLoopback>(state)});
  KJ_DEFER(state.tasks = kj::none);
  kj::TaskSet dispatchTasks(state);
  state.tasks = dispatchTasks;
  state.fixture = &fixture;
  auto context = fixture.newIoContext();
  state.context = context.get();
  auto socketPipe = kj::newTwoWayPipe();
  kj::Own<IoChannelFactory::SubrequestChannel> channel;
  kj::Promise<void> callbackEntered = nullptr;
  auto registration = fixture.newIncomingRequest(*context);
  fixture.enterContext(*registration, [&](const TestFixture::Environment& env) {
    auto opened = env.js.newPromiseAndResolver<SocketInfo>();
    opened.resolver.resolve(env.js, SocketInfo{});
    auto socket = setupSocket(env.js, kj::mv(socketPipe.ends[0]), kj::none, kj::none,
        SocketOptions{.allowHalfOpen = true}, kj::heap<kj::TlsStarterCallback>(),
        SecureTransportKind::OFF, kj::none, false, kj::mv(opened));
    auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<Socket>>());
    jsg::JsObject(env.js.v8Context()->Global())
        .set(env.js, "issuedSocket"_kj, jsg::JsValue(handler.wrap(env.js, kj::mv(socket))));
    policyScript(env, R"JS(
      globalThis.socketClosed = false;
      issuedSocket.closed.then(() => { socketClosed = true; });
      globalThis.openingReady = new Promise(resolve => { globalThis.notifyOpening = resolve; });
    )JS"_kj);
    callbackEntered = env.context.awaitJs(
        env.js, env.js.toPromise(policyScript(env, "openingReady"_kj)).whenResolved(env.js));
    auto callbacks = KJ_ASSERT_NONNULL(policyScript(env, R"JS(({
      async connect(address) {
        if (address !== "public:443") throw Error("unexpected address");
        await issuedSocket.opened;
        notifyOpening();
        await new Promise(resolve => { globalThis.releaseOpening = resolve; });
        const socket = issuedSocket;
        globalThis.issuedSocket = undefined;
        return socket;
      }
    }))JS"_kj)
            .tryCast<jsg::JsObject>());
    channel = open_compute::newPolicySubrequestChannel(env.context, callbacks);
  });
  auto client = channel->startRequest({});
  auto tunnel = kj::newTwoWayPipe();
  kj::HttpHeaders headers(context->getHeaderTable());
  TestConnectResponse response;
  auto pending = client->connect("public:443", headers, *tunnel.ends[0], response, {})
                     .eagerlyEvaluate(nullptr);
  {
    auto opening = pending.fork();
    pending = opening.addBranch();
    kj::mv(callbackEntered)
        .exclusiveJoin(opening.addBranch().then([]() {
      KJ_FAIL_REQUIRE("The policy tunnel completed before its opening callback was ready.");
    })).wait(fixture.getWaitScope());
  }
  KJ_EXPECT(!response.accepted);
  pending = nullptr;
  fixture.pollEventLoop();
  // The canonical open Socket watcher may retain its original request until real close.
  fixture.enterContext(*registration, [&](const TestFixture::Environment& env) {
    KJ_EXPECT(!policyScript(env, "socketClosed"_kj).isTrue());
    policyScript(env, "releaseOpening()"_kj);
  });
  KJ_EXPECT(socketPipe.ends[1]->readAllText().wait(fixture.getWaitScope()) == "");
  fixture.pollEventLoop();
  fixture.enterContext(*registration, [&](const TestFixture::Environment& env) {
    KJ_EXPECT(policyScript(env, "socketClosed"_kj).isTrue());
    KJ_EXPECT(policyScript(env, "issuedSocket === undefined"_kj).isTrue());
  });
  KJ_EXPECT(!response.accepted);
  fixture.drainAndDestroy(kj::mv(registration));
  dispatchTasks.onEmpty().wait(fixture.getWaitScope());
  KJ_EXPECT(!context->hasCurrentIncomingRequest());
}

KJ_TEST("public policy TCP channel rejects opening failures before accepting the tunnel") {
  PolicyDispatchState state;
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  compilePolicyTestFlags(flags);
  TestFixture fixture({.featureFlags = flags.asReader(),
    .actorId = Worker::Actor::Id(kj::str("policy-open-reject")),
    .actorLoopback = kj::refcounted<PolicyDispatchLoopback>(state)});
  KJ_DEFER(state.tasks = kj::none);
  kj::TaskSet dispatchTasks(state);
  state.tasks = dispatchTasks;
  state.fixture = &fixture;
  auto context = fixture.newIoContext();
  state.context = context.get();
  auto channel =
      policyChannel(fixture, *context, "({async connect() { throw Error('opening denied'); }})"_kj);
  auto client = channel->startRequest({});
  auto tunnel = kj::newTwoWayPipe();
  kj::HttpHeaders headers(context->getHeaderTable());
  TestConnectResponse response;
  KJ_EXPECT_THROW_MESSAGE("opening denied",
      client->connect("public:443", headers, *tunnel.ends[0], response, {})
          .wait(fixture.getWaitScope()));
  KJ_EXPECT(!response.accepted);
  dispatchTasks.onEmpty().wait(fixture.getWaitScope());
}

KJ_TEST("public policy HTTP cancellation closes late returned and accepted WebSockets") {
  for (auto accept: {""_kj, "client.accept();"_kj, "client.accept(); client.close();"_kj}) {
    PolicyDispatchState state;
    capnp::MallocMessageBuilder arena;
    auto flags = arena.initRoot<CompatibilityFlags>();
    compilePolicyTestFlags(flags);
    TestFixture fixture({.featureFlags = flags.asReader(),
      .actorId = Worker::Actor::Id(kj::str("policy-ws-cancel")),
      .actorLoopback = kj::refcounted<PolicyDispatchLoopback>(state)});
    KJ_DEFER(state.tasks = kj::none);
    kj::TaskSet dispatchTasks(state);
    state.tasks = dispatchTasks;
    state.fixture = &fixture;
    auto context = fixture.newIoContext();
    state.context = context.get();
    auto script = kj::str(R"JS(({
      async fetch() {
        await new Promise(resolve => { globalThis.release = resolve; });
        const pair = new WebSocketPair();
        globalThis.client = pair[0];
        pair[1].accept();
        globalThis.peerClosed = false;
        pair[1].addEventListener('close', () => { peerClosed = true; pair[1].close(); });
    )JS",
        accept, R"JS(
        client.close = () => { throw Error('public method must not control native cancellation'); };
        return new Response(null, {status: 101, webSocket: client});
      }
    }))JS");
    auto channel = policyChannel(fixture, *context, script);
    auto client = channel->startRequest({});
    kj::HttpHeaders headers(context->getHeaderTable());
    kj::NullStream body;
    PolicyResponse response;
    auto pending = client->request(kj::HttpMethod::GET, "https://public/", headers, body, response)
                       .eagerlyEvaluate(nullptr);
    fixture.pollEventLoop();
    pending = nullptr;
    auto inspect = fixture.newIncomingRequest(*context);
    fixture.enterContext(
        *inspect, [&](const TestFixture::Environment& env) { policyScript(env, "release()"_kj); });
    fixture.pollEventLoop();
    fixture.enterContext(*inspect, [&](const TestFixture::Environment& env) {
      KJ_EXPECT(policyScript(env, "peerClosed"_kj).isTrue());
    });
    KJ_EXPECT(response.status == 0);
    fixture.drainAndDestroy(kj::mv(inspect));
    dispatchTasks.onEmpty().wait(fixture.getWaitScope());
  }
}

KJ_TEST("public policy pending tunnel closes when its original owner aborts") {
  policyTunnelTest(PolicyTunnelEnd::OWNER_ABORT);
}

KJ_TEST("public policy pending HTTP cannot dispatch into a reconstructed owner after abort") {
  PolicyDispatchState state;
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  compilePolicyTestFlags(flags);
  TestFixture fixture({.featureFlags = flags.asReader(),
    .actorId = Worker::Actor::Id(kj::str("policy-pending-owner")),
    .actorLoopback = kj::refcounted<PolicyDispatchLoopback>(state)});
  KJ_DEFER(state.tasks = kj::none);
  kj::TaskSet dispatchTasks(state);
  state.tasks = dispatchTasks;
  state.fixture = &fixture;
  auto context = fixture.newIoContext();
  state.context = context.get();
  auto channel = policyChannel(fixture, *context,
      "({async fetch() { globalThis.entered = true; await new Promise(() => {}); }})"_kj);
  auto client = channel->startRequest({});
  kj::HttpHeaders headers(context->getHeaderTable());
  kj::NullStream body;
  PolicyResponse response;
  auto pending = client->request(kj::HttpMethod::GET, "https://public/", headers, body, response)
                     .eagerlyEvaluate(nullptr);
  fixture.pollEventLoop();
  KJ_EXPECT(state.calls == 1);
  context->abort(KJ_EXCEPTION(DISCONNECTED, "policy owner aborted"));
  auto error =
      KJ_ASSERT_NONNULL(kj::runCatchingExceptions([&]() { pending.wait(fixture.getWaitScope()); }));
  KJ_EXPECT(error.getDetail(WORKER_REQUEST_DELIVERED_DETAIL_ID) != kj::none);
  auto replacement = fixture.newIoContext();
  state.context = replacement.get();
  KJ_EXPECT_THROW_MESSAGE("owner is no longer running", channel->startRequest({}));
  KJ_EXPECT(state.calls == 1, response.status == 0);
  dispatchTasks.onEmpty().wait(fixture.getWaitScope());
}

class PolicyWebSocketResponse final: public kj::HttpService::Response {
 public:
  kj::Own<kj::AsyncOutputStream> send(
      uint, kj::StringPtr, const kj::HttpHeaders&, kj::Maybe<uint64_t>) override {
    KJ_UNREACHABLE;
  }
  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override {
    headers.forEach([](kj::StringPtr name, kj::StringPtr) {
      KJ_EXPECT(strcasecmp(name.cStr(), "x-open-compute-service-websocket-handoffs") != 0);
    });
    auto pipe = kj::newWebSocketPipe();
    peer = kj::mv(pipe.ends[1]);
    acceptance.fulfiller->fulfill();
    return kj::mv(pipe.ends[0]);
  }
  kj::Own<kj::WebSocket> peer;
  kj::PromiseFulfillerPair<void> acceptance = kj::newPromiseAndFulfiller<void>();
};

struct PolicyNativeLeaseState {
  uint created = 0;
  uint reads = 0;
  bool canceled = false;
  bool dropped = false;
};

class PolicyNativeLeaseSource final: public ReadableStreamSource {
 public:
  explicit PolicyNativeLeaseSource(kj::Rc<PolicyNativeLeaseState> state): state(kj::mv(state)) {
    this->state->created++;
  }
  ~PolicyNativeLeaseSource() noexcept(false) {
    state->dropped = true;
  }
  kj::Promise<size_t> tryRead(void*, size_t, size_t) override {
    state->reads++;
    return kj::NEVER_DONE;
  }
  void cancel(kj::Exception) override {
    state->canceled = true;
  }

 private:
  kj::Rc<PolicyNativeLeaseState> state;
};

enum class PolicyLeaseEnd { CLOSE, CANCEL, LOST, DATA, LATE_CANCEL, OWNER_ABORT, ACTIVATION_ERROR };

void policyWebSocketLeaseTest(PolicyLeaseEnd end) {
  auto nativeLeaseState = kj::rc<PolicyNativeLeaseState>();
  PolicyDispatchState state;
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  compilePolicyTestFlags(flags);
  TestFixture fixture({.featureFlags = flags.asReader(),
    .actorId = Worker::Actor::Id(kj::str("policy-ws-lease")),
    .actorLoopback = kj::refcounted<PolicyDispatchLoopback>(state)});
  KJ_DEFER(state.tasks = kj::none);
  kj::TaskSet dispatchTasks(state);
  state.tasks = dispatchTasks;
  state.fixture = &fixture;
  auto context = fixture.newIoContext();
  state.context = context.get();
  if (end == PolicyLeaseEnd::OWNER_ABORT) {
    fixture.enterWorkerLock([&](Worker::Lock& lock) {
      JSG_WITHIN_CONTEXT_SCOPE(lock, lock.getContext(), [&](jsg::Lock& js) {
        js.global().set(js, "nativeLease"_kj,
            jsg::JsValue(js.wrapReturningFunction(js.v8Context(),
                [state = nativeLeaseState.addRef()](jsg::Lock& js,
                    const v8::FunctionCallbackInfo<v8::Value>&) mutable -> v8::Local<v8::Value> {
          auto lease = js.alloc<ReadableStream>(
              IoContext::current(), kj::heap<PolicyNativeLeaseSource>(state.addRef()));
          auto& handler = KJ_REQUIRE_NONNULL(js.tryGetTypeHandler<jsg::Ref<ReadableStream>>());
          return handler.wrap(js, kj::mv(lease));
        })));
      });
    });
  }
  auto source = kj::str(R"JS(({
    async fetch() {
      const pair = new WebSocketPair();
      pair[1].accept();
      pair[1].addEventListener('message', event => pair[1].send(event.data));
      pair[1].addEventListener('close', () => pair[1].close());
      globalThis.leaseCanceled = false;
  )JS",
      end == PolicyLeaseEnd::OWNER_ABORT ? "const lease = nativeLease();"_kj
                                         : R"JS(const lease = new ReadableStream({
        start(controller) { globalThis.leaseController = controller; },
        cancel() { leaseCanceled = true; }
      });)JS"_kj,
      R"JS(
      const response = new Response(null, {status: 101, webSocket: pair[0]});
  )JS",
      end == PolicyLeaseEnd::LATE_CANCEL
          ? "globalThis.lateSocketClosed = false; pair[1].addEventListener('close', () => { lateSocketClosed = true; });"_kj
          : ""_kj,
      end == PolicyLeaseEnd::LATE_CANCEL
          ? "await new Promise(resolve => { globalThis.release = resolve; });"_kj
          : ""_kj,
      "return {response,"_kj,
      end == PolicyLeaseEnd::ACTIVATION_ERROR
          ? "leaseBody: null, errorCode: 'SERVICE_UNAVAILABLE'};"_kj
          : "leaseBody: lease, errorCode: null};"_kj,
      R"JS(
    }
  }))JS");
  auto channel = policyChannel(fixture, *context, source, PolicyFetchReply::PACKET);
  auto client = channel->startRequest({});
  kj::HttpHeaders headers(context->getHeaderTable());
  headers.setPtr(kj::HttpHeaderId::UPGRADE, "websocket");
  headers.setPtr(kj::HttpHeaderId::CONNECTION, "Upgrade");
  kj::NullStream body;
  PolicyWebSocketResponse response;
  auto pending = client->request(kj::HttpMethod::GET, "https://public/", headers, body, response)
                     .eagerlyEvaluate(nullptr);
  if (end == PolicyLeaseEnd::LATE_CANCEL) {
    fixture.pollEventLoop();
    pending = nullptr;
    fixture.pollEventLoop();
    fixture.enterWorkerLock([&](Worker::Lock& lock) {
      JSG_WITHIN_CONTEXT_SCOPE(lock, lock.getContext(), [&](jsg::Lock& js) {
        jsg::NonModuleScript::compile(js, "release()"_kj, "policy-channel-test.js"_kj)
            .runAndReturn(js);
      });
    });
    fixture.pollEventLoop();
    KJ_EXPECT(response.peer.get() == nullptr);
    // The accepted read loop holds the original request until the real WebSocket closes.
    dispatchTasks.onEmpty().wait(fixture.getWaitScope());
    KJ_EXPECT(!context->hasCurrentIncomingRequest());
    fixture.enterWorkerLock([&](Worker::Lock& lock) {
      JSG_WITHIN_CONTEXT_SCOPE(lock, lock.getContext(), [&](jsg::Lock& js) {
        KJ_EXPECT(jsg::NonModuleScript::compile(js, "leaseCanceled"_kj, "policy-channel-test.js"_kj)
                .runAndReturn(js)
                .isTrue());
        KJ_EXPECT(
            jsg::NonModuleScript::compile(js, "lateSocketClosed"_kj, "policy-channel-test.js"_kj)
                .runAndReturn(js)
                .isTrue());
      });
    });
    fixture.pollEventLoop();
    KJ_EXPECT(!context->hasCurrentIncomingRequest());
    return;
  }
  auto inspect = fixture.newIncomingRequest(*context);
  if (end == PolicyLeaseEnd::ACTIVATION_ERROR) {
    KJ_EXPECT_THROW_MESSAGE("SERVICE_UNAVAILABLE", pending.wait(fixture.getWaitScope()));
    KJ_EXPECT(response.peer.get() == nullptr);
  } else {
    if (end == PolicyLeaseEnd::OWNER_ABORT) {
      // Acceptance is asynchronous; propagate request failure instead of assuming one poll is ready.
      auto ready = pending.fork();
      pending = ready.addBranch();
      kj::mv(response.acceptance.promise)
          .exclusiveJoin(ready.addBranch().then([]() {
        KJ_FAIL_REQUIRE("The policy WebSocket request completed before acceptance.");
      })).wait(fixture.getWaitScope());
    } else {
      fixture.pollEventLoop();
    }
    KJ_REQUIRE(response.peer.get() != nullptr);
    response.peer->send("echo"_kj).wait(fixture.getWaitScope());
    auto message = response.peer->receive().wait(fixture.getWaitScope());
    KJ_EXPECT(message.is<kj::String>());
    KJ_EXPECT(message.get<kj::String>() == "echo");
    fixture.enterContext(*inspect, [&](const TestFixture::Environment& env) {
      KJ_EXPECT(!policyScript(env, "leaseCanceled"_kj).isTrue());
    });
    if (end == PolicyLeaseEnd::OWNER_ABORT) {
      KJ_EXPECT(nativeLeaseState->created == 1);
      KJ_EXPECT(nativeLeaseState->reads > 0);
      KJ_EXPECT(!nativeLeaseState->canceled, !nativeLeaseState->dropped);
    }
    if (end == PolicyLeaseEnd::CLOSE) {
      response.peer->close(1000, ""_kj).wait(fixture.getWaitScope());
      response.peer->receive().wait(fixture.getWaitScope());
      pending.wait(fixture.getWaitScope());
    } else if (end == PolicyLeaseEnd::CANCEL) {
      pending = nullptr;
    } else if (end == PolicyLeaseEnd::OWNER_ABORT) {
      context->abort(KJ_EXCEPTION(DISCONNECTED, "policy owner aborted"));
      auto error = KJ_ASSERT_NONNULL(
          kj::runCatchingExceptions([&]() { pending.wait(fixture.getWaitScope()); }));
      KJ_EXPECT(error.getDetail(WORKER_REQUEST_DELIVERED_DETAIL_ID) != kj::none);
      KJ_EXPECT_THROW_MESSAGE("owner is no longer running", channel->startRequest({}));
      KJ_EXPECT_THROW_RECOVERABLE(
          DISCONNECTED, response.peer->receive().wait(fixture.getWaitScope()));
      bool entered = false;
      KJ_EXPECT_THROW_MESSAGE("policy owner aborted",
          context->run([&](Worker::Lock&) { entered = true; }).wait(fixture.getWaitScope()));
      KJ_EXPECT(!entered);
    } else {
      fixture.enterContext(*inspect, [&](const TestFixture::Environment& env) {
        policyScript(env,
            end == PolicyLeaseEnd::DATA ? "leaseController.enqueue(new Uint8Array([1]))"_kj
                                        : "leaseController.close()"_kj);
      });
      KJ_EXPECT_THROW_MESSAGE("WebSocket owner disconnected", pending.wait(fixture.getWaitScope()));
    }
    fixture.pollEventLoop();
    if (end != PolicyLeaseEnd::LOST && end != PolicyLeaseEnd::DATA &&
        end != PolicyLeaseEnd::OWNER_ABORT)
      fixture.enterContext(*inspect, [&](const TestFixture::Environment& env) {
        KJ_EXPECT(policyScript(env, "leaseCanceled"_kj).isTrue());
      });
  }
  response.peer = nullptr;
  fixture.drainAndDestroy(kj::mv(inspect));
  dispatchTasks.onEmpty().wait(fixture.getWaitScope());
  if (end == PolicyLeaseEnd::OWNER_ABORT) {
    // The dead owner's JS reactions stay deferred. Its real body owner drops at context teardown.
    client = nullptr;
    channel = nullptr;
    state.context = nullptr;
    context = nullptr;
    fixture.pollEventLoop();
    KJ_EXPECT(nativeLeaseState->dropped);
  }
}

KJ_TEST("public policy WebSocket channel holds its private lease through real exchange and close") {
  policyWebSocketLeaseTest(PolicyLeaseEnd::CLOSE);
}
KJ_TEST("public policy WebSocket channel cancellation releases its private lease") {
  policyWebSocketLeaseTest(PolicyLeaseEnd::CANCEL);
}
KJ_TEST("public policy WebSocket channel aborts when its private lease ends unexpectedly") {
  policyWebSocketLeaseTest(PolicyLeaseEnd::LOST);
}
KJ_TEST("public policy WebSocket owner abort releases an acquired lease and rejects reuse") {
  policyWebSocketLeaseTest(PolicyLeaseEnd::OWNER_ABORT);
}
KJ_TEST("public policy WebSocket channel rejects unexpected private ownership bytes") {
  policyWebSocketLeaseTest(PolicyLeaseEnd::DATA);
}
KJ_TEST("public policy WebSocket late canceled Response releases its acquired ownership") {
  policyWebSocketLeaseTest(PolicyLeaseEnd::LATE_CANCEL);
}
KJ_TEST(
    "public policy WebSocket activation failure closes the acquired Response without sending it") {
  policyWebSocketLeaseTest(PolicyLeaseEnd::ACTIVATION_ERROR);
}

}  // namespace
}  // namespace workerd::api
