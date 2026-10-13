#include "global-scope.h"
#include "sockets.h"
#include "wrapped-binding.h"

#include <workerd/io/compatibility-date.h>
#include <workerd/io/io-context.h>
#include <workerd/io/worker-interface.h>
#include <workerd/io/worker.h>
#include <workerd/jsg/script.h>
#include <workerd/tests/test-fixture.h>

#include <capnp/message.h>
#include <kj/test.h>

namespace workerd::api {
namespace {

// Minimal WorkerInterface that tracks when connect() is called and exposes the pipe.
class MockConnectWorkerInterface final: public WorkerInterface {
 public:
  MockConnectWorkerInterface(
      bool& connectCalled, kj::HttpHeaderTable& headerTable, kj::Maybe<kj::AsyncIoStream&>& pipeEnd)
      : connectCalled(connectCalled),
        headerTable(headerTable),
        pipeEnd(pipeEnd) {}

  kj::Promise<void> connect(kj::StringPtr host,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings settings) override {
    connectCalled = true;
    pipeEnd = connection;
    kj::HttpHeaders responseHeaders(headerTable);
    response.accept(200, "OK"_kj, responseHeaders);
    return kj::NEVER_DONE;
  }

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& response) override {
    KJ_UNIMPLEMENTED("not used in this test");
  }
  kj::Promise<void> prewarm(kj::StringPtr url) override {
    KJ_UNIMPLEMENTED("not used in this test");
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date scheduledTime, kj::StringPtr cron) override {
    KJ_UNIMPLEMENTED("not used in this test");
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date scheduledTime, uint32_t retryCount) override {
    KJ_UNIMPLEMENTED("not used in this test");
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return event->notSupported();
  }

 private:
  bool& connectCalled;
  kj::HttpHeaderTable& headerTable;
  kj::Maybe<kj::AsyncIoStream&>& pipeEnd;
};

struct ConnectTestIoChannelFactory final: public TestFixture::DummyIoChannelFactory {
  ConnectTestIoChannelFactory(TimerChannel& timer,
      bool& connectCalled,
      kj::HttpHeaderTable& headerTable,
      kj::Maybe<kj::AsyncIoStream&>& pipeEnd)
      : DummyIoChannelFactory(timer),
        connectCalled(connectCalled),
        headerTable(headerTable),
        pipeEnd(pipeEnd) {}

  kj::Own<WorkerInterface> startSubrequest(uint channel, SubrequestMetadata metadata) override {
    return kj::heap<MockConnectWorkerInterface>(connectCalled, headerTable, pipeEnd);
  }

  void abortIsolate(kj::StringPtr reason) override {
    JSG_FAIL_REQUIRE(Error, "abortIsolate() is not implemented for this runtime.");
  }

  bool& connectCalled;
  kj::HttpHeaderTable& headerTable;
  kj::Maybe<kj::AsyncIoStream&>& pipeEnd;
};

// Turn-based timeout: resolves after n event loop turns, returning 0.
kj::Promise<size_t> turnTimeout(int n) {
  for (int i = 0; i < n; i++) {
    co_await kj::evalLater([]() {});
  }
  co_return 0;
}

struct AsyncSendState {
  bool called = false;
  kj::Array<kj::byte> observed;
};

class AsyncObservingDatagramChannel final: public DatagramChannel {
 public:
  explicit AsyncObservingDatagramChannel(AsyncSendState& state): state(state) {}

  kj::Promise<kj::Maybe<kj::Array<kj::byte>>> receive() override {
    return kj::Promise<kj::Maybe<kj::Array<kj::byte>>>(kj::NEVER_DONE);
  }

  kj::Promise<void> send(kj::ArrayPtr<const kj::byte> datagram) override {
    state.called = true;
    return kj::evalLater(
        [state = &state, datagram]() { state->observed = kj::heapArray<kj::byte>(datagram); });
  }

 private:
  AsyncSendState& state;
};

bool socketHasProtocolProperty(CompatibilityFlags::Reader flags) {
  AsyncSendState state;
  TestFixture fixture(TestFixture::SetupParams{.featureFlags = flags});
  bool result = false;

  fixture.runInIoContext([&](const TestFixture::Environment& env) {
    auto socket = setupDatagramSocket(
        env.js, kj::heap<AsyncObservingDatagramChannel>(state), kj::none, kj::none);
    auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<Socket>>());
    auto object = KJ_ASSERT_NONNULL(
        jsg::JsValue(handler.wrap(env.js, kj::mv(socket))).tryCast<jsg::JsObject>());
    result = object.has(env.js, "protocol"_kj);
  });

  return result;
}

KJ_TEST("Socket protocol property requires experimental flag") {
  capnp::MallocMessageBuilder disabledFlagsMessage;
  auto disabledFlags = disabledFlagsMessage.initRoot<CompatibilityFlags>();
  disabledFlags.setStreamsJavaScriptControllers(true);
  KJ_EXPECT(!socketHasProtocolProperty(disabledFlags.asReader()));

  capnp::MallocMessageBuilder enabledFlagsMessage;
  auto enabledFlags = enabledFlagsMessage.initRoot<CompatibilityFlags>();
  enabledFlags.setStreamsJavaScriptControllers(true);
  enabledFlags.setWorkerdExperimental(true);
  KJ_EXPECT(socketHasProtocolProperty(enabledFlags.asReader()));
}

KJ_TEST("UDP writable stream snapshots bytes before asynchronous send") {
  capnp::MallocMessageBuilder flagsMessage;
  auto flags = flagsMessage.initRoot<CompatibilityFlags>();
  flags.setStreamsJavaScriptControllers(true);
  TestFixture fixture(TestFixture::SetupParams{.featureFlags = flags.asReader()});
  AsyncSendState state;

  fixture.runInIoContext([&](const TestFixture::Environment& env) -> kj::Promise<void> {
    auto socket = setupDatagramSocket(
        env.js, kj::heap<AsyncObservingDatagramChannel>(state), kj::none, kj::none);

    auto data = jsg::JsUint8Array::create(env.js, "before"_kjb);
    auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<Datagram>>());
    auto chunk = jsg::JsValue(handler.wrap(env.js, env.js.alloc<Datagram>(env.js, data)));
    auto writePromise = socket->getWritable(env.js).writeForTest(env.js, chunk);
    env.js.runMicrotasks();
    KJ_REQUIRE(state.called);

    data.asArrayPtr().copyFrom("after!"_kjb);
    return env.context.awaitJs(env.js, kj::mv(writePromise));
  });

  KJ_EXPECT(state.observed.asPtr() == "before"_kjb);
}

// The output-gate write test body, run against both stream backends: with useTsStreams
// the typescript_implemented_streams compat flag (plus the bootstrap autogate) is enabled
// and the socket's streams are TypeScript-implemented.
void runSocketWriteOutputGateTest(bool useTsStreams) {
  bool connectCalled = false;
  kj::HttpHeaderTable headerTable;
  kj::Maybe<kj::AsyncIoStream&> pipeEnd;

  capnp::MallocMessageBuilder flagsMessage;
  auto flags = flagsMessage.initRoot<CompatibilityFlags>();
  flags.setTypeScriptImplementedStreams(useTsStreams);

  Worker::Actor::Id actorId = kj::str("test-actor-write");
  TestFixture fixture(TestFixture::SetupParams{
    .featureFlags = flags.asReader(),
    .autogates = useTsStreams ? kj::Maybe(kj::arr("per-isolate-javascript-bootstrap"_kj))
                              : kj::Maybe<kj::Array<kj::StringPtr>>(kj::none),
    .actorId = kj::mv(actorId),
    .useRealTimers = false,
    .ioChannelFactory = kj::Function<kj::Rc<IoChannelFactory>(TimerChannel&)>(
        [&](TimerChannel& timer) -> kj::Rc<IoChannelFactory> {
    return kj::rc<ConnectTestIoChannelFactory>(timer, connectCalled, headerTable, pipeEnd);
  }),
  });

  static constexpr kj::StringPtr errorsToIgnore[] = {
    "failed to invoke drain()"_kj,
    "no subrequests"_kj,
  };

  fixture.runInIoContext(kj::Function<kj::Promise<void>(const TestFixture::Environment&)>(
                             [&](const TestFixture::Environment& env) -> kj::Promise<void> {
    auto& actor = env.context.getActorOrThrow();

    // Step 1: Connect before locking the gate so the pipe is established.
    auto socket = connectImpl(env.js, kj::none, kj::str("localhost:1234"), kj::none);
    env.js.runMicrotasks();

    // Prepare write data and lock gate BEFORE any co_await (Worker lock still held).
    auto paf = kj::newPromiseAndFulfiller<void>();
    auto blocker = actor.getOutputGate().lockWhile(kj::mv(paf.promise), nullptr);
    jsg::JsValue jsBuffer = jsg::JsUint8Array::create(env.js, "hi"_kjb);
    socket->getWritable(env.js).writeForTest(env.js, jsBuffer).markAsHandled(env.js);

    // Connect can be deferred by other pending output locks. Wait for it.
    // After co_await, Worker lock is released -- no V8 calls allowed.
    for (int i = 0; i < 10 && pipeEnd == kj::none; i++) {
      co_await kj::evalLater([]() {});
    }
    KJ_ASSERT(connectCalled);
    auto& pipe = KJ_ASSERT_NONNULL(pipeEnd);

    // Step 4: Race tryRead against a turn-based timeout. The output gate is locked,
    // so the write drain is stuck on outputLock — data cannot reach the pipe.
    auto buf = kj::heapArray<kj::byte>(2);
    auto bytesRead =
        co_await pipe.tryRead(buf.begin(), 1, buf.size()).exclusiveJoin(turnTimeout(20));
    KJ_EXPECT(bytesRead == 0, "read must time out while output gate is locked");

    // Step 5: Release the gate.
    paf.fulfiller->fulfill();

    // Step 6: Read again — data should arrive now.
    bytesRead = co_await pipe.tryRead(buf.begin(), 1, buf.size());
    KJ_EXPECT(bytesRead == 2, "read must succeed after output gate releases");
    KJ_EXPECT(buf[0] == 'h');
    KJ_EXPECT(buf[1] == 'i');
  }),
      errorsToIgnore);
}

KJ_TEST("socket writes are blocked by output gate") {
  runSocketWriteOutputGateTest(false);
}

KJ_TEST("socket writes are blocked by output gate (TypeScript streams)") {
  runSocketWriteOutputGateTest(true);
}

enum class SocketCloseStreams { NATIVE, TYPESCRIPT };

void socketCloseErroredWriterTest(SocketCloseStreams implementation) {
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  flags.setStreamsJavaScriptControllers(true);
  flags.setTypeScriptImplementedStreams(implementation == SocketCloseStreams::TYPESCRIPT);
  TestFixture fixture({.featureFlags = flags.asReader(),
    .autogates = implementation == SocketCloseStreams::TYPESCRIPT
        ? kj::Maybe(kj::arr("per-isolate-javascript-bootstrap"_kj))
        : kj::Maybe<kj::Array<kj::StringPtr>>(kj::none)});
  auto request = fixture.newIncomingRequest();
  auto pipe = kj::newTwoWayPipe();
  kj::Promise<void> aborted = nullptr;
  fixture.enterContext(*request, [&](const TestFixture::Environment& env) {
    auto opened = env.js.newPromiseAndResolver<SocketInfo>();
    opened.resolver.resolve(env.js, SocketInfo{});
    auto socket = setupSocket(env.js, kj::mv(pipe.ends[0]), kj::none, kj::none,
        SocketOptions{.allowHalfOpen = true}, kj::heap<kj::TlsStarterCallback>(),
        SecureTransportKind::OFF, kj::none, false, kj::mv(opened));
    auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<Socket>>());
    jsg::JsObject(env.js.v8Context()->Global())
        .set(env.js, "socket"_kj, jsg::JsValue(handler.wrap(env.js, kj::mv(socket))));
    auto started = jsg::NonModuleScript::compile(env.js, R"JS((async () => {
      await socket.opened;
      globalThis.closeReason = new Error("socket writer aborted");
      globalThis.closedState = "pending";
      globalThis.closedMessage = "";
      globalThis.closedDone = socket.closed.then(() => { closedState = "resolved"; }, error => {
        closedState = "rejected";
        closedMessage = error.message;
      });
      globalThis.readPipeEnded = false;
      const pipe = socket.readable.pipeTo(new WritableStream({write() {}}));
      globalThis.readPipeDone = pipe.then(() => { readPipeEnded = true; }, () => { readPipeEnded = true; });
      await socket.writable.abort(closeReason);
    })())JS"_kj,
        "socket-close-error-test.js"_kj)
                       .runAndReturn(env.js);
    aborted = env.context.awaitJs(env.js, env.js.toPromise(started).whenResolved(env.js));
  });
  aborted.wait(fixture.getWaitScope());
  kj::Promise<void> close = nullptr;
  fixture.enterContext(*request, [&](const TestFixture::Environment& env) {
    auto script = [&](kj::StringPtr source) {
      return jsg::NonModuleScript::compile(env.js, source, "socket-close-error-test.js"_kj)
          .runAndReturn(env.js);
    };
    KJ_EXPECT(script("socket.readable.locked"_kj).isTrue());
    KJ_EXPECT(!script("readPipeEnded"_kj).isTrue());
    KJ_EXPECT(script("closedState === 'pending'"_kj).isTrue());
    close = env.context.awaitJs(env.js,
        env.js
            .toPromise(script(R"JS((async () => {
      await socket.close();
      await readPipeDone;
      await closedDone;
      if (!readPipeEnded) throw Error("Socket close did not terminate the read pipe");
      if (closedState !== "rejected") throw Error("Socket.closed did not preserve rejection");
      if (closedMessage !== closeReason.message) throw Error("Socket.closed changed the original error");
    })())JS"_kj))
            .whenResolved(env.js));
  });
  close.wait(fixture.getWaitScope());
  KJ_EXPECT(pipe.ends[1]->readAllText().wait(fixture.getWaitScope()) == "");
  fixture.drainAndDestroy(kj::mv(request));
}

KJ_TEST("Socket close releases active read pipes after writable errors") {
  socketCloseErroredWriterTest(SocketCloseStreams::NATIVE);
}

KJ_TEST("Socket close releases active read pipes after writable errors (TypeScript streams)") {
  socketCloseErroredWriterTest(SocketCloseStreams::TYPESCRIPT);
}

// Connect deferral test runs last -- its drain errors fire during process exit.
KJ_TEST("connectImpl defers connect until output gate clears") {
  bool connectCalled = false;
  kj::HttpHeaderTable headerTable;
  kj::Maybe<kj::AsyncIoStream&> pipeEnd;

  Worker::Actor::Id actorId = kj::str("test-actor");
  TestFixture fixture(TestFixture::SetupParams{
    .actorId = kj::mv(actorId),
    .useRealTimers = false,
    .ioChannelFactory = kj::Function<kj::Rc<IoChannelFactory>(TimerChannel&)>(
        [&](TimerChannel& timer) -> kj::Rc<IoChannelFactory> {
    return kj::rc<ConnectTestIoChannelFactory>(timer, connectCalled, headerTable, pipeEnd);
  }),
  });

  static constexpr kj::StringPtr errorsToIgnore[] = {
    "failed to invoke drain()"_kj,
    "no subrequests"_kj,
  };

  fixture.runInIoContext(kj::Function<kj::Promise<void>(const TestFixture::Environment&)>(
                             [&](const TestFixture::Environment& env) -> kj::Promise<void> {
    auto& actor = env.context.getActorOrThrow();
    auto paf = kj::newPromiseAndFulfiller<void>();
    auto blocker = actor.getOutputGate().lockWhile(kj::mv(paf.promise), nullptr);

    auto socket = connectImpl(env.js, kj::none, kj::str("localhost:1234"), kj::none);

    co_await kj::evalLater([]() {});
    KJ_EXPECT(!connectCalled, "connect must not happen while output gate is locked");
    paf.fulfiller->fulfill();
    co_await kj::evalLater([]() {});
    KJ_EXPECT(connectCalled, "connect must happen after output gate releases");
  }),
      errorsToIgnore);
}

enum class SocketAbortTransport { TCP, STARTTLS };

void socketNativeAbortTest(SocketAbortTransport transport) {
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  SimpleWorkerErrorReporter errors;
  compileCompatibilityFlags("2026-09-08", nullptr, flags, errors, false,
      CompatibilityDateValidation::CODE_VERSION, nullptr);
  KJ_ASSERT(errors.errors.empty(), kj::strArray(errors.errors, "\n"));
  TestFixture fixture({.featureFlags = flags.asReader(),
    .actorId = Worker::Actor::Id(kj::str("socket-native-abort"))});
  auto context = fixture.newIoContext();
  auto request = fixture.newIncomingRequest(*context);
  auto pipe = kj::newTwoWayPipe();
  bool tlsReleased = false;
  bool connectionReleased = false;
  auto handshake = kj::newPromiseAndFulfiller<void>();
  kj::Own<void> abortHandle;
  fixture.enterContext(*request, [&](const TestFixture::Environment& env) {
    auto starter = kj::heap<kj::TlsStarterCallback>(kj::Function<kj::Promise<void>(kj::StringPtr)>(
        [marker = kj::defer([&]() { tlsReleased = true; }),
            fulfiller = kj::mv(handshake.fulfiller)](kj::StringPtr) mutable -> kj::Promise<void> {
      fulfiller->fulfill();
      return kj::NEVER_DONE;
    }));
    auto connection = kj::mv(pipe.ends[0]).attach(kj::defer([&]() {
      KJ_EXPECT(tlsReleased);
      connectionReleased = true;
    }));
    auto opened = env.js.newPromiseAndResolver<SocketInfo>();
    opened.resolver.resolve(env.js, SocketInfo{});
    auto socket = setupSocket(env.js, kj::mv(connection), kj::none, kj::none,
        SocketOptions{.allowHalfOpen = true}, kj::mv(starter),
        transport == SocketAbortTransport::STARTTLS ? SecureTransportKind::STARTTLS
                                                    : SecureTransportKind::OFF,
        kj::str("socket.invalid"), false, kj::mv(opened));
    abortHandle = socket->getConnectionAbortHandle(env.js);
    auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<Socket>>());
    auto object = KJ_ASSERT_NONNULL(
        jsg::JsValue(handler.wrap(env.js, socket.addRef())).tryCast<jsg::JsObject>());
    KJ_EXPECT(!object.has(env.js, "getConnectionAbortHandle"_kj));
    auto global = jsg::JsObject(env.js.v8Context()->Global());
    global.set(env.js, "retainedSocket"_kj, object);
    if (transport == SocketAbortTransport::STARTTLS) {
      global.set(env.js, "retainedSecureSocket"_kj,
          jsg::JsValue(handler.wrap(env.js, socket->startTls(env.js, kj::none))));
    }
  });
  if (transport == SocketAbortTransport::STARTTLS) {
    handshake.promise.wait(fixture.getWaitScope());
  }
  KJ_EXPECT(!tlsReleased);
  KJ_EXPECT(!connectionReleased);
  context->abort(KJ_EXCEPTION(DISCONNECTED, "socket owner aborted"));
  abortHandle = nullptr;
  KJ_EXPECT(tlsReleased);
  KJ_EXPECT(connectionReleased);
  KJ_EXPECT(pipe.ends[1]->readAllText().wait(fixture.getWaitScope()) == "");
  bool entered = false;
  KJ_EXPECT_THROW_MESSAGE("socket owner aborted",
      context->run([&](Worker::Lock&) { entered = true; }).wait(fixture.getWaitScope()));
  KJ_EXPECT(!entered);
  fixture.drainAndDestroy(kj::mv(request));
}

KJ_TEST("Socket native cancellation releases retained physical stream ownership after abort") {
  socketNativeAbortTest(SocketAbortTransport::TCP);
}

KJ_TEST("Socket native cancellation releases a pending TLS starter before its connection") {
  socketNativeAbortTest(SocketAbortTransport::STARTTLS);
}

KJ_TEST("Socket native cancellation preserves UDP admission and releases the channel") {
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  SimpleWorkerErrorReporter errors;
  compileCompatibilityFlags("2026-09-08", nullptr, flags, errors, false,
      CompatibilityDateValidation::CODE_VERSION, nullptr);
  KJ_ASSERT(errors.errors.empty(), kj::strArray(errors.errors, "\n"));
  TestFixture fixture({.featureFlags = flags.asReader(),
    .actorId = Worker::Actor::Id(kj::str("socket-datagram-abort"))});
  auto context = fixture.newIoContext();
  auto request = fixture.newIncomingRequest(*context);
  AsyncSendState send;
  bool released = false;
  kj::Own<void> abortHandle;
  fixture.enterContext(*request, [&](const TestFixture::Environment& env) {
    kj::Own<DatagramChannel> channel =
        kj::heap<AsyncObservingDatagramChannel>(send).attach(kj::defer([&]() { released = true; }));
    auto socket = setupDatagramSocket(env.js, kj::mv(channel), kj::none, kj::none);
    abortHandle = socket->getConnectionAbortHandle(env.js);
    auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<Socket>>());
    jsg::JsObject(env.js.v8Context()->Global())
        .set(env.js, "retainedDatagramSocket"_kj,
            jsg::JsValue(handler.wrap(env.js, kj::mv(socket))));
  });
  KJ_EXPECT(!released);
  context->abort(KJ_EXCEPTION(DISCONNECTED, "socket owner aborted"));
  abortHandle = nullptr;
  KJ_EXPECT(released);
  fixture.drainAndDestroy(kj::mv(request));
}

KJ_TEST("INTERNAL binding dispatch rejects control copies and public policy receivers") {
  capnp::MallocMessageBuilder flagsMessage;
  auto flags = flagsMessage.initRoot<CompatibilityFlags>();
  SimpleWorkerErrorReporter errors;
  compileCompatibilityFlags("2026-09-08", nullptr, flags, errors, false,
      CompatibilityDateValidation::CODE_VERSION, nullptr);
  KJ_ASSERT(errors.errors.empty(), kj::strArray(errors.errors, "\n"));
  TestFixture fixture({.featureFlags = flags.asReader()});
  fixture.runInIoContext([&](const TestFixture::Environment& env) {
    auto module = env.js.alloc<WrappedBindingModule>();
    auto raw = env.js.alloc<Fetcher>(0, Fetcher::RequiresHostAndProtocol::YES, true);
    module->validatePublicTransport(raw.addRef());
    auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<Fetcher>>());
    auto rawObject = KJ_ASSERT_NONNULL(
        jsg::JsValue(handler.wrap(env.js, raw.addRef())).tryCast<jsg::JsObject>());
    jsg::JsObject(env.js.v8Context()->Global()).set(env.js, "originalTransport"_kj, rawObject);
    jsg::NonModuleScript::compile(env.js, R"JS(
      globalThis.prototypeHits = 0;
      for (const name of ['fetch', 'connect', 'root', 'rpc', 'get', 'startRpc']) {
        Object.defineProperty(Object.getPrototypeOf(originalTransport), name, {
          configurable: true,
          get() { prototypeHits++; throw Error('private receiver reached a public getter'); }
        });
      }
    )JS",
        "canonical-binding-prototype-test.js"_kj)
        .run(env.js);
    KJ_EXPECT(module->rpcTransportKind(env.js, rawObject) == "fetcher"_kj);
    auto root = module->getRpcTransportMember(env.js, rawObject, kj::str("root"), kj::str("stub"));
    KJ_EXPECT(module->rpcTransportKind(env.js, root) == "property"_kj);
    KJ_EXPECT(jsg::JsObject(env.js.v8Context()->Global())
                  .get(env.js, "prototypeHits"_kj)
                  .toString(env.js) == "0"_kj);
    auto control = module->createPrivateTransport(env.js, raw.addRef());
    auto reject = [&](jsg::Ref<Fetcher> receiver) {
      bool rejected = false;
      env.js.tryCatch(
          [&]() { module->validatePublicTransport(kj::mv(receiver)); }, [&](jsg::Value error) {
        rejected = true;
        KJ_EXPECT(env.js.exceptionToKj(kj::mv(error))
                .getDescription()
                .contains("SERVICE_BINDING_DENIED"));
      });
      KJ_EXPECT(rejected);
    };
    reject(control.addRef());
    auto controlObject = KJ_ASSERT_NONNULL(
        jsg::JsValue(handler.wrap(env.js, control.addRef())).tryCast<jsg::JsObject>());
    KJ_EXPECT(module->rpcTransportKind(env.js, controlObject) == "fetcher"_kj);
    KJ_EXPECT(module->rpcTransportKind(env.js,
                  module->getRpcTransportMember(
                      env.js, controlObject, kj::str("root"), kj::str("stub"))) == "property"_kj);
    reject(env.js.alloc<open_compute::Fetcher>(env.js, raw.addRef(), env.js.obj()));
    reject(env.js.alloc<open_compute::durable_objects::DurableObject>(env.js,
        module->createDurableObjectId(env.js,
            kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), kj::none,
            kj::none),
        raw.addRef(), env.js.obj()));
  });
}

struct InternalRpcDisposalState {
  bool released = false;
};
class InternalRpcDisposalTarget final: public rpc::JsRpcTarget::Server {
 public:
  explicit InternalRpcDisposalTarget(InternalRpcDisposalState& state): state(state) {}
  ~InternalRpcDisposalTarget() noexcept(false) {
    state.released = true;
  }

 private:
  InternalRpcDisposalState& state;
};

KJ_TEST("INTERNAL pending RPC disposal releases the native holder without a Symbol getter") {
  InternalRpcDisposalState state;
  TestFixture fixture;
  fixture.runInIoContext([&](const TestFixture::Environment& env) {
    auto module = env.js.alloc<WrappedBindingModule>();
    auto weak = kj::atomicRefcounted<JsRpcPromise::WeakRef>();
    auto observation = kj::atomicAddRef(*weak);
    weak->resultKind = JsRpcResultKind::PRIVATE_STUB;
    auto resolver = jsg::check(v8::Promise::Resolver::New(env.js.v8Context()));
    auto broken = capnp::newBrokenPipeline(KJ_EXCEPTION(FAILED, "unused test pipeline"));
    auto pipeline =
        rpc::JsRpcTarget::CallResults::Pipeline(capnp::AnyPointer::Pipeline(kj::mv(broken)));
    auto promise = env.js.alloc<JsRpcPromise>(
        jsg::JsRef<jsg::JsPromise>(env.js, jsg::JsPromise(resolver->GetPromise())), kj::mv(weak),
        env.context.addObject(kj::heap(kj::mv(pipeline))), kj::none, kj::none, kj::none, kj::none);
    auto& promiseHandler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<JsRpcPromise>>());
    module->disposeRpcTransport(env.js,
        KJ_ASSERT_NONNULL(
            jsg::JsValue(promiseHandler.wrap(env.js, promise.addRef())).tryCast<jsg::JsObject>()));
    KJ_EXPECT(observation->disposed);
    KJ_EXPECT(observation->disposeNativeResult);
    rpc::JsRpcTarget::Client client(kj::heap<InternalRpcDisposalTarget>(state));
    auto holder =
        env.js.alloc<JsRpcStub>(env.context.addObject(kj::heap(kj::mv(client))), kj::none);
    auto& stubHandler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<JsRpcStub>>());
    auto holderObject = KJ_ASSERT_NONNULL(
        jsg::JsValue(stubHandler.wrap(env.js, holder.addRef())).tryCast<jsg::JsObject>());
    jsg::JsObject(env.js.v8Context()->Global()).set(env.js, "nativeHolder"_kj, holderObject);
    jsg::NonModuleScript::compile(env.js, R"JS(
      globalThis.disposeGetterHits = 0;
      Object.defineProperty(Object.getPrototypeOf(nativeHolder), Symbol.dispose, {
        configurable: true,
        get() { disposeGetterHits++; throw Error('native disposal must not read public Symbol'); }
      });
    )JS",
        "canonical-binding-disposal-test.js"_kj)
        .run(env.js);
    promise->resolve(env.js, holderObject);
    KJ_EXPECT(state.released);
    KJ_EXPECT(jsg::JsObject(env.js.v8Context()->Global())
                  .get(env.js, "disposeGetterHits"_kj)
                  .toString(env.js) == "0"_kj);
  });
}

void internalPendingStreamDisposalTest(SocketCloseStreams implementation) {
  capnp::MallocMessageBuilder arena;
  auto flags = arena.initRoot<CompatibilityFlags>();
  SimpleWorkerErrorReporter errors;
  compileCompatibilityFlags("2026-09-08", nullptr, flags, errors, false,
      CompatibilityDateValidation::CODE_VERSION, nullptr);
  KJ_ASSERT(errors.errors.empty(), kj::strArray(errors.errors, "\n"));
  flags.setTypeScriptImplementedStreams(implementation == SocketCloseStreams::TYPESCRIPT);
  TestFixture fixture({.featureFlags = flags.asReader(),
    .autogates = implementation == SocketCloseStreams::TYPESCRIPT
        ? kj::Maybe(kj::arr("per-isolate-javascript-bootstrap"_kj))
        : kj::Maybe<kj::Array<kj::StringPtr>>(kj::none)});
  fixture.runInIoContext([&](const TestFixture::Environment& env) {
    auto module = env.js.alloc<WrappedBindingModule>();
    auto weak = kj::atomicRefcounted<JsRpcPromise::WeakRef>();
    weak->resultKind = JsRpcResultKind::PRIVATE_STREAM;
    auto resolver = jsg::check(v8::Promise::Resolver::New(env.js.v8Context()));
    auto broken = capnp::newBrokenPipeline(KJ_EXCEPTION(FAILED, "unused stream pipeline"));
    auto pipeline =
        rpc::JsRpcTarget::CallResults::Pipeline(capnp::AnyPointer::Pipeline(kj::mv(broken)));
    auto promise = env.js.alloc<JsRpcPromise>(
        jsg::JsRef<jsg::JsPromise>(env.js, jsg::JsPromise(resolver->GetPromise())), kj::mv(weak),
        env.context.addObject(kj::heap(kj::mv(pipeline))), kj::none, kj::none, kj::none, kj::none);
    auto& handler = KJ_ASSERT_NONNULL(env.js.tryGetTypeHandler<jsg::Ref<JsRpcPromise>>());
    module->disposeRpcTransport(env.js,
        KJ_ASSERT_NONNULL(
            jsg::JsValue(handler.wrap(env.js, promise.addRef())).tryCast<jsg::JsObject>()));
    auto body = jsg::NonModuleScript::compile(env.js, R"JS(
      globalThis.leaseCanceled = 0;
      globalThis.nativeLeaseBody = new ReadableStream({ cancel() { leaseCanceled++; } });
      globalThis.streamThenGetterHits = 0;
      Object.defineProperty(Object.getPrototypeOf(nativeLeaseBody), 'then', {
        configurable: true,
        get() { streamThenGetterHits++; throw Error('private stream reached a public getter'); }
      });
      nativeLeaseBody;
    )JS",
        "canonical-private-stream-disposal.js"_kj)
                    .runAndReturn(env.js);
    promise->resolve(env.js, body);
    auto protectedBody = jsg::NonModuleScript::compile(env.js, R"JS((() => {
      const then = Object.getOwnPropertyDescriptor(nativeLeaseBody, 'then');
      return streamThenGetterHits === 0 && then && then.value === undefined &&
          !then.writable && !then.configurable && !then.enumerable;
    })())JS",
        "canonical-private-stream-protection.js"_kj)
                             .runAndReturn(env.js);
    KJ_EXPECT(protectedBody.isTrue());
    KJ_EXPECT(jsg::JsObject(env.js.v8Context()->Global())
                  .get(env.js, "leaseCanceled"_kj)
                  .toString(env.js) == "1"_kj);
  });
}

KJ_TEST("INTERNAL pending stream disposal cancels the actual returned lease body") {
  internalPendingStreamDisposalTest(SocketCloseStreams::NATIVE);
}
KJ_TEST(
    "INTERNAL pending stream disposal cancels the actual returned lease body (TypeScript streams)") {
  internalPendingStreamDisposalTest(SocketCloseStreams::TYPESCRIPT);
}

enum class InternalRpcResultProbe { PRIVATE_STUB, PRIVATE_DATA, USER_STUB };
class InternalRpcResultServer final: public rpc::JsRpcTarget::Server {
 public:
  explicit InternalRpcResultServer(kj::Own<capnp::MallocMessageBuilder> value)
      : value(kj::mv(value)) {}
  kj::Promise<void> call(CallContext context) override {
    KJ_REQUIRE(value.get() != nullptr, "result fixture is single-use");
    context.getResults().setResult(value->getRoot<rpc::JsValue>().asReader());
    context.getResults().setHasDisposer(false);
    value = nullptr;
    return kj::READY_NOW;
  }

 private:
  kj::Own<capnp::MallocMessageBuilder> value;
};

void internalRpcResultPrototypeTest(InternalRpcResultProbe probe) {
  InternalRpcDisposalState holderState;
  capnp::MallocMessageBuilder flagsMessage;
  auto flags = flagsMessage.initRoot<CompatibilityFlags>();
  SimpleWorkerErrorReporter errors;
  compileCompatibilityFlags("2026-09-08", nullptr, flags, errors, false,
      CompatibilityDateValidation::CODE_VERSION, nullptr);
  KJ_ASSERT(errors.errors.empty(), kj::strArray(errors.errors, "\n"));
  TestFixture fixture({.featureFlags = flags.asReader()});
  fixture.runInIoContext([&](const TestFixture::Environment& env) -> kj::Promise<void> {
    auto& js = env.js;
    auto& stubHandler = KJ_ASSERT_NONNULL(js.tryGetTypeHandler<jsg::Ref<JsRpcStub>>());
    auto global = jsg::JsObject(js.v8Context()->Global());
    rpc::JsRpcTarget::Client holderClient(kj::heap<InternalRpcDisposalTarget>(holderState));
    auto holder =
        js.alloc<JsRpcStub>(env.context.addObject(kj::heap(kj::mv(holderClient))), kj::none);
    auto payload = probe == InternalRpcResultProbe::PRIVATE_DATA
        ? jsg::JsValue(js.obj())
        : jsg::JsValue(stubHandler.wrap(js, holder.addRef()));
    if (probe == InternalRpcResultProbe::PRIVATE_DATA) {
      KJ_ASSERT_NONNULL(payload.tryCast<jsg::JsObject>())
          .set(js, "handle"_kj, js.str("controlled-private-admission"_kj));
    }
    auto message = kj::heap<capnp::MallocMessageBuilder>();
    auto result = message->initRoot<rpc::JsValue>();
    RpcSerializerExternalHandler external(RpcSerializerExternalHandler::TRANSFER,
        rpc::JsValue::ExternalPusher::Client(KJ_EXCEPTION(FAILED, "unused result external pusher")),
        kj::none);
    jsg::Serializer serializer(js,
        {.version = 15,
          .omitHeader = false,
          .treatClassInstancesAsPlainObjects = false,
          .externalHandler = external});
    serializer.write(js, payload);
    auto bytes = serializer.release();
    result.setV8Serialized(bytes.data);
    result.adoptExternals(external.build(capnp::Orphanage::getForMessageContaining(result)));
    rpc::JsRpcTarget::Client returnClient(kj::heap<InternalRpcResultServer>(kj::mv(message)));
    auto transport =
        js.alloc<JsRpcStub>(env.context.addObject(kj::heap(kj::mv(returnClient))), kj::none);
    global.set(
        js, "privateResultTransport"_kj, jsg::JsValue(stubHandler.wrap(js, kj::mv(transport))));
    auto module = js.alloc<WrappedBindingModule>();
    auto& moduleHandler = KJ_ASSERT_NONNULL(js.tryGetTypeHandler<jsg::Ref<WrappedBindingModule>>());
    global.set(js, "nativeBinding"_kj, jsg::JsValue(moduleHandler.wrap(js, kj::mv(module))));
    global.set(js, "resultMode"_kj,
        js.str(probe == InternalRpcResultProbe::PRIVATE_DATA    ? "data"_kj
                : probe == InternalRpcResultProbe::PRIVATE_STUB ? "stub"_kj
                                                                : "value"_kj));
    auto script = jsg::NonModuleScript::compile(js, R"JS((async () => {
      const nativePrototype = Object.getPrototypeOf(privateResultTransport);
      const targetPrototype = resultMode === 'data' ? Object.prototype : nativePrototype;
      const oldThen = Object.getOwnPropertyDescriptor(targetPrototype, 'then');
      let hits = 0;
      Object.defineProperty(targetPrototype, 'then', {
        configurable: true,
        get() { if (resultMode !== 'data' || Object.hasOwn(this, 'handle')) hits++; return undefined; }
      });
      try {
        const method = nativeBinding.getRpcTransportMember(privateResultTransport, 'acquire', resultMode);
        const pending = Reflect.apply(method, undefined, []);
        const received = await new Promise((resolve, reject) =>
          nativeBinding.thenRpcTransport(pending, resolve, reject));
        const own = Object.getOwnPropertyDescriptor(received, 'then');
        if (resultMode === 'value') {
          if (hits === 0 || own !== undefined || Object.getPrototypeOf(received) !== nativePrototype)
            throw Error('user native identity/prototype behavior changed');
        } else {
          if (hits !== 0 || !own || own.value !== undefined || own.writable || own.configurable || own.enumerable)
            throw Error('private result was exposed to a prototype getter');
        }
        if (resultMode === 'data') {
          if (received.handle !== 'controlled-private-admission') throw Error('private data changed');
        } else {
          if (resultMode === 'stub') {
            const duplicate = nativeBinding.duplicateRpcTransport(received, 'private');
            const ownDuplicateThen = Object.getOwnPropertyDescriptor(duplicate, 'then');
            if (!ownDuplicateThen || ownDuplicateThen.value !== undefined || ownDuplicateThen.writable ||
                ownDuplicateThen.configurable || ownDuplicateThen.enumerable || hits !== 0)
              throw Error('private native duplicate was not protected');
            nativeBinding.disposeRpcTransport(duplicate);
          }
          nativeBinding.disposeRpcTransport(received);
        }
        nativeBinding.disposeRpcTransport(pending);
      } finally {
        if (oldThen) Object.defineProperty(targetPrototype, 'then', oldThen);
        else delete targetPrototype.then;
      }
    })())JS",
        "canonical-private-result-test.js"_kj)
                      .runAndReturn(js);
    return env.context.awaitJs(js, js.toPromise(script).whenResolved(js));
  });
}

KJ_TEST("INTERNAL RPC result blocks prototype then before first native resolution") {
  internalRpcResultPrototypeTest(InternalRpcResultProbe::PRIVATE_STUB);
}
KJ_TEST("INTERNAL RPC private admission blocks Object prototype then before resolution") {
  internalRpcResultPrototypeTest(InternalRpcResultProbe::PRIVATE_DATA);
}
KJ_TEST("INTERNAL RPC user result preserves its native identity and public prototype behavior") {
  internalRpcResultPrototypeTest(InternalRpcResultProbe::USER_STUB);
}
}  // namespace
}  // namespace workerd::api
