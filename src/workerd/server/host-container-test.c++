#include "container-auth.h"
#include "host-container.h"

#include <workerd/util/completion-membrane.h>

#include <kj/async-io.h>
#include <kj/test.h>
#include <kj/vector.h>

namespace workerd::server {
namespace {

KJ_TEST("Container callback credentials reject missing duplicate stale and malformed headers") {
  kj::HttpHeaderTable table;
  kj::Maybe<kj::String> token =
      kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  kj::HttpHeaders headers(table);
  KJ_EXPECT(!containerCallbackAuthorized(token, headers));
  headers.add(CONTAINER_CALLBACK_HEADER, KJ_ASSERT_NONNULL(token));
  KJ_EXPECT(containerCallbackAuthorized(token, headers));
  KJ_EXPECT(!containerCallbackAuthorized(kj::none, headers));
  KJ_EXPECT(!containerTokenMatches(KJ_ASSERT_NONNULL(token), "short"));
  KJ_EXPECT(!containerTokenMatches(KJ_ASSERT_NONNULL(token),
      "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
  headers.add(CONTAINER_CALLBACK_HEADER, KJ_ASSERT_NONNULL(token));
  KJ_EXPECT(!containerCallbackAuthorized(token, headers));
}

KJ_TEST("Host Container status and destroy do not reserve an absent lease") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  uint calls = 0;
  capnp::Capability::Client capability(kj::heap<HostContainerRpc>(
      [&calls](bool start, bool stop, bool completed, kj::String startup,
          kj::String expectedLeaseKey, bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    KJ_EXPECT(!start && !completed && startup.size() == 0);
    KJ_EXPECT(stop == (calls == 1));
    ++calls;
    return kj::Maybe<HostContainerRpc::Resolved>(kj::none);
  },
      kj::refcounted<HostContainerRpc::MutationQueue>()));
  auto container = capability.castAs<rpc::Container>();
  KJ_EXPECT(!container.statusRequest().send().wait(wait).getRunning());
  container.destroyRequest().send().wait(wait);
  KJ_EXPECT(calls == 2);
  KJ_EXPECT_THROW_MESSAGE(
      "Container operation failed", container.getTcpPortRequest().send().wait(wait));
}

class Engine final: public rpc::Container::Server {
 public:
  uint started = 0;
  uint destroyed = 0;

  kj::Promise<void> start(StartContext) override {
    ++started;
    return kj::READY_NOW;
  }
  kj::Promise<void> destroy(DestroyContext) override {
    ++destroyed;
    return kj::READY_NOW;
  }
};

KJ_TEST("Host Container mutation callbacks bracket the original native engine operation") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto engine = kj::heap<Engine>();
  auto& inspect = *engine;
  rpc::Container::Client native(kj::mv(engine));
  uint calls = 0;
  capnp::Capability::Client capability(kj::heap<HostContainerRpc>(
      [&calls, &inspect, &native](bool start, bool stop, bool completed, kj::String startup,
          kj::String expectedLeaseKey, bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    switch (calls++) {
      case 0:
        KJ_EXPECT(start && !stop && !completed && inspect.started == 0);
        break;
      case 1:
        KJ_EXPECT(!start && !stop && completed && inspect.started == 1);
        break;
      case 2:
        KJ_EXPECT(!start && stop && !completed && inspect.destroyed == 0);
        break;
      case 3:
        KJ_EXPECT(!start && stop && completed && inspect.destroyed == 1);
        break;
      default:
        KJ_FAIL_ASSERT("Unexpected Container callback");
    }
    return kj::Maybe<HostContainerRpc::Resolved>(
        HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>()));
  auto container = capability.castAs<rpc::Container>();
  container.startRequest().send().wait(wait);
  container.destroyRequest().send().wait(wait);
  KJ_EXPECT(calls == 4);
}

KJ_TEST("Host Container mutation queue preserves ordering after a waiting operation is canceled") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto queue = kj::refcounted<HostContainerRpc::MutationQueue>();
  auto first = queue->take();
  first.promise.wait(wait);
  { auto canceled = queue->take(); }
  auto last = queue->take();
  KJ_EXPECT(!last.promise.poll(wait));
  first.fulfiller->fulfill();
  last.promise.wait(wait);
  last.fulfiller->fulfill();
}

class StartingEngine final: public rpc::Container::Server {
 public:
  kj::PromiseFulfillerPair<void> startup = kj::newPromiseAndFulfiller<void>();
  kj::PromiseFulfillerPair<int32_t> exit = kj::newPromiseAndFulfiller<int32_t>();
  bool started = false;
  bool monitoring = false;
  bool portRequested = false;

  kj::Promise<void> start(StartContext) override {
    co_await kj::mv(startup.promise);
    started = true;
  }
  kj::Promise<void> monitor(MonitorContext context) override {
    KJ_REQUIRE(started);
    monitoring = true;
    context.getResults().setExitCode(co_await kj::mv(exit.promise));
  }
  kj::Promise<void> getTcpPort(GetTcpPortContext context) override {
    KJ_REQUIRE(started);
    portRequested = true;
    context.getResults().setPort(rpc::Container::Port::Client(nullptr));
    return kj::READY_NOW;
  }
  kj::Promise<void> destroy(DestroyContext) override {
    exit.fulfiller->fulfill(0);
    return kj::READY_NOW;
  }
};

KJ_TEST(
    "Container monitor and port await reservation and startup without locking their lifetimes") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto engine = kj::heap<StartingEngine>();
  auto& state = *engine;
  rpc::Container::Client native(kj::mv(engine));
  auto reserved = kj::newPromiseAndFulfiller<void>();
  uint lookups = 0;
  capnp::Capability::Client grant(kj::heap<HostContainerRpc>(
      [&native, &reserved, &lookups](bool start, bool, bool, kj::String, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    ++lookups;
    if (start) co_await kj::mv(reserved.promise);
    co_return kj::Maybe<HostContainerRpc::Resolved>(
        HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>()));
  auto container = grant.castAs<rpc::Container>();
  auto start = container.startRequest().send().dropPipeline();
  auto monitor = container.monitorRequest().send().dropPipeline();
  auto port = container.getTcpPortRequest().send().dropPipeline();
  KJ_EXPECT(!start.poll(wait) && !monitor.poll(wait) && !port.poll(wait));
  KJ_EXPECT(lookups == 1 && !state.started && !state.monitoring && !state.portRequested);
  reserved.fulfiller->fulfill();
  KJ_EXPECT(!start.poll(wait) && !monitor.poll(wait) && !port.poll(wait));
  KJ_EXPECT(lookups == 1);
  state.startup.fulfiller->fulfill();
  start.wait(wait);
  port.wait(wait);
  KJ_EXPECT(!monitor.poll(wait) && state.monitoring && state.portRequested);
  // A running monitor must not block the destroy mutation that will finish it.
  container.destroyRequest().send().wait(wait);
  KJ_EXPECT(monitor.wait(wait).getExitCode() == 0);
}

KJ_TEST(
    "Failed Container reservation releases waiting monitor and port without reaching the engine") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto reserved = kj::newPromiseAndFulfiller<void>();
  capnp::Capability::Client grant(kj::heap<HostContainerRpc>(
      [&reserved](bool start, bool, bool, kj::String, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    if (start) co_await kj::mv(reserved.promise);
    co_return kj::none;
  },
      kj::refcounted<HostContainerRpc::MutationQueue>()));
  auto container = grant.castAs<rpc::Container>();
  auto start = container.startRequest().send().dropPipeline();
  auto monitor = container.monitorRequest().send().dropPipeline();
  auto port = container.getTcpPortRequest().send().dropPipeline();
  KJ_EXPECT(!monitor.poll(wait) && !port.poll(wait));
  reserved.fulfiller->reject(KJ_EXCEPTION(FAILED, "Reservation failed."));
  KJ_EXPECT_THROW_MESSAGE("Container operation failed", start.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container operation failed", monitor.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container operation failed", port.wait(wait));
}

KJ_TEST("Container epoch revocation interrupts pending reservation and queued monitor and port") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto reserved = kj::newPromiseAndFulfiller<void>();
  auto revocation = kj::newPromiseAndFulfiller<void>();
  capnp::Capability::Client grant(kj::heap<HostContainerRpc>(
      [&reserved](bool start, bool, bool, kj::String, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    if (start) co_await kj::mv(reserved.promise);
    co_return kj::none;
  },
      kj::refcounted<HostContainerRpc::MutationQueue>()));
  auto membrane = kj::refcounted<RevokerMembrane>(kj::mv(revocation.promise));
  auto container = capnp::membrane(grant.castAs<rpc::Container>(), kj::mv(membrane));
  auto start = container.startRequest().send().dropPipeline();
  auto monitor = container.monitorRequest().send().dropPipeline();
  auto port = container.getTcpPortRequest().send().dropPipeline();
  KJ_EXPECT(!start.poll(wait) && !monitor.poll(wait) && !port.poll(wait));
  revocation.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "Container attachment revoked."));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", start.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", monitor.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", port.wait(wait));
}

class OwnerActivity final: public RequestTracker::Hooks {
 public:
  uint entered = 0;
  uint left = 0;
  void active() override {
    ++entered;
  }
  void inactive() override {
    ++left;
  }
};

KJ_TEST("Host Container owner activity follows monitor completion failure and revocation") {
  for (uint outcome = 0; outcome < 3; ++outcome) {
    kj::EventLoop loop;
    kj::WaitScope wait(loop);
    OwnerActivity hooks;
    auto tracker = kj::refcounted<RequestTracker>(hooks);
    auto engine = kj::heap<StartingEngine>();
    auto& native = *engine;
    native.started = true;
    rpc::Container::Client client(kj::mv(engine));
    capnp::Capability::Client grant(kj::heap<HostContainerRpc>(
        [&tracker, &client](bool, bool, bool, kj::String, kj::String,
            bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      return kj::Maybe<HostContainerRpc::Resolved>(
          HostContainerRpc::Resolved{rpc::Container::Client(client), kj::str("lease"),
            kj::heap<RequestTracker::ActiveRequest>(tracker->startRequest())});
    },
        kj::refcounted<HostContainerRpc::MutationQueue>()));
    auto revocation = kj::newPromiseAndFulfiller<void>();
    auto fenced =
        capnp::membrane(kj::mv(grant), kj::refcounted<RevokerMembrane>(kj::mv(revocation.promise)));
    auto monitor = fenced.castAs<rpc::Container>().monitorRequest().send().dropPipeline();
    KJ_EXPECT(!monitor.poll(wait) && native.monitoring && tracker->isActive());
    if (outcome == 0) {
      native.exit.fulfiller->fulfill(0);
      KJ_EXPECT(monitor.wait(wait).getExitCode() == 0);
    } else if (outcome == 1) {
      native.exit.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "Unknown process state."));
      KJ_EXPECT_THROW_MESSAGE("Container operation failed", monitor.wait(wait));
    } else {
      revocation.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "Container attachment revoked."));
      KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", monitor.wait(wait));
    }
    kj::evalLater([] {}).wait(wait);
    KJ_EXPECT(!tracker->isActive() && hooks.entered == hooks.left);
  }
}

KJ_TEST("Delayed monitor cleanup retains its original lease key after replacement startup") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto oldEngine = kj::heap<StartingEngine>();
  auto& old = *oldEngine;
  old.started = true;
  rpc::Container::Client oldClient(kj::mv(oldEngine));
  auto replacementEngine = kj::heap<Engine>();
  auto& replacement = *replacementEngine;
  rpc::Container::Client replacementClient(kj::mv(replacementEngine));
  kj::StringPtr current = "old";
  uint stops = 0;
  capnp::Capability::Client grant(kj::heap<HostContainerRpc>(
      [&oldClient, &replacementClient, &current, &stops](bool start, bool stop, bool, kj::String,
          kj::String expected, bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    if (start) current = "replacement";
    KJ_REQUIRE(expected.size() == 0 || expected == current, "Lease generation changed.");
    if (stop) ++stops;
    return kj::Maybe<HostContainerRpc::Resolved>(HostContainerRpc::Resolved{
      rpc::Container::Client(current == "old" ? oldClient : replacementClient), kj::str(current),
      {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>()));
  auto container = grant.castAs<rpc::Container>();
  auto monitor = container.monitorRequest().send().dropPipeline();
  KJ_EXPECT(!monitor.poll(wait) && old.monitoring);
  container.startRequest().send().wait(wait);
  KJ_EXPECT(replacement.started == 1);
  old.exit.fulfiller->fulfill(0);
  KJ_EXPECT_THROW_MESSAGE("Container operation failed", monitor.wait(wait));
  KJ_EXPECT(stops == 0 && replacement.destroyed == 0);
}

class ExitedEngine final: public rpc::Container::Server {
 public:
  explicit ExitedEngine(int32_t code, bool disconnected): code(code), disconnected(disconnected) {}
  kj::Promise<void> start(StartContext) override {
    return kj::READY_NOW;
  }
  kj::Promise<void> monitor(MonitorContext context) override {
    KJ_REQUIRE(!disconnected, "Docker wait transport disconnected.");
    context.getResults().setExitCode(code);
    return kj::READY_NOW;
  }

 private:
  int32_t code;
  bool disconnected;
};

KJ_TEST(
    "Positive native exit codes persist stop intent and cleanup before returning monitor result") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  for (int32_t code: {0, 7, 137}) {
    rpc::Container::Client native(kj::heap<ExitedEngine>(code, false));
    uint stage = 0;
    capnp::Capability::Client grant(kj::heap<HostContainerRpc>(
        [&native, &stage](bool start, bool stop, bool completed, kj::String,
            kj::String expectedLeaseKey,
            bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      KJ_EXPECT(!start);
      KJ_EXPECT(expectedLeaseKey == (stage == 0 ? "" : "lease"));
      KJ_EXPECT(stop == (stage != 0));
      KJ_EXPECT(completed == (stage == 2));
      ++stage;
      return kj::Maybe<HostContainerRpc::Resolved>(
          HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}});
    },
        kj::refcounted<HostContainerRpc::MutationQueue>()));
    auto result = grant.castAs<rpc::Container>().monitorRequest().send().wait(wait);
    KJ_EXPECT(result.getExitCode() == code && stage == 3);
  }
}

KJ_TEST("Unknown native monitor transport failure never requests stop or releases its lease") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  rpc::Container::Client native(kj::heap<ExitedEngine>(0, true));
  uint lookups = 0;
  capnp::Capability::Client grant(kj::heap<HostContainerRpc>(
      [&native, &lookups](bool start, bool stop, bool completed, kj::String, kj::String,
          bool) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    KJ_EXPECT(!start && !stop && !completed);
    ++lookups;
    return kj::Maybe<HostContainerRpc::Resolved>(
        HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>()));
  KJ_EXPECT_THROW_MESSAGE("Container operation failed",
      grant.castAs<rpc::Container>().monitorRequest().send().wait(wait));
  KJ_EXPECT(lookups == 1);
}

KJ_TEST("Monitor lookup after a proved start can wait an exited lease without Running evidence") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  for (int32_t code: {0, 7, 137}) {
    rpc::Container::Client native(kj::heap<ExitedEngine>(code, false));
    uint stage = 0;
    capnp::Capability::Client grant(kj::heap<HostContainerRpc>(
        [&native, &stage](bool start, bool stop, bool completed, kj::String, kj::String expected,
            bool monitor) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
      KJ_EXPECT(start == (stage == 0));
      KJ_EXPECT(monitor == (stage == 2));
      KJ_EXPECT(stop == (stage >= 3));
      KJ_EXPECT(completed == (stage == 1 || stage == 4));
      KJ_EXPECT(expected == (stage == 0 ? "" : "lease"));
      ++stage;
      return kj::Maybe<HostContainerRpc::Resolved>(
          HostContainerRpc::Resolved{rpc::Container::Client(native), kj::str("lease"), {}});
    },
        kj::refcounted<HostContainerRpc::MutationQueue>()));
    auto container = grant.castAs<rpc::Container>();
    container.startRequest().send().wait(wait);
    KJ_EXPECT(stage == 2);
    KJ_EXPECT(container.monitorRequest().send().wait(wait).getExitCode() == code);
    KJ_EXPECT(stage == 5);
  }
}

KJ_TEST("Monitor lookup cannot bind a replacement after its original start was proved") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  rpc::Container::Client native(kj::heap<ExitedEngine>(0, false));
  uint stops = 0;
  capnp::Capability::Client grant(kj::heap<HostContainerRpc>(
      [&native, &stops](bool, bool stop, bool, kj::String, kj::String expected,
          bool monitor) -> kj::Promise<kj::Maybe<HostContainerRpc::Resolved>> {
    if (stop) ++stops;
    if (monitor) KJ_EXPECT(expected == "lease");
    return kj::Maybe<HostContainerRpc::Resolved>(HostContainerRpc::Resolved{
      rpc::Container::Client(native), kj::str(monitor ? "replacement" : "lease"), {}});
  },
      kj::refcounted<HostContainerRpc::MutationQueue>()));
  auto container = grant.castAs<rpc::Container>();
  container.startRequest().send().wait(wait);
  KJ_EXPECT_THROW_MESSAGE(
      "Container operation failed", container.monitorRequest().send().wait(wait));
  KJ_EXPECT(stops == 0);
}

class PendingCapability final: public capnp::Capability::Server {
 public:
  explicit PendingCapability(bool& active): active(active) {}

  DispatchCallResult dispatchCall(uint64_t,
      uint16_t,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    active = true;
    return {
      kj::Promise<void>(kj::NEVER_DONE).attach(kj::defer([this] { active = false; })), false, true};
  }

 private:
  bool& active;
};

class DerivedCapabilities final: public rpc::Container::Server {
 public:
  bool portActive = false;
  bool processActive = false;
  capnp::ByteStream::Client writer = nullptr;

  kj::Promise<void> getTcpPort(GetTcpPortContext context) override {
    capnp::Capability::Client pending(kj::heap<PendingCapability>(portActive));
    context.getResults().setPort(pending.castAs<rpc::Container::Port>());
    return kj::READY_NOW;
  }
  kj::Promise<void> exec(ExecContext context) override {
    writer = context.getParams().getStdoutWriter();
    capnp::Capability::Client pending(kj::heap<PendingCapability>(processActive));
    context.getResults().initProcess().setHandle(pending.castAs<rpc::Container::ProcessHandle>());
    return kj::READY_NOW;
  }
};

KJ_TEST("Container grant revocation cancels already-started derived port and process operations") {
  kj::EventLoop loop;
  kj::WaitScope wait(loop);
  auto derived = kj::heap<DerivedCapabilities>();
  auto& inspect = *derived;
  rpc::Container::Client native(kj::mv(derived));
  auto revocation = kj::newPromiseAndFulfiller<void>();
  auto membrane = kj::refcounted<RevokerMembrane>(kj::mv(revocation.promise));
  auto container = capnp::membrane(native, kj::mv(membrane));
  auto port = container.getTcpPortRequest().send().wait(wait).getPort();
  auto portCall = port.connectRequest().send().dropPipeline();
  KJ_EXPECT(!portCall.poll(wait) && inspect.portActive);
  auto process = container.execRequest().send().wait(wait).getProcess().getHandle();
  auto processCall = process.waitRequest().send().dropPipeline();
  KJ_EXPECT(!processCall.poll(wait) && inspect.processActive);
  revocation.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "Container attachment revoked."));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", portCall.wait(wait));
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", processCall.wait(wait));
  KJ_EXPECT(!inspect.portActive && !inspect.processActive);
  KJ_EXPECT_THROW_MESSAGE("Container attachment revoked", port.connectRequest().send().wait(wait));
}

class PendingCallback final: public kj::HttpService {
 public:
  explicit PendingCallback(kj::HttpHeaderTable& table): table(table) {}
  uint active = 0;
  kj::Maybe<kj::String> token =
      kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

  kj::Promise<void> request(kj::HttpMethod,
      kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncInputStream&,
      Response& response) override {
    return response.sendError(405, "Method Not Allowed", table);
  }
  kj::Promise<void> connect(kj::StringPtr,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream&,
      ConnectResponse& response,
      kj::HttpConnectSettings) override {
    KJ_REQUIRE(containerCallbackAuthorized(token, headers));
    ++active;
    KJ_DEFER(--active);
    kj::HttpHeaders result(table);
    response.accept(202, "Accepted", result);
    co_await kj::Promise<void>(kj::NEVER_DONE);
  }

 private:
  kj::HttpHeaderTable& table;
};

KJ_TEST("Container reverse callback revocation closes a started HTTP CONNECT stream") {
  auto io = kj::setupAsyncIo();
  kj::HttpHeaderTable table;
  PendingCallback callback(table);
  kj::HttpServer server(io.provider->getTimer(), table, callback);
  auto pipe = kj::newTwoWayPipe();
  kj::Canceler revocation;
  auto serving =
      revocation.wrap(server.listenHttp(kj::mv(pipe.ends[0]))).eagerlyEvaluate([](kj::Exception&&) {
  });
  auto client = kj::newHttpClient(table, *pipe.ends[1]);
  kj::HttpHeaders headers(table);
  headers.add(CONTAINER_CALLBACK_HEADER, KJ_ASSERT_NONNULL(callback.token));
  auto request = client->connect("example.com:443", headers, {});
  KJ_EXPECT(request.status.wait(io.waitScope).statusCode == 202 && callback.active);
  revocation.cancel("Container attachment revoked.");
  KJ_EXPECT(!callback.active);
  kj::byte byte;
  KJ_EXPECT(request.connection->tryRead(&byte, 1, 1).wait(io.waitScope) == 0);
  serving.wait(io.waitScope);
}

KJ_TEST("Container reverse callback streams exceed the idle pool and all revoke together") {
  auto io = kj::setupAsyncIo();
  kj::HttpHeaderTable table;
  PendingCallback callback(table);
  kj::HttpServer server(io.provider->getTimer(), table, callback);
  kj::Canceler revocation;
  kj::Vector<kj::Promise<void>> serving;
  kj::Vector<kj::Own<kj::AsyncIoStream>> clients;
  kj::Vector<kj::Own<kj::HttpClient>> httpClients;
  kj::Vector<kj::Own<kj::AsyncIoStream>> tunnels;
  for (uint i = 0; i < 32; ++i) {
    auto pipe = kj::newTwoWayPipe();
    serving.add(revocation.wrap(server.listenHttp(kj::mv(pipe.ends[0])))
            .eagerlyEvaluate([](kj::Exception&&) {}));
    clients.add(kj::mv(pipe.ends[1]));
    auto client = kj::newHttpClient(table, *clients.back());
    kj::HttpHeaders headers(table);
    headers.add(CONTAINER_CALLBACK_HEADER, KJ_ASSERT_NONNULL(callback.token));
    auto request = client->connect("example.com:443", headers, {});
    KJ_EXPECT(request.status.wait(io.waitScope).statusCode == 202);
    tunnels.add(kj::mv(request.connection));
    httpClients.add(kj::mv(client));
  }
  KJ_EXPECT(callback.active == 32);
  revocation.cancel("Container attachment revoked.");
  KJ_EXPECT(callback.active == 0);
  kj::byte byte;
  for (auto& tunnel: tunnels) KJ_EXPECT(tunnel->tryRead(&byte, 1, 1).wait(io.waitScope) == 0);
  kj::joinPromises(serving.releaseAsArray()).wait(io.waitScope);
}

}  // namespace
}  // namespace workerd::server
