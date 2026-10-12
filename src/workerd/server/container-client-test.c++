// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Tests for container-client JSON decoding.
//
// These tests verify that JSON responses from the Docker API are decoded into
// Cap'n Proto messages whose backing storage outlives the decode call. The
// original decodeJsonResponse<T>() returned a Builder pointing into a
// stack-local MallocMessageBuilder; the builder was then accessed after the
// message was destroyed (use-after-free). Under ASAN this test would crash
// with the old code.

#include "container-auth.h"
#include "container-client.h"

#include <kj/encoding.h>
#include <kj/test.h>
#include <kj/vector.h>

namespace workerd::server {
namespace {

class PrivateExecChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata) override {
    KJ_FAIL_ASSERT("Unexpected private fixture outbound request");
  }
  void requireAllowsTransfer() override {
    KJ_FAIL_ASSERT("Unexpected fixture transfer");
  }
  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage) override {
    KJ_FAIL_ASSERT("Unexpected fixture token request");
  }
};

class SignalDockerService final: public kj::HttpService,
                                 public kj::TaskSet::ErrorHandler,
                                 public ChannelTokenHandler::Resolver {
 public:
  explicit SignalDockerService(kj::HttpHeaderTable& table): table(table) {}

  kj::Vector<kj::String> signals;

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders&,
      kj::AsyncInputStream&,
      Response& response) override {
    kj::HttpHeaders headers(table);
    if (method == kj::HttpMethod::GET && url.startsWith("/volumes")) {
      auto body = "{\"Volumes\":[]}"_kj;
      auto output = response.send(200, "OK", headers, body.size());
      co_await output->write(body.asBytes());
      co_return;
    }
    if (method == kj::HttpMethod::POST) {
      KJ_REQUIRE(url.startsWith("/containers/application/kill?signal="));
      signals.add(kj::str(url));
    } else {
      KJ_REQUIRE(method == kj::HttpMethod::DELETE);
    }
    response.send(204, "No Content", headers, uint64_t(0));
    co_return;
  }

  void taskFailed(kj::Exception&& exception) override {
    KJ_FAIL_ASSERT("Unexpected signal fixture task failure", exception);
  }

  kj::Own<IoChannelFactory::SubrequestChannel> resolveEntrypoint(
      kj::StringPtr, kj::Maybe<kj::StringPtr>, Frankenvalue, Persistent) override {
    KJ_FAIL_ASSERT("Unexpected signal fixture entrypoint resolution");
  }
  kj::Own<IoChannelFactory::ActorClassChannel> resolveActorClass(
      kj::StringPtr, kj::Maybe<kj::StringPtr>, Frankenvalue, Persistent) override {
    KJ_FAIL_ASSERT("Unexpected signal fixture class resolution");
  }
  kj::Own<IoChannelFactory::ActorChannel> resolveActor(
      kj::StringPtr, kj::ArrayPtr<const byte>, kj::Maybe<kj::StringPtr>, Persistent) override {
    KJ_FAIL_ASSERT("Unexpected signal fixture actor resolution");
  }

 private:
  kj::HttpHeaderTable& table;
};

KJ_TEST("Container signals preserve the full Linux numeric range in Docker requests") {
  auto io = kj::setupAsyncIo();
  kj::HttpHeaderTable table;
  SignalDockerService service(table);
  auto listener =
      io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
  kj::HttpServer server(io.provider->getTimer(), table, service);
  auto serving = server.listenHttp(*listener).eagerlyEvaluate(nullptr);
  capnp::ByteStreamFactory byteStreams;
  ChannelTokenHandler tokens(service);
  kj::TaskSet tasks(service);
  kj::Promise<void> cleanup = kj::READY_NOW;
  auto implementation = kj::refcounted<ContainerClient>(byteStreams, io.provider->getTimer(),
      io.provider->getNetwork(), kj::none, kj::str("127.0.0.1:", listener->getPort()),
      kj::str("application"), kj::none, kj::str("unused-sidecar"), tasks, kj::READY_NOW,
      [&](kj::Promise<void> promise) { cleanup = kj::mv(promise); }, tokens, ContainerPrivileges{});
  rpc::Container::Client client(implementation->addRef());
  for (auto signo: {1u, 31u, 32u, 34u, 64u}) {
    auto request = client.signalRequest();
    request.setSigno(signo);
    request.send().wait(io.waitScope);
    KJ_ASSERT(service.signals.size() > 0);
    KJ_EXPECT(service.signals.back() == kj::str("/containers/application/kill?signal=", signo));
  }
  KJ_EXPECT(service.signals.size() == 5);
  for (auto signo: {0u, 65u}) {
    auto request = client.signalRequest();
    request.setSigno(signo);
    KJ_EXPECT_THROW_MESSAGE("Invalid signal number", request.send().wait(io.waitScope));
  }
  KJ_EXPECT(service.signals.size() == 5);
  implementation->shutdown();
  cleanup.wait(io.waitScope);
  tasks.onEmpty().wait(io.waitScope);
}

// Holds the Docker attach stream open after the inspected process has exited.
class ExecDockerFixture final {
 public:
  ExecDockerFixture(kj::HttpHeaderTable& table, kj::TaskSet& tasks, bool privateMode = false)
      : privateMode(privateMode),
        table(table),
        tasks(tasks) {
    auto pair = kj::newPromiseAndFulfiller<void>();
    releaseOutput = kj::mv(pair.fulfiller);
    outputReady = pair.promise.fork();
    auto drained = kj::newPromiseAndFulfiller<void>();
    releaseDrained = kj::mv(drained.fulfiller);
    outputDrained = drained.promise.fork();
    auto terminal = kj::newPromiseAndFulfiller<void>();
    releaseTerminal = kj::mv(terminal.fulfiller);
    terminalReady = terminal.promise.fork();
  }

  bool exited = false;
  bool spawnPending = true;
  bool outputClosed = false;
  uint16_t port = 0;
  kj::Own<kj::PromiseFulfiller<void>> releaseOutput;
  kj::Own<kj::PromiseFulfiller<void>> releaseTerminal;
  bool privateMode;
  bool privatePty = false;
  bool unknownTerminal = false;
  bool spawnFailed = false;
  bool invalidStarted = false;
  bool rejectContainerCreate = false;
  uint containerStarts = 0;
  kj::String containerCreateBody;
  uint createRequests = 0;
  uint runRequests = 0;
  bool truncateTerminal = false;
  uint drainFault = 0;
  bool wrongTerminalIdentity = false;
  bool invalidRunUpgrade = false;
  bool largeOutput = false;
  kj::Vector<uint32_t> privateSignals;
  kj::Vector<kj::String> resizes;
  kj::String identity;
  kj::String bootstrapKey;
  kj::String bootstrapStart;
  kj::String stdinBytes;
  uint disconnectedRequests = 0;

  kj::Promise<void> listen(kj::ConnectionReceiver& listener) {
    for (;;) {
      tasks.add(serve(co_await listener.accept()).catch_([this](kj::Exception&& exception) {
        if (privateMode && exception.getType() == kj::Exception::Type::DISCONNECTED) {
          ++disconnectedRequests;
        } else
          kj::throwFatalException(kj::mv(exception));
      }));
    }
  }

 private:
  kj::HttpHeaderTable& table;
  kj::TaskSet& tasks;
  kj::ForkedPromise<void> outputReady = nullptr;
  kj::Own<kj::PromiseFulfiller<void>> releaseDrained;
  kj::ForkedPromise<void> outputDrained = nullptr;
  kj::ForkedPromise<void> terminalReady = nullptr;

  kj::Promise<void> serve(kj::Own<kj::AsyncIoStream> stream) {
    auto input = kj::newHttpInputStream(*stream, table);
    auto incoming = co_await input->readRequestAllowingConnect();
    KJ_IF_SOME(connect, incoming.tryGet<kj::HttpInputStream::Connect>()) {
      KJ_REQUIRE(privateMode && connect.authority == "/open-compute/callback-channel");
      co_await stream->write("HTTP/1.1 200 Connection Established\r\n\r\n"_kj.asBytes());
      co_await stream->readAllBytes(1024);
      co_return;
    }
    auto& request = incoming.get<kj::HttpInputStream::Request>();
    auto requestBody = co_await request.body->readAllText(1048576);
    auto url = request.url;
    if (url == "/exec/process/start") {
      if (privateMode) {
        auto parsed = decodeJsonResponse<docker_api::Docker::ExecStartRequest>(requestBody);
        KJ_REQUIRE(!parsed->getRoot<docker_api::Docker::ExecStartRequest>().getTty());
      }
      co_await stream->write(
          "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n"_kj
              .asBytes());
      if (privateMode) {
        // Split READY across Docker frames to exercise framing rather than write boundaries.
        const byte first[] = {1, 0, 0, 0, 0, 0, 0, 3, 'O', 'C', '_'};
        const byte second[] = {1, 0, 0, 0, 0, 0, 0, 14, 'E', 'X', 'E', 'C', '_', 'R', 'E', 'A', 'D',
          'Y', '_', 'V', '1', '\n'};
        co_await stream->write(kj::arrayPtr(first));
        co_await stream->write(kj::arrayPtr(second));
        byte key[32];
        byte length[4];
        co_await stream->read(key, sizeof(key));
        co_await stream->read(length, sizeof(length));
        bootstrapKey = kj::encodeHex(kj::arrayPtr(key));
        uint32_t size = (uint32_t(length[0]) << 24) | (uint32_t(length[1]) << 16) |
            (uint32_t(length[2]) << 8) | length[3];
        KJ_REQUIRE(size > 0 && size <= 1048576);
        auto bytes = kj::heapArray<byte>(size);
        co_await stream->read(bytes.asPtr());
        bootstrapStart = kj::str(bytes.asChars());
        capnp::MallocMessageBuilder parsed;
        auto root = parsed.initRoot<capnp::JsonValue>();
        capnp::JsonCodec().decodeRaw(bootstrapStart, root);
        KJ_REQUIRE(root.isObject() && root.getObject().size() == 4);
        for (auto field: root.getObject()) {
          if (field.getName() == "pty") privatePty = field.getValue().getBoolean();
          if (field.getName() == "argv") {
            auto argv = field.getValue().getArray();
            KJ_REQUIRE(argv.size() == 1 && argv[0].getString() == "/no-shell/command");
          }
        }
      }
      co_await outputReady.addBranch();
      if (largeOutput) {
        const byte header[] = {1, 0, 0, 0, 0, 16, 0, 0};
        auto bytes = kj::heapArray<byte>(1048576);
        bytes.asPtr().fill('x');
        co_await stream->write(kj::arrayPtr(header));
        co_await stream->write(bytes.asPtr());
      } else {
        const byte frame[] = {1, 0, 0, 0, 0, 0, 0, 4, 'l', 'a', 't', 'e'};
        co_await stream->write(kj::arrayPtr(frame));
      }
      stream->shutdownWrite();
      outputClosed = true;
      releaseDrained->fulfill();
      if (privateMode) stdinBytes = co_await stream->readAllText(1024);
      co_return;
    }

    uint status = 200;
    kj::String body;
    if (url == "/containers/create?name=application") {
      containerCreateBody = kj::mv(requestBody);
      status = rejectContainerCreate ? 400 : 201;
      body = kj::str("{}");
    } else if (url == "/containers/create?name=application-proxy") {
      status = 201;
      body = kj::str("{}");
    } else if (url == "/containers/application/start") {
      ++containerStarts;
      status = 204;
      body = kj::str("");
    } else if (url == "/containers/application-proxy/start") {
      status = 204;
      body = kj::str("");
    } else if (url == "/images/image/json") {
      body = kj::str("{}");
    } else if (url.startsWith("/volumes")) {
      body = kj::str("{\"Volumes\":[]}");
    } else if (url == "/containers/application/json") {
      body = kj::str("{\"State\":{\"Status\":\"running\"}}");
    } else if (url == "/containers/application-proxy/json") {
      body = kj::str(
          "{\"State\":{\"Status\":\"running\"},\"NetworkSettings\":{\"Ports\":{\"39001/tcp\":[{\"HostPort\":\"",
          port, "\"}]}}}");
    } else if (url == "/networks/bridge") {
      body = kj::str(
          "{\"IPAM\":{\"Config\":[{\"Gateway\":\"127.0.0.1\",\"Subnet\":\"127.0.0.0/8\"}]}}");
    } else if (url == "/egress") {
      status = 204;
      body = kj::str("");
    } else if (url == "/ca") {
      body = kj::str("fixture certificate");
    } else if (url == "/containers/application/exec") {
      ++createRequests;
      if (privateMode) {
        auto parsed = decodeJsonResponse<docker_api::Docker::ExecCreateRequest>(requestBody);
        auto created = parsed->getRoot<docker_api::Docker::ExecCreateRequest>();
        auto cmd = created.getCmd();
        KJ_REQUIRE(!created.getTty() && cmd.size() == 3);
        KJ_REQUIRE(cmd[0] == "/opt/open-compute/exec/helper" && cmd[1] == "--open-compute-exec");
        identity = kj::str(cmd[2]);
        KJ_REQUIRE(identity.size() == 64);
        for (char c: identity) KJ_REQUIRE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
        KJ_REQUIRE(!created.hasEnv() && !created.hasWorkingDir());
      }
      status = 201;
      body = kj::str("{\"Id\":\"process\"}");
    } else if (url.endsWith("/wait?condition=removed")) {
      body = kj::str("{\"StatusCode\":0}");
    } else if (url == "/exec/process/json") {
      KJ_REQUIRE(!privateMode, "Private exec must not inspect the wrapper as the child");
      body = spawnPending
          ? kj::str("{\"Pid\":0,\"Running\":false,\"ExitCode\":0}")
          : kj::str("{\"Pid\":42,\"Running\":", exited ? "false" : "true", ",\"ExitCode\":7}");
    } else if (url == "/open-compute/exec") {
      KJ_REQUIRE(privateMode && request.method == kj::HttpMethod::POST);
      kj::Maybe<kj::StringPtr> token;
      request.headers.forEach([&](auto name, auto value) {
        if (workerd::strcaseeq(name, CONTAINER_CONTROL_HEADER)) token = value;
      });
      KJ_REQUIRE(KJ_ASSERT_NONNULL(token) ==
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
      capnp::MallocMessageBuilder parsed;
      auto root = parsed.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(requestBody, root);
      kj::String op;
      uint32_t signal = 0, rows = 0, cols = 0;
      for (auto field: root.getObject()) {
        auto name = field.getName();
        auto value = field.getValue();
        if (name == "identity") {
          KJ_REQUIRE(value.getString() == identity);
        }
        if (name == "key") {
          bool authenticated = containerTokenMatches(bootstrapKey, value.getString());
          KJ_REQUIRE(authenticated);
        }
        if (name == "op") op = kj::str(value.getString());
        if (name == "signal") signal = value.getNumber();
        if (name == "rows") rows = value.getNumber();
        if (name == "cols") cols = value.getNumber();
      }
      if (op == "run") {
        ++runRequests;
        KJ_REQUIRE(root.getObject().size() == 3);
        KJ_REQUIRE(
            request.headers.get(kj::HttpHeaderId::UPGRADE).orDefault("") == "open-compute-exec-v1");
        auto upgrade =
            kj::str("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: ",
                invalidRunUpgrade ? "wrong-protocol" : "open-compute-exec-v1", "\r\n\r\n");
        co_await stream->write(upgrade.asBytes());
        auto writeFrame = [&](kj::StringPtr value) -> kj::Promise<void> {
          byte header[] = {byte(value.size() >> 24), byte(value.size() >> 16),
            byte(value.size() >> 8), byte(value.size())};
          kj::ArrayPtr<const byte> pieces[] = {kj::arrayPtr(header), value.asBytes()};
          co_await stream->write(kj::arrayPtr(pieces));
        };
        if (spawnFailed) {
          co_await writeFrame(
              "{\"ok\":false,\"code\":\"SPAWN_FAILED\",\"state\":\"spawn-failed\"}");
          co_return;
        }
        co_await writeFrame(kj::str("{\"ok\":true,\"state\":\"running\",\"pid\":",
            invalidStarted ? 0 : 51, ",\"startTicks\":99}"));
        if (invalidStarted || invalidRunUpgrade) {
          co_await stream->readAllBytes(1);
          co_return;
        }
        auto peerClosed = [&]() -> kj::Promise<void> {
          co_await stream->readAllBytes(1);
          kj::throwFatalException(KJ_EXCEPTION(DISCONNECTED, "Private run owner disconnected"));
        };
        co_await terminalReady.addBranch().exclusiveJoin(peerClosed());
        if (truncateTerminal) {
          const byte partial[] = {0, 0, 0, 20, '{'};
          co_await stream->write(kj::arrayPtr(partial));
          co_return;
        }
        if (unknownTerminal) {
          co_await writeFrame(
              "{\"ok\":false,\"code\":\"CHILD_STATUS_UNKNOWN\",\"state\":\"unknown\"}");
        } else {
          co_await writeFrame(kj::str("{\"ok\":true,\"state\":\"exited\",\"pid\":",
              wrongTerminalIdentity ? 52 : 51, ",\"startTicks\":99,\"exitCode\":7}"));
        }
        co_await outputDrained.addBranch().exclusiveJoin(peerClosed());
        if (drainFault == 1) co_return;
        if (drainFault == 2) {
          co_await writeFrame(
              "{\"ok\":false,\"code\":\"OUTPUT_DRAIN_FAILED\",\"state\":\"drained\"}");
        } else if (drainFault == 3) {
          co_await writeFrame("{\"ok\":true,\"state\":\"drained\",\"exitCode\":7}");
        } else if (drainFault == 5) {
          const byte partial[] = {0, 0, 0, 20, '{'};
          co_await stream->write(kj::arrayPtr(partial));
        } else if (drainFault == 7) {
          const byte oversized[] = {0, 0, 16, 1};
          co_await stream->write(kj::arrayPtr(oversized));
        } else {
          co_await writeFrame("{\"ok\":true,\"state\":\"drained\"}");
          if (drainFault == 4) {
            co_await writeFrame("{\"ok\":true,\"state\":\"drained\"}");
          } else if (drainFault == 6) {
            co_await stream->write("x"_kj.asBytes());
          }
        }
        co_return;
      } else if (op == "signal") {
        KJ_REQUIRE(root.getObject().size() == 4);
        privateSignals.add(signal);
      } else if (op == "resize") {
        KJ_REQUIRE(root.getObject().size() == 5);
        resizes.add(kj::str(cols, "x", rows));
      } else {
        KJ_FAIL_REQUIRE("Unexpected private exec operation");
      }
      if (op == "signal" && invalidStarted && exited) {
        body = kj::str("{\"ok\":false,\"code\":\"CHILD_NOT_RUNNING\",\"state\":\"exited\","
                       "\"pid\":51,\"startTicks\":99,\"exitCode\":7}");
      } else {
        body = kj::str("{\"ok\":true}");
      }
    } else {
      KJ_REQUIRE(request.method == kj::HttpMethod::DELETE, url);
      status = 204;
      body = kj::str("");
    }
    auto response = kj::str("HTTP/1.1 ", status, " Fixture\r\nContent-Length: ", body.size(),
        "\r\nConnection: close\r\n\r\n", body);
    co_await stream->write(response.asBytes());
  }
};

KJ_TEST("Container start replaces image entrypoint and command only for a nonempty override") {
  for (bool privateMode: {false, true}) {
    for (uint mode: {0u, 1u, 2u, 3u}) {
      auto io = kj::setupAsyncIo();
      kj::HttpHeaderTable table;
      SignalDockerService resolver(table);
      kj::TaskSet tasks(resolver);
      ExecDockerFixture fixture(table, tasks, privateMode);
      fixture.rejectContainerCreate = mode == 3;
      auto listener =
          io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
      fixture.port = listener->getPort();
      auto serving = fixture.listen(*listener).eagerlyEvaluate(nullptr);
      capnp::ByteStreamFactory byteStreams;
      ChannelTokenHandler tokens(resolver);
      kj::Maybe<kj::Own<IoChannelFactory::SubrequestChannel>> outbound;
      if (privateMode) outbound = kj::refcounted<PrivateExecChannel>();
      kj::Promise<void> cleanup = kj::READY_NOW;
      auto implementation = kj::refcounted<ContainerClient>(byteStreams, io.provider->getTimer(),
          io.provider->getNetwork(), kj::mv(outbound), kj::str("127.0.0.1:", fixture.port),
          kj::str("application"), kj::str("image"), kj::str("unused-sidecar"), tasks, kj::READY_NOW,
          [&](kj::Promise<void> promise) { cleanup = kj::mv(promise); }, tokens,
          ContainerPrivileges{});
      if (privateMode) {
        implementation->setHostCredentials(
            kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            kj::str("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
      }
      rpc::Container::Client client(implementation->addRef());
      auto request = client.startRequest();
      if (mode == 1) request.initEntrypoint(0);
      if (mode >= 2) {
        auto override = request.initEntrypoint(2);
        override.set(0, "/replacement");
        override.set(1, "argument");
      }
      if (mode == 3) {
        KJ_EXPECT_THROW_MESSAGE("Create container failed", request.send().wait(io.waitScope));
      } else {
        request.send().wait(io.waitScope);
      }
      capnp::MallocMessageBuilder parsed;
      auto root = parsed.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(fixture.containerCreateBody, root);
      bool hasCommand = false, hasEntrypoint = false;
      for (auto field: root.getObject()) {
        if (field.getName() == "Cmd") {
          hasCommand = true;
          KJ_EXPECT(field.getValue().isArray() && field.getValue().getArray().size() == 0);
        }
        if (field.getName() == "Entrypoint") {
          hasEntrypoint = true;
          auto ep = field.getValue().getArray();
          KJ_REQUIRE(ep.size() == 2);
          KJ_EXPECT(ep[0].getString() == "/replacement" && ep[1].getString() == "argument");
        }
      }
      KJ_EXPECT(hasCommand == (mode >= 2) && hasEntrypoint == (mode >= 2));
      KJ_EXPECT(fixture.containerStarts == (mode == 3 ? 0 : 1));
      implementation->shutdown();
      cleanup.wait(io.waitScope);
      tasks.onEmpty().wait(io.waitScope);
    }
  }
}

KJ_TEST("Docker exec exit resolves before descendants close stdout and preserves late output") {
  for (bool exitsWithoutPid: {false, true}) {
    auto io = kj::setupAsyncIo();
    kj::HttpHeaderTable table;
    SignalDockerService resolver(table);
    kj::TaskSet tasks(resolver);
    ExecDockerFixture fixture(table, tasks);
    auto listener =
        io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    fixture.port = listener->getPort();
    auto serving = fixture.listen(*listener).eagerlyEvaluate(nullptr);
    capnp::ByteStreamFactory byteStreams;
    ChannelTokenHandler tokens(resolver);
    kj::Promise<void> cleanup = kj::READY_NOW;
    auto implementation = kj::refcounted<ContainerClient>(byteStreams, io.provider->getTimer(),
        io.provider->getNetwork(), kj::none, kj::str("127.0.0.1:", fixture.port),
        kj::str("application"), kj::none, kj::str("unused-sidecar"), tasks, kj::READY_NOW,
        [&](kj::Promise<void> promise) { cleanup = kj::mv(promise); }, tokens,
        ContainerPrivileges{});
    rpc::Container::Client client(implementation->addRef());
    KJ_ASSERT(client.statusRequest().send().wait(io.waitScope).getRunning());
    auto pipe = kj::newOneWayPipe();
    auto output = pipe.in->readAllText().eagerlyEvaluate(nullptr);
    auto request = client.execRequest();
    request.initCmd(1).set(0, "fixture-command");
    request.initParams();
    request.setStdoutWriter(
        byteStreams.kjToCapnp(capnp::ExplicitEndOutputStream::wrap(kj::mv(pipe.out), []() {})));
    auto response = request.send().wait(io.waitScope);
    auto handle = response.getProcess().getHandle();
    auto wait = handle.waitRequest().send().eagerlyEvaluate(nullptr);
    io.provider->getTimer().afterDelay(75 * kj::MILLISECONDS).wait(io.waitScope);
    KJ_EXPECT(!wait.poll(io.waitScope));
    if (exitsWithoutPid) {
      fixture.releaseOutput->fulfill();
    } else {
      fixture.spawnPending = false;
      fixture.exited = true;
    }
    auto result =
        io.provider->getTimer().timeoutAfter(2 * kj::SECONDS, kj::mv(wait)).wait(io.waitScope);
    KJ_EXPECT(result.getExitCode() == (exitsWithoutPid ? 0 : 7));
    if (!exitsWithoutPid) {
      KJ_EXPECT(!fixture.outputClosed);
      KJ_EXPECT(!output.poll(io.waitScope));
      fixture.releaseOutput->fulfill();
    }
    KJ_EXPECT(output.wait(io.waitScope) == "late");
    KJ_EXPECT(fixture.outputClosed);
    implementation->shutdown();
    cleanup.wait(io.waitScope);
    tasks.onEmpty().wait(io.waitScope);
  }
}

KJ_TEST(
    "Private exec supervises the actual child without shell, PID files, or Docker control execs") {
  for (bool pty: {false, true}) {
    for (bool unknownTerminal: {false, true}) {
      auto io = kj::setupAsyncIo();
      kj::HttpHeaderTable table;
      SignalDockerService resolver(table);
      kj::TaskSet tasks(resolver);
      ExecDockerFixture fixture(table, tasks, true);
      fixture.unknownTerminal = unknownTerminal;
      // Fast children still have an authenticated child identity when the run receipt returns.
      fixture.exited = true;
      auto listener =
          io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
      fixture.port = listener->getPort();
      auto serving = fixture.listen(*listener).eagerlyEvaluate(nullptr);
      capnp::ByteStreamFactory byteStreams;
      ChannelTokenHandler tokens(resolver);
      auto implementation = kj::refcounted<ContainerClient>(byteStreams, io.provider->getTimer(),
          io.provider->getNetwork(), kj::refcounted<PrivateExecChannel>(),
          kj::str("127.0.0.1:", fixture.port), kj::str("application"), kj::none,
          kj::str("unused-sidecar"), tasks, kj::READY_NOW, [](kj::Promise<void> promise) {}, tokens,
          ContainerPrivileges{});
      implementation->setHostCredentials(
          kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
          kj::str("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
      rpc::Container::Client client(implementation->addRef());
      KJ_ASSERT(client.statusRequest().send().wait(io.waitScope).getRunning());
      auto pipe = kj::newOneWayPipe();
      auto output = pipe.in->readAllText().eagerlyEvaluate(nullptr);
      auto request = client.execRequest();
      request.initCmd(1).set(0, "/no-shell/command");
      auto params = request.initParams();
      if (pty) {
        params.initPty().setCols(80);
        params.getPty().setRows(24);
      }
      bool outputAborted = false;
      request.setStdoutWriter(byteStreams.kjToCapnp(
          capnp::ExplicitEndOutputStream::wrap(kj::mv(pipe.out), [&]() { outputAborted = true; })));
      auto response = request.send().wait(io.waitScope);
      request = nullptr;
      KJ_EXPECT(response.getProcess().getPid() == 51);
      KJ_EXPECT(fixture.privatePty == pty);
      auto handle = response.getProcess().getHandle();
      // The eager watcher must not consume the public stdinWriter() call.
      auto stdinResponse = handle.stdinWriterRequest().send().wait(io.waitScope);
      auto stdinWriter = byteStreams.capnpToKjExplicitEnd(stdinResponse.getWriter());
      stdinWriter->write("stdin-after-bootstrap"_kj.asBytes()).wait(io.waitScope);
      stdinWriter->end().wait(io.waitScope);
      io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(io.waitScope);
      KJ_EXPECT(fixture.runRequests == 1);
      for (uint32_t signo = 1; signo <= 64; ++signo) {
        auto signal = handle.killRequest();
        signal.setSigno(signo);
        signal.send().wait(io.waitScope);
      }
      for (auto signo: {0u, 65u}) {
        auto signal = handle.killRequest();
        signal.setSigno(signo);
        KJ_EXPECT_THROW_MESSAGE("Invalid signal number", signal.send().wait(io.waitScope));
      }
      KJ_EXPECT(fixture.privateSignals.size() == 64);
      for (auto i: kj::indices(fixture.privateSignals)) {
        KJ_EXPECT(fixture.privateSignals[i] == i + 1);
      }
      auto resize = handle.resizeRequest();
      resize.setCols(100);
      resize.setRows(40);
      if (pty) {
        resize.send().wait(io.waitScope);
        KJ_EXPECT(fixture.resizes.size() == 1 && fixture.resizes[0] == "100x40");
      } else {
        KJ_EXPECT_THROW_MESSAGE("PTY", resize.send().wait(io.waitScope));
      }
      fixture.releaseTerminal->fulfill();
      auto waiting = handle.waitRequest().send();
      if (unknownTerminal) {
        KJ_EXPECT_THROW_MESSAGE("Container exec control failed", waiting.wait(io.waitScope));
      } else {
        KJ_EXPECT(waiting.wait(io.waitScope).getExitCode() == 7);
        KJ_EXPECT(handle.waitRequest().send().wait(io.waitScope).getExitCode() == 7);
      }
      KJ_EXPECT(fixture.runRequests == 1);
      KJ_EXPECT(fixture.createRequests == 1);
      KJ_EXPECT(!fixture.outputClosed && !output.poll(io.waitScope));
      fixture.releaseOutput->fulfill();
      KJ_EXPECT(output.wait(io.waitScope) == "late");
      KJ_EXPECT(outputAborted == unknownTerminal);
      implementation->shutdown();
      tasks.onEmpty().wait(io.waitScope);
      KJ_EXPECT(fixture.stdinBytes == "stdin-after-bootstrap");
    }
  }
}

KJ_TEST(
    "Private exec rejects failed startup and cleans unpublished children without inventing an exit") {
  for (uint mode: {0u, 1u, 2u}) {
    auto io = kj::setupAsyncIo();
    kj::HttpHeaderTable table;
    SignalDockerService resolver(table);
    kj::TaskSet tasks(resolver);
    ExecDockerFixture fixture(table, tasks, true);
    fixture.spawnFailed = mode == 0;
    fixture.invalidStarted = mode == 1;
    fixture.exited = true;
    auto listener =
        io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    fixture.port = listener->getPort();
    auto serving = fixture.listen(*listener).eagerlyEvaluate(nullptr);
    capnp::ByteStreamFactory byteStreams;
    ChannelTokenHandler tokens(resolver);
    auto implementation = kj::refcounted<ContainerClient>(byteStreams, io.provider->getTimer(),
        io.provider->getNetwork(), kj::refcounted<PrivateExecChannel>(),
        kj::str("127.0.0.1:", fixture.port), kj::str("application"), kj::none,
        kj::str("unused-sidecar"), tasks, kj::READY_NOW, [](kj::Promise<void>) {}, tokens,
        ContainerPrivileges{});
    implementation->setHostCredentials(
        kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        kj::str("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
    rpc::Container::Client client(implementation->addRef());
    KJ_ASSERT(client.statusRequest().send().wait(io.waitScope).getRunning());
    auto request = client.execRequest();
    request.initCmd(1).set(0, "/no-shell/command");
    request.initParams();
    if (mode == 0) {
      KJ_EXPECT_THROW_MESSAGE(
          "Container exec process could not start", request.send().wait(io.waitScope));
    } else if (mode == 1) {
      KJ_EXPECT_THROW_MESSAGE("Container exec control failed", request.send().wait(io.waitScope));
    } else {
      // Drop every public capability without ever asking for exitCode.
      { auto response = request.send().wait(io.waitScope); }
    }
    io.provider->getTimer().afterDelay(25 * kj::MILLISECONDS).wait(io.waitScope);
    if (mode == 0) {
      KJ_EXPECT(fixture.runRequests == 1 && fixture.privateSignals.size() == 0);
    } else {
      KJ_EXPECT(fixture.runRequests == 1);
      if (mode == 1) {
        KJ_EXPECT(fixture.privateSignals.size() == 1 && fixture.privateSignals[0] == 9);
      } else
        KJ_EXPECT(fixture.privateSignals.size() == 0);
    }
    fixture.releaseTerminal->fulfill();
    fixture.releaseOutput->fulfill();
    io.provider->getTimer().afterDelay(25 * kj::MILLISECONDS).wait(io.waitScope);
    implementation->shutdown();
    tasks.onEmpty().wait(io.waitScope);
    KJ_EXPECT(fixture.createRequests == 1);
  }
}

KJ_TEST(
    "Private run terminal is independent of unread output and rejects lost or corrupt ownership") {
  for (uint mode: {0u, 1u, 2u, 3u, 4u}) {
    auto io = kj::setupAsyncIo();
    kj::HttpHeaderTable table;
    SignalDockerService resolver(table);
    kj::TaskSet tasks(resolver);
    ExecDockerFixture fixture(table, tasks, true);
    fixture.largeOutput = mode == 0;
    fixture.truncateTerminal = mode == 1;
    fixture.wrongTerminalIdentity = mode == 2;
    fixture.invalidRunUpgrade = mode == 4;
    auto listener =
        io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    fixture.port = listener->getPort();
    auto serving = fixture.listen(*listener).eagerlyEvaluate(nullptr);
    capnp::ByteStreamFactory byteStreams;
    ChannelTokenHandler tokens(resolver);
    auto implementation = kj::refcounted<ContainerClient>(byteStreams, io.provider->getTimer(),
        io.provider->getNetwork(), kj::refcounted<PrivateExecChannel>(),
        kj::str("127.0.0.1:", fixture.port), kj::str("application"), kj::none,
        kj::str("unused-sidecar"), tasks, kj::READY_NOW, [](kj::Promise<void>) {}, tokens,
        ContainerPrivileges{});
    implementation->setHostCredentials(
        kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        kj::str("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
    rpc::Container::Client client(implementation->addRef());
    KJ_ASSERT(client.statusRequest().send().wait(io.waitScope).getRunning());
    auto pipe = kj::newOneWayPipe();
    auto request = client.execRequest();
    request.initCmd(1).set(0, "/no-shell/command");
    request.initParams();
    request.setStdoutWriter(
        byteStreams.kjToCapnp(capnp::ExplicitEndOutputStream::wrap(kj::mv(pipe.out), []() {})));
    if (mode == 4) {
      KJ_EXPECT_THROW_MESSAGE("Container exec control failed", request.send().wait(io.waitScope));
    } else {
      auto response = request.send().wait(io.waitScope);
      auto handle = response.getProcess().getHandle();
      if (mode == 0) {
        fixture.releaseOutput->fulfill();
        io.provider->getTimer().afterDelay(25 * kj::MILLISECONDS).wait(io.waitScope);
      }
      if (mode == 3) implementation->revokeHostCallbacks();
      fixture.releaseTerminal->fulfill();
      if (mode == 0) {
        auto waiting = handle.waitRequest().send();
        KJ_EXPECT(io.provider->getTimer()
                      .timeoutAfter(kj::SECONDS, kj::mv(waiting))
                      .wait(io.waitScope)
                      .getExitCode() == 7);
        auto output = pipe.in->readAllBytes().wait(io.waitScope);
        KJ_EXPECT(output.size() == 1048576);
        for (auto value: output) KJ_EXPECT(value == 'x');
      } else {
        KJ_EXPECT_THROW_MESSAGE(
            "Container exec control failed", handle.waitRequest().send().wait(io.waitScope));
      }
    }
    if (mode != 0) fixture.releaseOutput->fulfill();
    io.provider->getTimer().afterDelay(25 * kj::MILLISECONDS).wait(io.waitScope);
    implementation->shutdown();
    tasks.onEmpty().wait(io.waitScope);
    KJ_EXPECT(fixture.runRequests == 1 && fixture.createRequests == 1);
    if (mode == 4) KJ_EXPECT(fixture.privateSignals.size() == 1 && fixture.privateSignals[0] == 9);
  }
}

KJ_TEST("Private output requires an exact drain receipt and EOF without revoking the child exit") {
  for (uint fault = 0; fault <= 7; ++fault) {
    KJ_CONTEXT(fault);
    auto io = kj::setupAsyncIo();
    kj::HttpHeaderTable table;
    SignalDockerService resolver(table);
    kj::TaskSet tasks(resolver);
    ExecDockerFixture fixture(table, tasks, true);
    fixture.drainFault = fault;
    auto listener =
        io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    fixture.port = listener->getPort();
    auto serving = fixture.listen(*listener).eagerlyEvaluate(nullptr);
    capnp::ByteStreamFactory byteStreams;
    ChannelTokenHandler tokens(resolver);
    auto implementation = kj::refcounted<ContainerClient>(byteStreams, io.provider->getTimer(),
        io.provider->getNetwork(), kj::refcounted<PrivateExecChannel>(),
        kj::str("127.0.0.1:", fixture.port), kj::str("application"), kj::none,
        kj::str("unused-sidecar"), tasks, kj::READY_NOW, [](kj::Promise<void>) {}, tokens,
        ContainerPrivileges{});
    implementation->setHostCredentials(
        kj::str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        kj::str("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
    rpc::Container::Client client(implementation->addRef());
    KJ_ASSERT(client.statusRequest().send().wait(io.waitScope).getRunning());
    auto stdoutPipe = kj::newOneWayPipe();
    auto stderrPipe = kj::newOneWayPipe();
    auto stdoutResult = stdoutPipe.in->readAllText().eagerlyEvaluate(nullptr);
    auto stderrResult = stderrPipe.in->readAllText().eagerlyEvaluate(nullptr);
    auto request = client.execRequest();
    request.initCmd(1).set(0, "/no-shell/command");
    request.initParams();
    bool stdoutAborted = false;
    bool stderrAborted = false;
    request.setStdoutWriter(byteStreams.kjToCapnp(capnp::ExplicitEndOutputStream::wrap(
        kj::mv(stdoutPipe.out), [&]() { stdoutAborted = true; })));
    request.setStderrWriter(byteStreams.kjToCapnp(capnp::ExplicitEndOutputStream::wrap(
        kj::mv(stderrPipe.out), [&]() { stderrAborted = true; })));
    auto response = request.send().wait(io.waitScope);
    request = nullptr;
    auto handle = response.getProcess().getHandle();
    fixture.releaseTerminal->fulfill();
    KJ_EXPECT(handle.waitRequest().send().wait(io.waitScope).getExitCode() == 7);
    KJ_EXPECT(!stdoutResult.poll(io.waitScope) && !stderrResult.poll(io.waitScope));
    fixture.releaseOutput->fulfill();
    KJ_EXPECT(io.provider->getTimer()
                  .timeoutAfter(kj::SECONDS, kj::mv(stdoutResult))
                  .wait(io.waitScope) == "late");
    KJ_EXPECT(io.provider->getTimer()
                  .timeoutAfter(kj::SECONDS, kj::mv(stderrResult))
                  .wait(io.waitScope) == "");
    KJ_EXPECT(stdoutAborted == (fault != 0) && stderrAborted == (fault != 0));
    KJ_EXPECT(handle.waitRequest().send().wait(io.waitScope).getExitCode() == 7);
    implementation->shutdown();
    tasks.onEmpty().wait(io.waitScope);
    KJ_EXPECT(fixture.runRequests == 1 && fixture.createRequests == 1);
  }
}

KJ_TEST("Managed application cannot capture sidecar credentials or silently restart its process") {
  capnp::MallocMessageBuilder message;
  auto create = message.initRoot<docker_api::Docker::ContainerCreateRequest>();
  configureHostContainerSecurity(create.initHostConfig());
  capnp::JsonCodec codec;
  codec.handleByAnnotation<docker_api::Docker::ContainerCreateRequest>();
  codec.setHasMode(capnp::HasMode::NON_DEFAULT);
  auto decoded =
      decodeJsonResponse<docker_api::Docker::ContainerCreateRequest>(codec.encode(create));
  auto host = decoded->getRoot<docker_api::Docker::ContainerCreateRequest>().getHostConfig();
  KJ_EXPECT(host.getRestartPolicy().getName() == "no");
  KJ_EXPECT(host.getCapDrop().size() == 1);
  KJ_EXPECT(host.getCapDrop()[0] == "NET_RAW");
  KJ_EXPECT(host.getCapAdd().size() == 0);
  KJ_EXPECT(!host.getPrivileged());
}

// Regression test for VULN-127728: ContainerInspectResponse decode must not
// use-after-free.  With the old buggy code every field access after decode
// dereferences freed heap memory (detectable by ASAN).
KJ_TEST("decodeJsonResponse ContainerInspectResponse - no use-after-free") {
  // Minimal JSON matching what Docker returns for /containers/<id>/json.
  auto json = R"({
    "Id": "abc123",
    "Created": "2025-01-01T00:00:00Z",
    "Path": "/bin/sh",
    "Args": ["--http-egress-port", "9000"],
    "State": {
      "Status": "running",
      "Running": true,
      "Paused": false,
      "Restarting": false,
      "OOMKilled": false,
      "Dead": false,
      "Pid": 42,
      "ExitCode": 0,
      "Error": "",
      "StartedAt": "2025-01-01T00:00:01Z",
      "FinishedAt": "0001-01-01T00:00:00Z"
    },
    "NetworkSettings": {
      "Bridge": "",
      "SandboxID": "",
      "HairpinMode": false,
      "LinkLocalIPv6Address": "",
      "LinkLocalIPv6PrefixLen": 0,
      "SandboxKey": "",
      "EndpointID": "",
      "Gateway": "172.17.0.1",
      "GlobalIPv6Address": "",
      "GlobalIPv6PrefixLen": 0,
      "IPAddress": "172.17.0.2",
      "IPPrefixLen": 16,
      "IPv6Gateway": "",
      "MacAddress": "02:42:ac:11:00:02",
      "Networks": {},
      "Ports": {
        "8080/tcp": [
          {"HostIp": "0.0.0.0", "HostPort": "55000"}
        ]
      }
    }
  })"_kj;

  auto message = decodeJsonResponse<docker_api::Docker::ContainerInspectResponse>(json);
  auto root = message->getRoot<docker_api::Docker::ContainerInspectResponse>();

  // Every access below would be a heap-use-after-free with the old code.
  KJ_EXPECT(root.getId() == "abc123");
  KJ_EXPECT(root.hasState());

  auto state = root.getState();
  KJ_EXPECT(state.getStatus() == "running");
  KJ_EXPECT(state.getRunning() == true);

  KJ_EXPECT(root.hasArgs());
  auto args = root.getArgs();
  KJ_EXPECT(args.size() == 2);
  KJ_EXPECT(args[0] == "--http-egress-port");
  KJ_EXPECT(args[1] == "9000");

  auto ports = root.getNetworkSettings().getPorts().getObject();
  KJ_EXPECT(ports.size() == 1);
  KJ_EXPECT(ports[0].getName() == "8080/tcp");
  auto array = ports[0].getValue().getArray();
  KJ_EXPECT(array.size() == 1);
  auto obj = array[0].getObject();
  KJ_EXPECT(obj.size() == 2);
  // HostPort is obj[1] in the order Docker returns
  auto mappedPort = obj[1].getValue().getString();
  KJ_EXPECT(mappedPort == "55000");
}

KJ_TEST("decodeJsonResponse NetworkInspectResponse - no use-after-free") {
  auto json = R"({
    "Name": "bridge",
    "Id": "net123",
    "IPAM": {
      "Driver": "default",
      "Config": [
        {"Subnet": "172.17.0.0/16", "Gateway": "172.17.0.1"}
      ]
    }
  })"_kj;

  auto message = decodeJsonResponse<docker_api::Docker::NetworkInspectResponse>(json);
  auto root = message->getRoot<docker_api::Docker::NetworkInspectResponse>();

  KJ_EXPECT(root.getName() == "bridge");
  auto ipamConfig = root.getIpam().getConfig();
  KJ_EXPECT(ipamConfig.size() == 1);
  KJ_EXPECT(ipamConfig[0].getSubnet() == "172.17.0.0/16");
  KJ_EXPECT(ipamConfig[0].getGateway() == "172.17.0.1");
}

KJ_TEST("decodeJsonResponse ContainerMonitorResponse - no use-after-free") {
  auto json = R"({"StatusCode": 0})"_kj;

  auto message = decodeJsonResponse<docker_api::Docker::ContainerMonitorResponse>(json);
  auto root = message->getRoot<docker_api::Docker::ContainerMonitorResponse>();

  KJ_EXPECT(root.getStatusCode() == 0);
}

KJ_TEST("decodeJsonResponse ContainerMonitorResponse - non-zero exit") {
  auto json = R"({"StatusCode": 137})"_kj;

  auto message = decodeJsonResponse<docker_api::Docker::ContainerMonitorResponse>(json);
  auto root = message->getRoot<docker_api::Docker::ContainerMonitorResponse>();

  KJ_EXPECT(root.getStatusCode() == 137);
}

KJ_TEST("ContainerCreateRequest encodes structured mounts with NoCopy") {
  capnp::JsonCodec codec;
  codec.handleByAnnotation<docker_api::Docker::ContainerCreateRequest>();

  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<docker_api::Docker::ContainerCreateRequest>();
  root.setImage("test-image");

  auto mounts = root.initHostConfig().initMounts(1);
  auto mount = mounts[0];
  mount.setType("volume");
  mount.setSource("snapshot-clone-volume");
  mount.setTarget("/app/data");
  mount.initVolumeOptions().setNoCopy(true);

  auto json = codec.encode(root);
  auto jsonText = json.asPtr();

  KJ_EXPECT(jsonText.contains("\"Mounts\""));
  KJ_EXPECT(jsonText.contains("\"VolumeOptions\""));
  KJ_EXPECT(jsonText.contains("\"NoCopy\":true"));

  auto decoded = decodeJsonResponse<docker_api::Docker::ContainerCreateRequest>(jsonText);
  auto decodedRoot = decoded->getRoot<docker_api::Docker::ContainerCreateRequest>();
  auto decodedMounts = decodedRoot.getHostConfig().getMounts();

  KJ_REQUIRE(decodedMounts.size() == 1);
  KJ_EXPECT(decodedMounts[0].getType() == "volume");
  KJ_EXPECT(decodedMounts[0].getSource() == "snapshot-clone-volume");
  KJ_EXPECT(decodedMounts[0].getTarget() == "/app/data");
  KJ_EXPECT(decodedMounts[0].getVolumeOptions().getNoCopy());
}

KJ_TEST("ContainerCreateRequest encodes Entrypoint as array") {
  capnp::JsonCodec codec;
  codec.handleByAnnotation<docker_api::Docker::ContainerCreateRequest>();

  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<docker_api::Docker::ContainerCreateRequest>();
  root.setImage("test-image");

  auto entrypoint = root.initEntrypoint(1);
  entrypoint.set(0, "/bin/cp");

  auto json = codec.encode(root);
  auto jsonText = json.asPtr();

  KJ_EXPECT(jsonText.contains("\"Entrypoint\":[\"/bin/cp\"]"));

  auto decoded = decodeJsonResponse<docker_api::Docker::ContainerCreateRequest>(jsonText);
  auto decodedRoot = decoded->getRoot<docker_api::Docker::ContainerCreateRequest>();
  auto decodedEntrypoint = decodedRoot.getEntrypoint();

  KJ_REQUIRE(decodedEntrypoint.size() == 1);
  KJ_EXPECT(decodedEntrypoint[0] == "/bin/cp");
}

KJ_TEST("ContainerCreateRequest encodes HostConfig Dns") {
  capnp::JsonCodec codec;
  codec.handleByAnnotation<docker_api::Docker::ContainerCreateRequest>();

  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<docker_api::Docker::ContainerCreateRequest>();
  root.setImage("test-image");

  auto dns = root.initHostConfig().initDns(2);
  dns.set(0, "1.1.1.1");
  dns.set(1, "8.8.8.8");

  auto json = codec.encode(root);
  auto jsonText = json.asPtr();

  KJ_EXPECT(jsonText.contains("\"Dns\""));
  KJ_EXPECT(jsonText.contains("1.1.1.1"));
  KJ_EXPECT(jsonText.contains("8.8.8.8"));

  auto decoded = decodeJsonResponse<docker_api::Docker::ContainerCreateRequest>(jsonText);
  auto decodedRoot = decoded->getRoot<docker_api::Docker::ContainerCreateRequest>();
  auto decodedDns = decodedRoot.getHostConfig().getDns();

  KJ_REQUIRE(decodedDns.size() == 2);
  KJ_EXPECT(decodedDns[0] == "1.1.1.1");
  KJ_EXPECT(decodedDns[1] == "8.8.8.8");
}

KJ_TEST("ContainerCreateRequest encodes configured container privileges") {
  capnp::JsonCodec codec;
  codec.handleByAnnotation<docker_api::Docker::ContainerCreateRequest>();

  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<docker_api::Docker::ContainerCreateRequest>();
  root.setImage("test-image");

  auto hostConfig = root.initHostConfig();
  ContainerPrivileges privileges{
    .capabilities = kj::arr(kj::str("SYS_ADMIN")),
    .devices = kj::arr(ContainerPrivileges::Device{
      .pathOnHost = kj::str("/dev/fuse"),
      .pathInContainer = kj::str("/dev/fuse"),
      .cgroupPermissions = kj::str("rwm"),
    }),
    .securityOpt = kj::arr(kj::str("apparmor:unconfined")),
  };
  configureContainerPrivileges(hostConfig, privileges);

  auto json = codec.encode(root);
  auto jsonText = json.asPtr();

  KJ_EXPECT(jsonText.contains("\"CapAdd\""));
  KJ_EXPECT(jsonText.contains("\"SYS_ADMIN\""));
  KJ_EXPECT(jsonText.contains("\"Devices\""));
  KJ_EXPECT(jsonText.contains("\"PathOnHost\":\"/dev/fuse\""));
  KJ_EXPECT(jsonText.contains("\"PathInContainer\":\"/dev/fuse\""));
  KJ_EXPECT(jsonText.contains("\"CgroupPermissions\":\"rwm\""));
  KJ_EXPECT(jsonText.contains("\"SecurityOpt\""));
  KJ_EXPECT(jsonText.contains("\"apparmor:unconfined\""));

  auto decoded = decodeJsonResponse<docker_api::Docker::ContainerCreateRequest>(jsonText);
  auto decodedRoot = decoded->getRoot<docker_api::Docker::ContainerCreateRequest>();
  auto decodedHostConfig = decodedRoot.getHostConfig();
  auto decodedCapAdd = decodedHostConfig.getCapAdd();
  auto decodedDevices = decodedHostConfig.getDevices();
  auto decodedSecurityOpt = decodedHostConfig.getSecurityOpt();

  KJ_REQUIRE(decodedCapAdd.size() == 1);
  KJ_EXPECT(decodedCapAdd[0] == "SYS_ADMIN");

  KJ_REQUIRE(decodedDevices.size() == 1);
  KJ_EXPECT(decodedDevices[0].getPathOnHost() == "/dev/fuse");
  KJ_EXPECT(decodedDevices[0].getPathInContainer() == "/dev/fuse");
  KJ_EXPECT(decodedDevices[0].getCgroupPermissions() == "rwm");

  KJ_REQUIRE(decodedSecurityOpt.size() == 1);
  KJ_EXPECT(decodedSecurityOpt[0] == "apparmor:unconfined");
}

KJ_TEST("ContainerCreateRequest omits empty container privileges") {
  capnp::JsonCodec codec;
  codec.handleByAnnotation<docker_api::Docker::ContainerCreateRequest>();

  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<docker_api::Docker::ContainerCreateRequest>();
  root.setImage("test-image");

  auto hostConfig = root.initHostConfig();
  ContainerPrivileges privileges;
  configureContainerPrivileges(hostConfig, privileges);

  auto json = codec.encode(root);
  auto jsonText = json.asPtr();

  KJ_EXPECT(!jsonText.contains("\"CapAdd\""));
  KJ_EXPECT(!jsonText.contains("\"SYS_ADMIN\""));
  KJ_EXPECT(!jsonText.contains("\"Devices\""));
  KJ_EXPECT(!jsonText.contains("\"PathOnHost\""));
  KJ_EXPECT(!jsonText.contains("\"PathInContainer\""));
  KJ_EXPECT(!jsonText.contains("\"CgroupPermissions\""));
  KJ_EXPECT(!jsonText.contains("\"SecurityOpt\""));
  KJ_EXPECT(!jsonText.contains("\"apparmor:unconfined\""));

  auto decoded = decodeJsonResponse<docker_api::Docker::ContainerCreateRequest>(jsonText);
  auto decodedRoot = decoded->getRoot<docker_api::Docker::ContainerCreateRequest>();
  auto decodedHostConfig = decodedRoot.getHostConfig();

  KJ_EXPECT(decodedHostConfig.getCapAdd().size() == 0);
  KJ_EXPECT(decodedHostConfig.getDevices().size() == 0);
  KJ_EXPECT(decodedHostConfig.getSecurityOpt().size() == 0);
}

}  // namespace
}  // namespace workerd::server
