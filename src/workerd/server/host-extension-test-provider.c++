#include <workerd/io/host-extension.capnp.h>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/debug.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <vector>

namespace workerd::server {
namespace {

constexpr uint32_t LIST_METHOD = 1;
constexpr uint32_t READ_METHOD = 2;
constexpr uint64_t MAX_FILE_SIZE = 64 * 1024 * 1024;

kj::OwnFd openRelative(capnp::Data::Reader encoded, bool directory) {
  KJ_REQUIRE(encoded.size() > 0 && encoded.size() <= 4096, "invalid fixture path");
  auto path = kj::heapString(encoded.asChars());
  KJ_REQUIRE(path[0] != '/' && path.findFirst('\\') == kj::none && path.findFirst('\0') == kj::none,
      "invalid fixture path");

  int root;
  KJ_SYSCALL(root = open(".", O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  kj::OwnFd parent(root);
  auto remaining = path.asPtr();
  while (remaining.size() > 0) {
    auto slash = remaining.findFirst('/');
    auto component = slash.orDefault(remaining.size());
    auto name = remaining.slice(0, component);
    KJ_REQUIRE(name.size() > 0 && name != kj::StringPtr(".") && name != kj::StringPtr(".."),
        "invalid fixture path");
    bool last = component == remaining.size();
    int flags = O_RDONLY | O_CLOEXEC | O_NOFOLLOW;
    if (!last || directory) flags |= O_DIRECTORY;
    kj::String terminated = kj::str(name);
    int next;
    KJ_SYSCALL(next = openat(parent.get(), terminated.cStr(), flags));
    parent = kj::OwnFd(next);
    if (last) break;
    remaining = remaining.slice(component + 1);
  }
  return parent;
}

class FileStream final: public rpc::HostExtensionStream::Server {
 public:
  explicit FileStream(kj::OwnFd fd): fd(kj::mv(fd)) {}

  kj::Promise<void> read(ReadContext context) override {
    auto maxBytes = context.getParams().getMaxBytes();
    KJ_REQUIRE(maxBytes > 0 && maxBytes <= 64 * 1024, "invalid fixture read size");
    auto buffer = kj::heapArray<kj::byte>(maxBytes);
    ssize_t amount;
    KJ_SYSCALL(amount = ::read(fd.get(), buffer.begin(), buffer.size()));
    auto results = context.getResults(capnp::MessageSize{amount / sizeof(capnp::word) + 1, 0});
    results.setPayload(buffer.slice(0, amount));
    results.setEof(amount == 0);
    return kj::READY_NOW;
  }

  kj::Promise<void> cancel(CancelContext) override {
    fd = kj::OwnFd();
    return kj::READY_NOW;
  }

 private:
  kj::OwnFd fd;
};

class FileProvider final: public rpc::HostExtension::Server {
 public:
  kj::Promise<void> call(CallContext context) override {
    auto params = context.getParams();
    KJ_REQUIRE(params.getMethod() == LIST_METHOD, "unsupported fixture method");
    auto fd = openRelative(params.getPayload(), true);
    DIR* raw = fdopendir(fd.release());
    KJ_REQUIRE(raw != nullptr, "fixture directory unavailable");
    auto directory = kj::defer([raw]() { closedir(raw); });
    std::vector<std::string> names;
    errno = 0;
    while (auto* entry = readdir(raw)) {
      if (kj::StringPtr(entry->d_name) == "." || kj::StringPtr(entry->d_name) == "..") continue;
      struct stat stat;
      KJ_SYSCALL(fstatat(dirfd(raw), entry->d_name, &stat, AT_SYMLINK_NOFOLLOW));
      if (S_ISREG(stat.st_mode)) names.emplace_back(entry->d_name);
    }
    KJ_REQUIRE(errno == 0, "fixture directory unavailable");
    std::sort(names.begin(), names.end());
    kj::StringTree listing;
    for (auto& name: names) {
      listing = kj::strTree(kj::mv(listing), kj::StringPtr(name.data(), name.size()), '\n');
    }
    auto bytes = listing.flatten();
    context.getResults(capnp::MessageSize{bytes.size() / sizeof(capnp::word) + 1, 0})
        .setPayload(bytes.asBytes());
    return kj::READY_NOW;
  }

  kj::Promise<void> openStream(OpenStreamContext context) override {
    auto params = context.getParams();
    KJ_REQUIRE(params.getMethod() == READ_METHOD, "unsupported fixture method");
    auto fd = openRelative(params.getPayload(), false);
    struct stat stat;
    KJ_SYSCALL(fstat(fd.get(), &stat));
    KJ_REQUIRE(S_ISREG(stat.st_mode) && stat.st_size >= 0 &&
            static_cast<uint64_t>(stat.st_size) <= MAX_FILE_SIZE,
        "fixture file unavailable");
    context.getResults().setStream(kj::heap<FileStream>(kj::mv(fd)));
    return kj::READY_NOW;
  }
};

class Session {
 public:
  explicit Session(kj::Own<kj::AsyncCapabilityStream> stream): server(kj::heap<FileProvider>()) {
    server.accept(kj::mv(stream), 0);
  }

  kj::Promise<void> run() {
    co_await server.drain();
  }

 private:
  capnp::TwoPartyServer server;
};

class SessionErrors final: public kj::TaskSet::ErrorHandler {
 public:
  void taskFailed(kj::Exception&& exception) override {
    KJ_LOG(ERROR, "host extension fixture session failed", exception);
  }
};

kj::Promise<void> runProvider(kj::AsyncIoContext& io) {
  auto control =
      io.lowLevelProvider->wrapUnixSocketFd(3, kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
  SessionErrors errors;
  kj::TaskSet sessions(errors);
  for (;;) {
    kj::byte magic[4];
    kj::OwnFd fd;
    auto result = co_await control->tryReadWithFds(magic, sizeof(magic), sizeof(magic), &fd, 1);
    if (result.byteCount == 0) co_return;
    KJ_REQUIRE(result.byteCount == sizeof(magic) && result.capCount == 1 &&
            memcmp(magic, "OCP1", sizeof(magic)) == 0,
        "invalid host extension fixture attach");
    auto session = kj::heap<Session>(io.lowLevelProvider->wrapUnixSocketFd(kj::mv(fd)));
    sessions.add(session->run().attach(kj::mv(session)));
    const kj::byte ack = 0;
    co_await control->write(kj::arrayPtr(ack));
  }
}

}  // namespace
}  // namespace workerd::server

int main() {
  auto io = kj::setupAsyncIo();
  workerd::server::runProvider(io).wait(io.waitScope);
  return 0;
}
