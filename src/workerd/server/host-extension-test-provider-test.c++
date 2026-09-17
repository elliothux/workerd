#include <workerd/io/host-extension.capnp.h>

#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/debug.h>
#include <kj/test.h>

#include <cstdlib>
#include <cstring>

namespace workerd::server {
namespace {

void writeFile(kj::StringPtr path, kj::StringPtr content) {
  int fd;
  KJ_SYSCALL(fd = open(path.cStr(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
  kj::OwnFd owned(fd);
  KJ_SYSCALL(write(fd, content.begin(), content.size()), content.size());
}

void sendSession(int control, int session) {
  char magic[] = "OCP1";
  struct iovec payload = {.iov_base = magic, .iov_len = 4};
  char ancillary[CMSG_SPACE(sizeof(session))] = {};
  struct msghdr message = {
    .msg_iov = &payload,
    .msg_iovlen = 1,
    .msg_control = ancillary,
    .msg_controllen = sizeof(ancillary),
  };
  auto* header = CMSG_FIRSTHDR(&message);
  header->cmsg_level = SOL_SOCKET;
  header->cmsg_type = SCM_RIGHTS;
  header->cmsg_len = CMSG_LEN(sizeof(session));
  memcpy(CMSG_DATA(header), &session, sizeof(session));
  KJ_SYSCALL(sendmsg(control, &message, 0), 4);
}

KJ_TEST("host extension provider accepts an FD session and serves unary and stream calls") {
  auto* testRoot = getenv("TEST_SRCDIR");
  auto* workspace = getenv("TEST_WORKSPACE");
  KJ_REQUIRE(testRoot != nullptr && workspace != nullptr, "Bazel runfiles are unavailable");
  auto provider =
      kj::str(testRoot, '/', workspace, "/src/workerd/server/host-extension-test-provider");

  char path[] = "/tmp/workerd-host-extension-XXXXXX";
  KJ_REQUIRE(mkdtemp(path) != nullptr, "failed to create fixture directory");
  auto directory = kj::str(path, "/invoices");
  KJ_SYSCALL(mkdir(directory.cStr(), 0700));
  auto first = kj::str(directory, "/a.txt");
  auto second = kj::str(directory, "/b.txt");
  writeFile(first, "alpha");
  writeFile(second, "beta");
  KJ_DEFER({
    unlink(first.cStr());
    unlink(second.cStr());
    rmdir(directory.cStr());
    rmdir(path);
  });

  int control[2];
  int session[2];
  KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM, 0, control));
  KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM, 0, session));
  pid_t child;
  KJ_SYSCALL(child = fork());
  if (child == 0) {
    close(control[0]);
    if (control[1] != 3) {
      dup2(control[1], 3);
      close(control[1]);
    }
    close(session[0]);
    close(session[1]);
    chdir(path);
    execl(provider.cStr(), provider.cStr(), nullptr);
    _exit(127);
  }
  close(control[1]);
  bool reaped = false;
  KJ_DEFER({
    close(control[0]);
    if (!reaped) {
      kill(child, SIGKILL);
      waitpid(child, nullptr, 0);
    }
  });

  sendSession(control[0], session[1]);
  close(session[1]);
  char ack = 1;
  KJ_SYSCALL(read(control[0], &ack, 1), 1);
  KJ_EXPECT(ack == 0);

  auto io = kj::setupAsyncIo();
  {
    auto stream = io.lowLevelProvider->wrapUnixSocketFd(
        session[0], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
    capnp::TwoPartyClient client(*stream, 0);
    auto extension = client.bootstrap().castAs<rpc::HostExtension>();

    auto list = extension.callRequest();
    list.setMethod(1);
    list.setPayload(kj::StringPtr("invoices").asBytes());
    auto listResponse = list.send().wait(io.waitScope);
    auto listing = listResponse.getPayload();
    KJ_EXPECT(kj::StringPtr(reinterpret_cast<const char*>(listing.begin()), listing.size()) ==
        "a.txt\nb.txt\n");

    auto invalid = extension.callRequest();
    invalid.setMethod(1);
    invalid.setPayload(kj::StringPtr("../invoices").asBytes());
    KJ_EXPECT_THROW(FAILED, invalid.send().wait(io.waitScope));

    auto open = extension.openStreamRequest();
    open.setMethod(2);
    open.setPayload(kj::StringPtr("invoices/a.txt").asBytes());
    auto opened = open.send().wait(io.waitScope);
    auto remote = opened.getStream();
    kj::Vector<kj::byte> bytes;
    bool eof = false;
    while (!eof) {
      auto read = remote.readRequest();
      read.setMaxBytes(2);
      auto response = read.send().wait(io.waitScope);
      bytes.addAll(response.getPayload().asBytes());
      eof = response.getEof();
    }
    KJ_EXPECT(kj::StringPtr(reinterpret_cast<const char*>(bytes.begin()), bytes.size()) == "alpha");
  }

  close(control[0]);
  control[0] = -1;
  int status;
  KJ_SYSCALL(waitpid(child, &status, 0), child);
  reaped = true;
  KJ_EXPECT(WIFEXITED(status) && WEXITSTATUS(status) == 0, status);
}

}  // namespace
}  // namespace workerd::server
