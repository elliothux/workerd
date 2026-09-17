@0xa6cf773127c85b37;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("workerd::rpc");

# Direct, session-scoped data plane between workerd and an operator-owned native provider.
interface HostExtension {
  call @0 (method :UInt32, payload :Data) -> (payload :Data);
  openStream @1 (method :UInt32, payload :Data) -> (stream :HostExtensionStream);
}

interface HostExtensionStream {
  read @0 (maxBytes :UInt32) -> (payload :Data, eof :Bool);
  cancel @1 ();
}
