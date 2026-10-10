#pragma once

#include <workerd/api/js-readable-stream.h>
#include <workerd/io/compatibility-date.capnp.h>
#include <workerd/io/compatibility-date.h>
#include <workerd/io/io-channels.h>
#include <workerd/io/io-own.h>
#include <workerd/io/worker.h>
#include <workerd/jsg/setup.h>

namespace workerd::api {

class HostContainer;

class Fetcher;
class DurableObjectClass;
class DurableObjectFacets;

// JS stub pointing to a remote Worker loaded using WorkerLoader. This is not a stub for a specific
// entrypoint, but instead the entire Worker, allowing the caller to call any entrypoint (and
// specify arbitrary props).
class WorkerStub: public jsg::Object {
 public:
  WorkerStub(IoOwn<WorkerStubChannel> channel): channel(kj::mv(channel)) {}

  struct EntrypointOptions {
    jsg::Optional<jsg::JsRef<jsg::JsObject>> props;
    jsg::Optional<ResourceLimits> limits;

    JSG_STRUCT(props, limits);
  };

  jsg::Ref<Fetcher> getEntrypoint(jsg::Lock& js,
      jsg::Optional<kj::Maybe<kj::String>> name,
      jsg::Optional<EntrypointOptions> options);
  jsg::Ref<DurableObjectClass> getDurableObjectClass(jsg::Lock& js,
      jsg::Optional<kj::Maybe<kj::String>> name,
      jsg::Optional<EntrypointOptions> options);

  JSG_RESOURCE_TYPE(WorkerStub, CompatibilityFlags::Reader flags) {
    JSG_METHOD(getEntrypoint);
    JSG_METHOD(getDurableObjectClass);

    JSG_TS_OVERRIDE({
      getEntrypoint<T extends Rpc.WorkerEntrypointBranded | undefined>(
          name?: string, options?: WorkerStubEntrypointOptions): Fetcher<T>;
      getDurableObjectClass<T extends Rpc.DurableObjectBranded | undefined>(
          name?: string, options?: WorkerStubEntrypointOptions): DurableObjectClass<T>;
    });
  }

 private:
  IoOwn<WorkerStubChannel> channel;
};

// JS interface for worker loader binding.
class WorkerLoader: public jsg::Object {
 public:
  // Create a WorkerLoader backed by the given I/O channel.
  //
  // `compatDateValidation` will differ between workerd vs. production.
  explicit WorkerLoader(uint channel,
      CompatibilityDateValidation compatDateValidation,
      bool allowOpenComputePrivateEnv = false)
      : channel(channel),
        compatDateValidation(compatDateValidation),
        allowOpenComputePrivateEnv(allowOpenComputePrivateEnv) {}

  explicit WorkerLoader(IoOwn<IoChannelFactory::WorkerLoaderChannel> channel,
      CompatibilityDateValidation compatDateValidation,
      bool allowOpenComputePrivateEnv = false)
      : channel(kj::mv(channel)),
        compatDateValidation(compatDateValidation),
        allowOpenComputePrivateEnv(allowOpenComputePrivateEnv) {}

  struct Module {
    // Exactly one must be filled in.
    jsg::Optional<kj::String> js;               // ES module
    jsg::Optional<kj::String> cjs;              // Common JS module
    jsg::Optional<kj::String> text;             // text blob, imports as a string
    jsg::Optional<kj::Array<const byte>> data;  // byte blob, imports as ArrayBuffer
    jsg::Optional<jsg::Value> json;             // arbitrary JS value, will be serialized to JSON
                                                // and then parsed again when imported

    jsg::Optional<kj::String> py;  // Python module

    // WASM module, given either as bytes to compile, or an already-compiled WebAssembly.Module
    // whose compiled code is shared with the loaded worker.
    jsg::Optional<kj::OneOf<kj::Array<const byte>, jsg::V8Ref<v8::WasmModuleObject>>> wasm;

    JSG_STRUCT(js, cjs, text, data, json, py, wasm);

    JSG_STRUCT_TS_OVERRIDE(WorkerLoaderModule {
      wasm?: ArrayBuffer | ArrayBufferView | WebAssembly.Module;
    });

    // HACK: When we serialize the JSON in extractSource() we need to place the owned kj::String
    //   somewhere since Worker::Script::Source only gets a kj::StringPtr.
    kj::Maybe<kj::String> serializedJson;
  };

  struct WorkerCode {
    kj::String compatibilityDate;
    jsg::Optional<kj::Array<kj::String>> compatibilityFlags;
    jsg::Optional<bool> allowExperimental = false;

    jsg::Optional<ResourceLimits> limits;

    kj::String mainModule;

    // Modules are specified as an object mapping names to content. If the content is just a
    // string, an ES module is assumed for '.js' files and a Python module for '.py' files. For
    // Python dynamic workers, any other file name is treated as a text module as long as it's
    // in the python_modules/ directory. If the content is a `WebAssembly.Module` (e.g. obtained
    // via a source phase import), it becomes a WASM module sharing the already-compiled code.
    // If the content is any other object, the type of module is determined based on which
    // property is set.
    jsg::Dict<kj::OneOf<jsg::V8Ref<v8::WasmModuleObject>, Module, kj::String>> modules;

    // Any RPC-serializable value!
    jsg::Optional<jsg::JsRef<jsg::JsObject>> env;

    // Host-owned bindings, never exposed through the importable cloudflare:workers env.
    // With openComputeHostPolicy they are delivered only to the config-owned policy factory;
    // otherwise the host's handler adapter receives and filters them.
    jsg::Optional<jsg::JsRef<jsg::JsObject>> openComputePrivateEnv;

    struct HostBinding {
      kj::String kind;
      jsg::Ref<Fetcher> fetcher;
      jsg::Optional<kj::String> wrapperModule;

      JSG_STRUCT(kind, fetcher, wrapperModule);
    };

    // Typed product capabilities constructed only by a host-issued private loader. The raw
    // fetcher is materialized as a native or internal wrapped binding in the child isolate.
    jsg::Optional<jsg::Dict<HostBinding>> openComputeBindings;

    // Execute the fixed config-owned environment/export policy before tenant entrypoints.
    jsg::Optional<bool> openComputeHostPolicy;

    jsg::Optional<jsg::Ref<HostContainer>> openComputeContainer;

    // Dedicated Python snapshot selected and authenticated by the trusted host.
    jsg::Optional<kj::Array<const byte>> openComputePythonSnapshot;

    // `Fetcher` (e.g. service binding) representing the loaded worker's global outbound.
    //
    // If omitted, inherit the current worker's global outbound.
    //
    // If `null`, block the global outbound (all requests throw errors).
    jsg::Optional<kj::Maybe<jsg::Ref<Fetcher>>> globalOutbound;

    // Cache API backend selected only by a host-issued private loader. It is not an env binding.
    jsg::Optional<jsg::Ref<Fetcher>> openComputeCache;

    // Specify tail workers.
    jsg::Optional<kj::Array<jsg::Ref<Fetcher>>> tails;
    jsg::Optional<kj::Array<jsg::Ref<Fetcher>>> streamingTails;

    JSG_STRUCT(compatibilityDate,
        compatibilityFlags,
        allowExperimental,
        limits,
        mainModule,
        modules,
        env,
        openComputePrivateEnv,
        openComputeBindings,
        openComputeHostPolicy,
        openComputeContainer,
        openComputePythonSnapshot,
        globalOutbound,
        openComputeCache,
        tails,
        streamingTails);

    JSG_STRUCT_TS_OVERRIDE(WorkerLoaderWorkerCode {
      modules: Record<string, string | WebAssembly.Module | WorkerLoaderModule>;
    });
  };

  jsg::Ref<WorkerStub> get(
      jsg::Lock& js, kj::Maybe<kj::String> name, jsg::Function<jsg::Promise<WorkerCode>()> getCode);

  // Shortcut for `get(null, () => code)`.
  jsg::Ref<WorkerStub> load(jsg::Lock& js, WorkerCode code);

  // Host-only deploy-time validation. Returns a dedicated snapshot without invoking a handler.
  jsg::Promise<jsg::JsRef<jsg::JsUint8Array>> preparePython(jsg::Lock& js, WorkerCode code);

  void serialize(jsg::Lock& js, jsg::Serializer& serializer);
  static jsg::Ref<WorkerLoader> deserialize(
      jsg::Lock& js, rpc::SerializationTag tag, jsg::Deserializer& deserializer);

  JSG_RESOURCE_TYPE(WorkerLoader) {
    JSG_METHOD(get);
    JSG_METHOD(load);
    JSG_METHOD(preparePython);

    JSG_TS_ROOT();
  }

  JSG_SERIALIZABLE(rpc::SerializationTag::WORKER_LOADER);

 private:
  kj::OneOf<uint, IoOwn<IoChannelFactory::WorkerLoaderChannel>> channel;
  CompatibilityDateValidation compatDateValidation;
  bool allowOpenComputePrivateEnv;

  kj::Own<WorkerStubChannel> loadIsolate(IoContext& ioctx,
      kj::Maybe<kj::String> name,
      kj::Function<kj::Promise<DynamicWorkerSource>()> fetchSource);

  static DynamicWorkerSource toDynamicWorkerSource(jsg::Lock& js,
      IoContext& ioctx,
      CompatibilityDateValidation compatDateValidation,
      bool allowOpenComputePrivateEnv,
      WorkerCode code);

  static Worker::Script::Source extractSource(jsg::Lock& js, WorkerCode& code);
  static kj::Own<CompatibilityFlags::Reader> extractCompatFlags(
      jsg::Lock& js, WorkerCode& code, CompatibilityDateValidation compatDateValidation);

  kj::Promise<kj::Own<const Worker>> startWorker(
      Worker::Script::Source extractedSource, CompatibilityFlags::Reader compatibilityFlags);
};

// A host facet grant used by trusted wrappers. It is not a public Worker Loader API and cannot
// be minted by a tenant. The received grant cannot be transferred again.
class HostFacets: public jsg::Object {
 public:
  explicit HostFacets(uint channel): channel(channel) {}
  explicit HostFacets(IoOwn<IoChannelFactory::HostFacetChannel> channel)
      : channel(kj::mv(channel)) {}

  void create(jsg::Lock& js,
      kj::String name,
      uint depth,
      kj::String id,
      jsg::Ref<DurableObjectClass> actorClass);
  void revoke(jsg::Lock& js);
  void serialize(jsg::Lock& js, jsg::Serializer& serializer);
  static jsg::Ref<HostFacets> deserialize(
      jsg::Lock& js, rpc::SerializationTag tag, jsg::Deserializer& deserializer);

  JSG_RESOURCE_TYPE(HostFacets) {
    JSG_METHOD(create);
    JSG_METHOD(revoke);
  }
  JSG_SERIALIZABLE(rpc::SerializationTag::HOST_FACETS);

 private:
  kj::OneOf<uint, IoOwn<IoChannelFactory::HostFacetChannel>> channel;
};

// Host-only binding for minting and revoking isolated loader capabilities. It is deliberately
// separate from WorkerLoader and cannot be transferred into a dynamic Worker's env.
class WorkerLoaderFactory: public jsg::Object {
 public:
  explicit WorkerLoaderFactory(uint channel): channel(channel) {}

  // The host must refuse private capability assembly without native env isolation.
  uint getHostPolicyVersion() const {
    return 1;
  }

  jsg::Ref<WorkerLoader> get(jsg::Lock& js, kj::String name);
  jsg::Ref<WorkerLoader> getPrivate(jsg::Lock& js, kj::String name);
  void revoke(jsg::Lock& js, kj::String name);
  void revokePrefix(jsg::Lock& js, kj::String prefix);
  jsg::Ref<HostFacets> getFacets(jsg::Lock& js, jsg::Ref<DurableObjectFacets> facets);
  jsg::Ref<HostContainer> getContainer(jsg::Lock& js,
      jsg::Ref<DurableObjectFacets> facets,
      kj::String className,
      jsg::Function<jsg::Promise<jsg::Optional<HostContainerLease>>(
          bool, bool, bool, kj::String, kj::String)> resolve);
  jsg::Ref<Fetcher> getEntrypoint(jsg::Lock& js,
      jsg::Ref<WorkerStub> stub,
      kj::Array<jsg::Ref<Fetcher>> tails,
      jsg::Optional<kj::Maybe<kj::String>> name,
      jsg::Optional<WorkerStub::EntrypointOptions> options);

  JSG_RESOURCE_TYPE(WorkerLoaderFactory) {
    JSG_READONLY_PROTOTYPE_PROPERTY(hostPolicyVersion, getHostPolicyVersion);
    JSG_METHOD(get);
    JSG_METHOD(getPrivate);
    JSG_METHOD(revoke);
    JSG_METHOD(revokePrefix);
    JSG_METHOD(getFacets);
    JSG_METHOD(getContainer);
    JSG_METHOD(getEntrypoint);
  }

 private:
  uint channel;
};

class HostContainer: public jsg::Object {
 public:
  explicit HostContainer(IoOwn<IoChannelFactory::HostContainerChannel> channel)
      : channel(kj::mv(channel)) {}
  kj::Own<IoChannelFactory::HostContainerChannel> getChannel() {
    return kj::addRef(*channel);
  }
  jsg::Promise<void> start(jsg::Lock& js);
  void revoke() {
    channel->revoke();
  }
  JSG_RESOURCE_TYPE(HostContainer) {
    JSG_METHOD(start);
    JSG_METHOD(revoke);
  }

 private:
  IoOwn<IoChannelFactory::HostContainerChannel> channel;
};

// A single native-extension session. It may be delegated once into a dynamic Worker's env and
// cannot be persisted or transferred over RPC.
class HostExtensionPort: public jsg::Object {
 public:
  explicit HostExtensionPort(uint channel): channel(channel) {}
  explicit HostExtensionPort(IoOwn<IoChannelFactory::HostExtensionChannel> channel)
      : channel(kj::mv(channel)) {}

  jsg::Promise<jsg::JsRef<jsg::JsUint8Array>> call(
      jsg::Lock& js, uint32_t method, jsg::JsBufferSource payload);
  JsReadableStream stream(jsg::Lock& js, uint32_t method, jsg::JsBufferSource payload);
  void serialize(jsg::Lock& js, jsg::Serializer& serializer);
  static jsg::Ref<HostExtensionPort> deserialize(
      jsg::Lock& js, rpc::SerializationTag tag, jsg::Deserializer& deserializer);

  JSG_RESOURCE_TYPE(HostExtensionPort) {
    JSG_METHOD(call);
    JSG_METHOD(stream);
  }
  JSG_SERIALIZABLE(rpc::SerializationTag::HOST_EXTENSION_PORT);

 private:
  kj::OneOf<uint, IoOwn<IoChannelFactory::HostExtensionChannel>> channel;
};

// Host-only factory binding. Only static extension facade Workers receive this object.
class HostExtensionFactory: public jsg::Object {
 public:
  explicit HostExtensionFactory(uint channel): channel(channel) {}

  jsg::Ref<HostExtensionPort> get(jsg::Lock& js, kj::String identity);

  JSG_RESOURCE_TYPE(HostExtensionFactory) {
    JSG_METHOD(get);
  }

 private:
  uint channel;
};

#define EW_WORKER_LOADER_ISOLATE_TYPES                                                             \
  api::WorkerStub, api::WorkerStub::EntrypointOptions, api::WorkerLoader,                          \
      api::WorkerLoader::Module, api::WorkerLoader::WorkerCode,                                    \
      api::WorkerLoader::WorkerCode::HostBinding, api::WorkerLoaderFactory, api::HostFacets,       \
      api::HostContainer, workerd::HostContainerLease, api::HostExtensionPort,                     \
      api::HostExtensionFactory, workerd::ResourceLimits

}  // namespace workerd::api
