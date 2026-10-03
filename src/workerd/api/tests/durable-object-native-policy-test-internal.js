import wrapped from 'cloudflare-internal:wrapped-binding';

// Native API fixture. Product HMAC, routing and persistence are covered by their owning tests.
function policy(raw, id) {
  return {
    fetch(input, init) {
      const request = new Request(input, init);
      return raw.fetch(new Request('https://transport/' + id, request));
    },
    connect() {
      throw Error('DO_RPC_UNSUPPORTED');
    },
    echo(value) {
      const result = raw.echo(id, value);
      return new Proxy(() => undefined, {
        get(_target, name) {
          if (name === 'then') return result.then.bind(result);
          if (name === Symbol.dispose) {
            if (value === 'dispose-failure')
              return () => {
                throw Error('fixture dispose failure');
              };
            return result[Symbol.dispose].bind(result);
          }
          return Reflect.get(result, name, result);
        },
        apply(_target, _receiver, args) {
          return Reflect.apply(result, result, args);
        },
      });
    },
    id() {
      return raw.identity(id);
    },
    name() {
      return raw.name(id);
    },
    release: {
      then(fulfilled, rejected) {
        return raw.name(id).then(fulfilled, rejected);
      },
    },
  };
}

export function createDurableObjectStubPolicy({ fetcher, id }) {
  return policy(fetcher, id);
}

export default function bootstrap(env, privateEnv) {
  const raw = privateEnv.RAW;
  const value = 'a'.repeat(64);
  const id = () => wrapped.createDurableObjectId(value, 'alpha');
  const get = (identity) =>
    wrapped.createDurableObjectStub(identity, raw, policy(raw, value));
  const namespace = wrapped.createDurableObjectNamespace({
    newUniqueId: id,
    idFromName: id,
    idFromString: id,
    get,
    getByName: () => get(id()),
    jurisdiction: () => namespace,
  });
  env.NS = namespace;
  env.PIPE = privateEnv.PIPE;
  env.CHECK = () => {
    const equal = (actual, expected) => {
      if (actual !== expected) throw Error('native brand check failed');
    };
    const hostile = new Proxy(
      {},
      {
        get() {
          throw Error('native brand checks must not read properties');
        },
      }
    );
    equal(wrapped.isRpcStub(hostile), false);
    equal(wrapped.isRpcStub(null), false);
    equal(
      wrapped.isRpcStub(() => {}),
      false
    );
    const stub = wrapped.createServiceRpcStub({
      echo: (value) => value,
      [Symbol.dispose]() {},
    });
    equal(wrapped.isRpcStub(stub), true);
    equal(wrapped.isRpcStub(Object.create(Object.getPrototypeOf(stub))), false);
    equal(stub.constructor.name, 'RpcStub');
    stub[Symbol.dispose]();
  };
  return (exports) => exports;
}
