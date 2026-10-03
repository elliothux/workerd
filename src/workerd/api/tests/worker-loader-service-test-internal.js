const get = Reflect.get;
const apply = Reflect.apply;
const NativeProxy = Proxy;
const registered = new WeakMap();
import entrypoints from 'cloudflare-internal:workers';

function capabilityPolicy(callable) {
  let disposed = false;
  const echo = (value) => {
    if (disposed) throw Error('SERVICE_BINDING_DENIED');
    return value;
  };
  const policy = callable ? echo : { echo };
  policy.fail = () => {
    throw new TypeError('fixture rejection');
  };
  policy.dup = () => {
    if (disposed) throw Error('SERVICE_BINDING_DENIED');
    return capabilityPolicy(callable);
  };
  policy[Symbol.dispose] = () => {
    disposed = true;
  };
  return policy;
}

function capability(callable) {
  return wrapped.createServiceRpcStub(capabilityPolicy(callable));
}

export function createServiceBinding({ fetcher }) {
  const methods = {
    fetch(input, init) {
      const request = new Request(input, init);
      request.headers.set('x-policy', 'trusted');
      return fetcher.fetch(request);
    },
    echo(value) {
      return fetcher.echo(value);
    },
    target() {
      return capability(false);
    },
    callback() {
      return capability(true);
    },
    connect(address, options) {
      return fetcher.connect(address, options);
    },
  };
  return new NativeProxy(methods, {
    get(owner, name) {
      if (name === 'then') return undefined;
      if (!Object.hasOwn(owner, name)) throw Error('SERVICE_BINDING_DENIED');
      const method = get(owner, name);
      return (...args) => apply(method, owner, args);
    },
  });
}

export function registerServiceBinding(publicBinding, privateBinding) {
  registered.set(publicBinding, privateBinding);
}

export default function bootstrap(env) {
  if (env.SERVICE !== undefined && !registered.has(env.SERVICE)) {
    throw Error('missing native lifecycle identity');
  }
  if (env.WAIT_UNTIL_OBSERVER !== 'enabled') return (namespace) => namespace;
  return (namespace) => ({
    ...namespace,
    default: class extends namespace.default {
      async observe() {
        const tasks = [];
        try {
          entrypoints.withWaitUntilObserver(null, () => {});
          throw Error('invalid observer accepted');
        } catch (error) {
          if (!(error instanceof TypeError)) throw error;
        }
        const failedScope = [];
        const marker = Error('scope failure');
        try {
          entrypoints.withWaitUntilObserver(
            (promise) => failedScope.push(promise),
            () => {
              throw marker;
            }
          );
        } catch (error) {
          if (error !== marker) throw error;
        }
        entrypoints.waitUntil(Promise.resolve());
        if (failedScope.length !== 0)
          throw Error('waitUntil scope escaped after throw');
        const value = await entrypoints.withWaitUntilObserver(
          (promise) => {
            if (!(promise instanceof Promise))
              throw Error('non-native waitUntil promise');
            tasks.push(promise);
          },
          () => super.observe()
        );
        const outcomes = await Promise.allSettled(tasks);
        return {
          value: value.answer,
          originalPromise: tasks[0] === value.promise,
          count: tasks.length,
          statuses: outcomes.map((item) => item.status),
        };
      }
    },
  });
}
import wrapped from 'cloudflare-internal:wrapped-binding';
