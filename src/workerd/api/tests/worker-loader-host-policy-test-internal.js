const entries = Object.entries;
const apply = Reflect.apply;
const get = Reflect.get;
const NativeProxy = Proxy;
// Config-owned policy closure. The returned export transform carries private state without
// adding private values to env, handler props, a module export, or a serializable capability.
export default function bootstrap(env, privateEnv) {
  if (privateEnv.PRIVATE !== 'host-secret')
    throw Error('invalid private bootstrap');
  env.POLICY_READY = true;
  if (env.MODE === 'bootstrap-loop') {
    for (;;) {}
  }
  if (env.MODE === 'factory-value') return {};
  return (namespace) => {
    if (env.MODE === 'exports-loop') {
      for (;;) {}
    }
    if (env.MODE === 'exports-value') return null;
    if (env.MODE === 'exports-promise') return Promise.resolve(namespace);
    if (env.MODE === 'throw') throw Error('host policy failure');
    function wrap(value) {
      if (typeof value === 'function') {
        return class extends value {
          constructor(...args) {
            super(...args);
            // A constructor can return its own Proxy (the official Python bootstrap does).
            // Intercept the resulting instance rather than relying on subclass method overrides.
            return new NativeProxy(this, {
              get(target, key) {
                const method = get(target, key, target);
                if (key !== 'fetch') return method;
                return async (...args) => {
                  if (privateEnv.PRIVATE !== 'host-secret')
                    throw Error('lost private bootstrap');
                  const response = await apply(method, target, args);
                  return new Response((await response.text()) + ':policy');
                };
              },
            });
          }
        };
      }
      if (typeof value.fetch === 'function') {
        return {
          ...value,
          async fetch(...args) {
            if (privateEnv.PRIVATE !== 'host-secret')
              throw Error('lost private bootstrap');
            const response = await apply(value.fetch, value, args);
            return new Response((await response.text()) + ':policy');
          },
        };
      }
      // The official bootstrap exports named classes as a dictionary; wrap those same classes.
      const out = {};
      for (const [name, field] of entries(value)) out[name] = wrap(field);
      return out;
    }
    const out = {};
    for (const [name, value] of entries(namespace)) out[name] = wrap(value);
    return out;
  };
}
