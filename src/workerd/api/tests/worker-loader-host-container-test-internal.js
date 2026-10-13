import wrapped from 'cloudflare-internal:wrapped-binding';
import innerEnv from 'cloudflare-internal:env';

// Owning native fixture: issuance stays private, as in the runtime's INTERNAL module.
const issued = new WeakMap();
const fetchPublicTransport = wrapped.fetchPublicTransport.bind(wrapped);
const connectPublicTransport = wrapped.connectPublicTransport.bind(wrapped);
export function createServiceBinding({ fetcher }) {
  wrapped.validatePublicTransport(fetcher);
  const policy = {
    fetch(request) { return fetchPublicTransport(fetcher, request); },
    connect(address) { return connectPublicTransport(fetcher, address); },
  };
  issued.set(policy, policy);
  return policy;
}
export function registerServiceBinding() {}
export function createBindingChannelCallbacks(policy) {
  const callbacks = issued.get(policy);
  if (callbacks === undefined) throw Error('SERVICE_BINDING_DENIED');
  if (innerEnv.getCurrentEnv()?.DECLARED !== 'registration event') throw Error('registration env was not the tenant env');
  return {
    async fetch(request) { return { response: await callbacks.fetch(request), leaseBody: null, errorCode: null }; },
    connect(address) { return callbacks.connect(address); },
  };
}
export default function bootstrap(publicEnv, privateEnv) {
  if (privateEnv.raw !== undefined) {
    const policy = createServiceBinding({ fetcher: privateEnv.raw });
    publicEnv.OBJECT = wrapped.createDurableObjectStub(
      wrapped.createDurableObjectId('a'.repeat(64), 'channel'), privateEnv.raw, policy
    );
  }
  return (namespace) => namespace;
}
