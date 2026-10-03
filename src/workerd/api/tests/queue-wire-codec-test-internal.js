import wrapped from 'cloudflare-internal:wrapped-binding';

function check(condition) {
  if (!condition) throw Error('native Queue wire mismatch');
}

// Native API regression fixture. Product durable publication is qualified in the runtime tests.
export default function bootstrap(env) {
  let count = 0;
  let size = 0;
  const metrics = () => ({
    backlogCount: count,
    backlogBytes: size,
    oldestMessageTimestamp: new Date(1000),
  });
  const policy = {
    async send(bytes, format, delay) {
      check(delay === 3);
      if (format === 'json')
        check(JSON.parse(new TextDecoder().decode(bytes)).ok === true);
      else if (format === 'text')
        check(new TextDecoder().decode(bytes) === 'text');
      else if (format === 'bytes')
        check(bytes.length === 2 && bytes[0] === 1 && bytes[1] === 2);
      else if (format === 'v8') {
        const value = wrapped.decodeQueueV8(bytes);
        check(value.self === value);
        check(value.big === 12n);
      } else throw Error('unexpected native format');
      ++count;
      size += bytes.byteLength;
      return { metadata: { metrics: metrics() } };
    },
    async sendBatch(body, delay) {
      check(delay === 4);
      const { messages } = JSON.parse(body);
      check(messages.length === 2);
      check(messages[0].contentType === 'bytes');
      check(messages[0].body === 'AwQ=');
      check(messages[0].delaySecs === 0);
      check(messages[1].contentType === 'json');
      count += messages.length;
      return { metadata: { metrics: metrics() } };
    },
    async metrics() {
      return metrics();
    },
  };
  wrapped.installQueuePolicy(env.QUEUE, policy);
  let rejected = false;
  try {
    wrapped.installQueuePolicy(env.QUEUE, policy);
  } catch {
    rejected = true;
  }
  check(rejected);
  return (namespace) => namespace;
}
