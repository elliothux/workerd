import wrapped from 'cloudflare-internal:wrapped-binding';

export default function bootstrap() {
  for (const args of [
    ['a'.repeat(63)],
    ['A'.repeat(64)],
    ['g'.repeat(64)],
    ['a'.repeat(64), 'x'.repeat(1025)],
    ['a'.repeat(64), undefined, 'unknown'],
  ]) {
    let rejected = false;
    try {
      wrapped.createDurableObjectId(...args);
    } catch (error) {
      rejected =
        error instanceof TypeError && error.message === 'DO_ID_INVALID';
    }
    if (!rejected) throw Error('native ID factory accepted invalid input');
  }
  return {
    NAMED: wrapped.createDurableObjectId('a'.repeat(64), 'alpha', 'eu'),
    PARSED: wrapped.createDurableObjectId('a'.repeat(64), undefined, 'eu'),
    OTHER: wrapped.createDurableObjectId('b'.repeat(64)),
  };
}
