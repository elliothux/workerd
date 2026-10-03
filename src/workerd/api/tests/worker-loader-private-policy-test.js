// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Test-owned policy proves that authority remains in a closure, including after transfer.
export default function policy(_publicEnv, privateEnv) {
  const secret = privateEnv.secret;
  const privilegedLoader = privateEnv.privilegedLoader;
  return (namespace) => ({
    ...namespace,
    default: {
      ...namespace.default,
      async fetch(request, env, ctx) {
        const response = await namespace.default.fetch(request, env, ctx);
        if (privilegedLoader !== undefined) {
          let denied = false;
          try {
            privilegedLoader.load({
              compatibilityDate: '2025-01-01',
              mainModule: 'forbidden.js',
              modules: { 'forbidden.js': 'export default {};' },
              env: { privilegedLoader },
            });
          } catch (error) {
            denied =
              error instanceof DOMException && error.name === 'DataCloneError';
          }
          if (!denied)
            throw new Error('private loader grant was retransferred');
          const child = privilegedLoader.load({
            compatibilityDate: '2025-01-01',
            mainModule: 'child.js',
            modules: {
              'child.js':
                'export default { fetch(req, env) { if(env.secret !== undefined) throw new Error("private binding exposed");return new Response("child"); } };',
            },
            openComputeHostPolicy: true,
            openComputePrivateEnv: { secret: 'granted' },
          });
          return child.getEntrypoint().fetch(request);
        }
        if (secret !== null && typeof secret === 'object') {
          return Response.json({
            ...(await response.json()),
            greeting: await secret.greet('Alice'),
          });
        }
        if (secret === 'granted') return new Response(secret);
        return response;
      },
    },
  });
}
