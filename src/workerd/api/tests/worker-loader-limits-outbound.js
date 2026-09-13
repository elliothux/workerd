// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

let started = 0;
let maximumStarted = 0;

export default {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === '/hold') {
      started++;
      maximumStarted = Math.max(maximumStarted, started);
      await scheduler.wait(250);
      started--;
      return new Response('released');
    }
    if (path === '/maximum') return new Response(String(maximumStarted));
    return new Response('not found', { status: 404 });
  },
};
