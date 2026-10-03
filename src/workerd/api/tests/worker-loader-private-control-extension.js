// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0
import wrapped from 'cloudflare-internal:wrapped-binding';

// Test-only bridge. Production keeps this factory inside the INTERNAL host policy.
export function createPrivateTransport(transport) {
  return wrapped.createPrivateTransport(transport);
}
