// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/map.h>
#include <kj/refcount.h>

namespace workerd {

using kj::uint;

class WorkerStubChannel;

// A caller IoContext's distinct in-flight Dynamic Worker budget. Each channel retains this
// budget independently of the context, so releasing a request after its caller exits is safe.
class DynamicWorkerLimiter final: public kj::Refcounted {
 public:
  explicit DynamicWorkerLimiter(uint limit): limit(limit) {}

  // Identity must stay alive until the returned lease is destroyed. Concurrent leases for the
  // same identity share one slot; the last lease releases it, including on exception/cancel.
  kj::Own<void> acquire(const void* identity);

  // Bind a native stub to this caller's budget. Admission happens when an entrypoint request
  // starts, not when a loader or entrypoint handle is created or when source fetching starts.
  kj::Own<WorkerStubChannel> wrap(kj::Own<WorkerStubChannel> inner);

 private:
  uint limit;
  kj::HashMap<const void*, uint> active;
  class Lease;
};

}  // namespace workerd
