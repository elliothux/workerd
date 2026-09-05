// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "dynamic-worker-limiter.h"

#include <kj/test.h>
#include <kj/vector.h>

namespace workerd {
namespace {

KJ_TEST("distinct Dynamic Workers share slots until their final request ends") {
  int identities[11] = {};
  for (uint limit: {4u, 10u}) {
    auto limiter = kj::rc<DynamicWorkerLimiter>(limit);
    kj::Vector<kj::Own<void>> leases;
    for (uint i = 0; i < limit; ++i) leases.add(limiter->acquire(&identities[i]));
    auto duplicate = limiter->acquire(&identities[0]);
    KJ_EXPECT_THROW_MESSAGE(
        "Too many distinct Dynamic Workers", limiter->acquire(&identities[limit]));
    leases[0] = nullptr;
    KJ_EXPECT_THROW_MESSAGE(
        "Too many distinct Dynamic Workers", limiter->acquire(&identities[limit]));
    duplicate = nullptr;
    auto replacement = limiter->acquire(&identities[limit]);
    // A request lease can safely outlive the owning IoContext's reference.
    limiter = nullptr;
    leases.clear();
    replacement = nullptr;
  }
}

KJ_TEST("Dynamic Worker budgets are isolated between caller contexts") {
  int firstIdentity = 0;
  int secondIdentity = 0;
  auto first = kj::rc<DynamicWorkerLimiter>(1);
  auto second = kj::rc<DynamicWorkerLimiter>(1);
  auto firstRequest = first->acquire(&firstIdentity);
  auto secondRequest = second->acquire(&secondIdentity);
  KJ_EXPECT_THROW_MESSAGE("Too many distinct Dynamic Workers", first->acquire(&secondIdentity));
  firstRequest = nullptr;
  firstRequest = first->acquire(&secondIdentity);
}

}  // namespace
}  // namespace workerd
