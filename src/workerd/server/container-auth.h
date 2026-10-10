#pragma once

#include <workerd/util/strings.h>

#include <openssl/mem.h>

#include <kj/compat/http.h>

namespace workerd::server {

constexpr kj::StringPtr CONTAINER_CONTROL_HEADER = "X-Open-Compute-Container-Control"_kj;
constexpr kj::StringPtr CONTAINER_CALLBACK_HEADER = "X-Open-Compute-Container-Callback"_kj;

inline bool containerTokenMatches(kj::StringPtr expected, kj::StringPtr presented) {
  return expected.size() == 64 && presented.size() == 64 &&
      CRYPTO_memcmp(expected.begin(), presented.begin(), 64) == 0;
}

inline bool containerCallbackAuthorized(
    const kj::Maybe<kj::String>& expected, const kj::HttpHeaders& headers) {
  uint count = 0;
  kj::Maybe<kj::StringPtr> presented;
  headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
    if (workerd::strcaseeq(name, CONTAINER_CALLBACK_HEADER)) {
      ++count;
      presented = value;
    }
  });
  if (count != 1 || expected == kj::none || presented == kj::none) return false;
  return containerTokenMatches(KJ_ASSERT_NONNULL(expected), KJ_ASSERT_NONNULL(presented));
}

}  // namespace workerd::server
