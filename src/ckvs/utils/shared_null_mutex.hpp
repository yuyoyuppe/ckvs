#pragma once

#include "common.hpp"
namespace ckvs { namespace utils {
struct shared_null_mutex
{
  inline bool owns_lock() const noexcept { return false; }

  inline void lock() noexcept {}

  inline void lock_shared() noexcept {}

  inline void unlock_shared() noexcept {}

  inline void unlock() noexcept {}
};
}}