#pragma once

#include <atomic>
#include <new>
#include <emmintrin.h>
namespace ckvs { namespace utils {
class spinlock
{
  std::atomic_flag _flag{ATOMIC_FLAG_INIT};

public:
  bool try_lock() noexcept { return !_flag.test_and_set(std::memory_order_acquire); }

  void lock() noexcept
  {
    while(_flag.test_and_set(std::memory_order_acquire))
      _mm_pause();
  }
  void unlock() noexcept { _flag.clear(std::memory_order_release); }
};
}}