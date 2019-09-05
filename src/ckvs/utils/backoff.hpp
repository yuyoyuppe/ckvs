#pragma once

#include <thread>
#include <emmintrin.h>

namespace ckvs { namespace utils {
template <typename uint64_t multiplier>
class backoff
{
private:
  uint64_t _counter = 0;

public:
  uint64_t counter() const { return _counter; }
  uint64_t operator()()
  {
    for(size_t i = 0; i < _counter * multiplier; ++i)
    {
      std::this_thread::yield();
      _mm_pause();
    }
    return _counter++;
  }
};
}}