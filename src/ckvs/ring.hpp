#pragma once

#include <atomic>
#include <new>
#include <array>
#include <mutex>

#include "utils/common.hpp"
#include "utils/traits.hpp"
#include "utils/spinlock.hpp"

namespace ckvs {
template <typename ValueT, size_t capacity = 1024, typename LockT = utils::spinlock>
struct ring
{
  using value_t = ValueT;
  using lock_t  = LockT;
  using index_t = utils::least_unsigned_t<capacity, uint8_t, uint16_t, uint32_t, uint64_t>;

  index_t                           _start_idx = 0;
  index_t                           _end_idx   = 0;
  std::array<value_t, capacity + 1> _storage;
  alignas(std::hardware_destructive_interference_size) lock_t _lock;

public:
  bool empty()
  {
    std::unique_lock<lock_t> lock{_lock};
    return _end_idx == _start_idx;
  }

  bool try_push(value_t value)
  {
    std::unique_lock<lock_t> lock{_lock};
    const index_t            new_end = (_end_idx + 1u) % (capacity + 1u);
    const bool               is_full = new_end == _start_idx;
    if(is_full)
      return false;
    _storage[_end_idx] = std::move(value);
    _end_idx           = new_end;
    return true;
  }

  bool try_pop(value_t & value)
  {
    std::unique_lock<lock_t> lock{_lock, std::try_to_lock};
    if(!lock.owns_lock())
      return false;
    if(_end_idx == _start_idx)
      return false;
    const index_t new_start = (_start_idx + 1u) % (capacity + 1u);
    value                   = std::move(_storage[_start_idx]);
    _start_idx              = new_start;
    return true;
  }
};

}