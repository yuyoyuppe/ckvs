#pragma once

#include <atomic>
#include <new>
#include <array>
#include <mutex>

#include "utils/common.hpp"
#include "utils/traits.hpp"
#include "utils/spinlock.hpp"

namespace ckvs {
namespace utils {

template <typename ValueT, size_t capacity = 1024, typename LockT = utils::spinlock>
struct bounded_queue
{
  using value_t = ValueT;
  using lock_t  = LockT;

  std::queue<ValueT> _queue;
  alignas(std::hardware_destructive_interference_size) lock_t _lock;

public:
  bounded_queue() {}
  bool empty()
  {
    std::unique_lock<lock_t> lock{_lock};
    return _queue.empty();
  }

  bool try_push(value_t value)
  {
    std::unique_lock<lock_t> lock{_lock};
    const bool               is_full = _queue.size() == capacity;
    if(is_full)
      return false;
    _queue.push(std::move(value));
    return true;
  }

  bool try_pop(value_t & value)
  {
    std::unique_lock<lock_t> lock{_lock, std::try_to_lock};
    if(!lock.owns_lock() || _queue.empty())
      return false;
    value = std::move(_queue.front());
    _queue.pop();
    return true;
  }
};
}
