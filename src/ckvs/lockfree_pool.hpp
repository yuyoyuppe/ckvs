#pragma once

#include <new>
#include <memory>
#include <cinttypes>
#include <type_traits>

#include <utils/common.hpp>

#include <boost/lockfree/stack.hpp>

namespace ckvs {
template <typename T, typename CapacityT = size_t>
class lockfree_pool
{
private:
  static_assert(std::is_unsigned_v<CapacityT>);

  using chunk_t           = std::aligned_storage_t<sizeof(T), alignof(T)>;
  using chunk_dispenser_t = boost::lockfree::stack<chunk_t *, boost::lockfree::fixed_sized<true>>;

  CapacityT                  _size;
  std::unique_ptr<chunk_t[]> _chunks = nullptr;
  chunk_dispenser_t          _dispenser;

public:
  using value_t    = T;
  using capacity_t = CapacityT;

  class expiring_val
  {
    lockfree_pool * _pool;
    T *             _expiring;

  public:
    T * value() const noexcept { return _expiring; }

    expiring_val(T * val, lockfree_pool & pool) noexcept : _expiring(val), _pool(&pool) {}

    ~expiring_val() noexcept
    {
      if(_pool)
        _pool->release(_expiring);
    }

    expiring_val(const expiring_val &) = delete;
    expiring_val & operator=(const expiring_val &) = delete;

    expiring_val(expiring_val && r) noexcept
    {
      _pool       = r._pool;
      _expiring   = r._expiring;
      r._pool     = nullptr;
      r._expiring = nullptr;
    }
    expiring_val & operator=(expiring_val &&) = delete;
  };

  capacity_t capacity() const { return _size; }

  lockfree_pool(const capacity_t size) : _chunks{std::make_unique<chunk_t[]>(size)}, _dispenser{size}, _size{size}
  {
    for(capacity_t i = 0; i < size; ++i)
    {
      const auto chunk = &_chunks[i];
      const bool ok    = _dispenser.bounded_push(chunk);
      CKVS_ASSERT(ok);
    }
  }

  T * acquire() noexcept
  {
    chunk_t * chunk = nullptr;
    if(!_dispenser.pop(chunk))
      return nullptr;

    const auto result = reinterpret_cast<T *>(chunk);
    new(result) T{};
    return result;
  }

  void release(T * p) noexcept
  {
    p->~T();
    auto chunk = reinterpret_cast<chunk_t *>(p);
    CKVS_ASSERT((reinterpret_cast<uintptr_t>(chunk) % alignof(T)) == 0);
    CKVS_ASSERT(chunk >= &_chunks[0] && chunk <= &_chunks[_size - 1]);
    const bool ok = _dispenser.bounded_push(chunk);
    CKVS_ASSERT(ok);
  }
};
}