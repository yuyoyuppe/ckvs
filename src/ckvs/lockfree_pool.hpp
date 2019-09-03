#pragma once

#include <new>
#include <memory>
#include <cinttypes>
#include <type_traits>

#include <utils/common.hpp>

#include <boost/lockfree/stack.hpp>
#include <optional>

namespace ckvs {
template <typename T, typename CapacityT = size_t>
class lockfree_pool
{
private:
  static_assert(std::is_unsigned_v<CapacityT>);

  using chunk_t           = std::aligned_storage_t<sizeof(T), alignof(T)>;
  using chunk_dispenser_t = boost::lockfree::stack<chunk_t *, boost::lockfree::fixed_sized<true>>;

  CapacityT                          _size;
  std::unique_ptr<chunk_t[]>         _chunks;
  std::unique_ptr<chunk_dispenser_t> _dispenser; // have to wrap in ptr, since boost::stack isn't movable

public:
  using value_t    = T;
  using capacity_t = CapacityT;

  CapacityT idx(T * p) const noexcept
  {
    auto chunk = reinterpret_cast<chunk_t *>(p);
    CKVS_ASSERT((reinterpret_cast<uintptr_t>(chunk) % alignof(T)) == 0);
    CKVS_ASSERT(chunk >= &_chunks[0] && chunk <= &_chunks[_size - 1]);
    return static_cast<CapacityT>(chunk - &_chunks[0]);
  }

  capacity_t capacity() const { return _size; }

  lockfree_pool(const capacity_t size)
    : _size{size}, _chunks{std::make_unique<chunk_t[]>(size)}, _dispenser{std::make_unique<chunk_dispenser_t>(size)}
  {
    for(capacity_t i = 0; i < size; ++i)
    {
      const auto chunk = &_chunks[i];
      const bool ok    = _dispenser->bounded_push(chunk);
      CKVS_ASSERT(ok);
    }
  }

  T * acquire() noexcept
  {
    chunk_t * chunk = nullptr;
    if(!_dispenser->pop(chunk))
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
    const bool ok = _dispenser->bounded_push(chunk);
    CKVS_ASSERT(ok);
  }
};
}