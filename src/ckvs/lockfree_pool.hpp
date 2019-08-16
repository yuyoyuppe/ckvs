#pragma once

#include <new>
#include <memory>
#include <cinttypes>
#include <type_traits>

#include <utils/common.hpp>

#include <boost/lockfree/stack.hpp> // todo: not depend on boost

template <typename T>
class lockfree_pool
{
private:
  static_assert(sizeof(T) >= sizeof(void *));
  static_assert(std::is_trivial_v<T>);

  using chunk_t           = std::aligned_storage_t<sizeof(T), alignof(T)>;
  using chunk_dispenser_t = boost::lockfree::stack<chunk_t *, boost::lockfree::fixed_sized<true>>;

  std::unique_ptr<chunk_t[]> _chunks = nullptr;
  chunk_dispenser_t          _dispenser;

  size_t _size;

public:
  using value_type = T;

  size_t capacity() const { return _size; }

  lockfree_pool(const size_t size) : _chunks{std::make_unique<chunk_t[]>(size)}, _dispenser{size}, _size{size}
  {
    for(size_t i = 0; i < size; ++i)
    {
      const auto chunk = &_chunks[i];
      const bool ok    = _dispenser.bounded_push(chunk);
      CKVS_ASSERT(ok);
    }
  }
  ~lockfree_pool() noexcept {}

  T * allocate() noexcept
  {
    chunk_t * chunk = nullptr;
    if(!_dispenser.pop(chunk))
      return nullptr;

    const auto result = reinterpret_cast<T *>(chunk);
    new(result) T{};
    return result;
  }

  void deallocate(T * p) noexcept
  {
    p->~T();
    auto chunk = reinterpret_cast<chunk_t *>(p);
    CKVS_ASSERT((reinterpret_cast<uintptr_t>(chunk) % alignof(T)) == 0);
    CKVS_ASSERT(chunk >= &_chunks[0] && chunk <= &_chunks[_size - 1]);
    const bool ok = _dispenser.bounded_push(chunk);
    CKVS_ASSERT(ok);
  }
};

template <typename T, typename U>
bool operator==(const lockfree_pool<T> &, const lockfree_pool<U> &)
{
  return false;
}
template <typename T, typename U>
bool operator!=(const lockfree_pool<T> &, const lockfree_pool<U> &)
{
  return true;
}
