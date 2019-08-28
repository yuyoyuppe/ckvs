#pragma once

#include "utils/common.hpp"
#include "utils/validated_val.hpp"

#include "lockfree_pool.hpp"

#include "detail/page_cache_detail.hpp"

#include <optional>
#include <tuple>

namespace ckvs {

template <typename PageT, typename PageIdT, size_t EnableEvictAtCapacityPercents = 90>
class page_cache
{
  using page_t            = PageT;
  using page_id_t         = PageIdT;
  using page_descriptor_t = detail::page_descriptor<page_id_t, page_t>;
  using bucket_t          = detail::bucket<page_descriptor_t>;
  using bucket_lock_t     = typename bucket_t::lock_t;

  static_assert(std::is_trivial_v<page_t>);

public:
  using lock_t          = typename page_descriptor_t::lock_t;
  using r_page_lock_t   = std::shared_lock<lock_t>;
  using w_page_lock_t   = std::unique_lock<lock_t>;
  using r_locked_page_t = std::tuple<page_t &, r_page_lock_t>;
  using w_locked_page_t = std::tuple<page_t &, w_page_lock_t>;

private:
  // don't let those grow past a single cache-line size
  static_assert(sizeof(bucket_t) == std::hardware_destructive_interference_size);
  static_assert(sizeof(page_descriptor_t) == std::hardware_destructive_interference_size);

  page_id_t                                   _capacity;
  std::unique_ptr<bucket_t[]>                 _buckets;
  lockfree_pool<page_descriptor_t, page_id_t> _descriptor_pool;
  lockfree_pool<page_t, page_id_t>            _page_pool;

  using expiring_page_descriptor_t = typename lockfree_pool<page_descriptor_t, page_id_t>::expiring_val;

  static inline size_t evict_threshold = EnableEvictAtCapacityPercents;
  alignas(std::hardware_destructive_interference_size) struct size_info
  {
    std::atomic<page_id_t> _size;
    size_t                 _evict_threshold;
  } _size_info;

  static bool is_valid_capacity(const page_id_t cap) noexcept { return ((cap & (cap - 1)) == 0) || cap == 0; }

  inline bool nearly_full() noexcept
  {
    return _size_info._size.load(std::memory_order_relaxed) > _size_info._evict_threshold;
  }

  inline bucket_t & locate_bucket(const page_id_t page_id) noexcept
  {
    CKVS_ASSERT(page_id != 0);

    const page_id_t bid = std::hash<page_id_t>{}(page_id) & (_capacity - 1);
    return _buckets[bid];
  }

  void append_page_descriptor(bucket_t & b, page_descriptor_t & new_desc) noexcept
  {
    if(empty(b._descriptors))
    {
      b._descriptors.push_front(new_desc);
      return;
    }

    auto last = begin(b._descriptors);
    for(auto it = last; it != end(b._descriptors); last = ++it)
      continue;

    b._descriptors.insert_after(last, new_desc);
  }
  std::optional<expiring_page_descriptor_t> try_extract_page_descriptor(bucket_t & b, const page_id_t page_id) noexcept
  {
    auto it = std::find_if(
      begin(b._descriptors), end(b._descriptors), [page_id](const auto & d) { return d._page_id == page_id; });
    if(it == end(b._descriptors))
      return std::nullopt;

    page_descriptor_t * desc = &*it;
    b._descriptors.erase(it);
    return {expiring_page_descriptor_t{desc, _descriptor_pool}};
  }
  page_descriptor_t * try_get_page_descriptor(bucket_t & b, const page_id_t page_id) noexcept
  {
    const bool          try_evict = nearly_full();
    page_descriptor_t * found     = nullptr;
    for(auto it = begin(b._descriptors); it != end(b._descriptors); ++it)
    {
      if(!found && it->_page_id == page_id)
      {
        found                     = &*it;
        found->_recently_accessed = true;
        if(!try_evict)
          return found;
      }
      else if(try_evict)
      {
        if(it->_recently_accessed)
        {
          it->_recently_accessed = false;
        }
        else
        {
          w_page_lock_t lock{it->_page_lock, std::try_to_lock};
          if(!lock.owns_lock())
            continue;
          page_descriptor_t & evicted = *it;
          it                          = b._descriptors.erase(it);
          // todo: return descriptor to pool, contact paged_file_view
          evicted._recently_accessed;
        }
      }
    }
    return found;
  }

  template <typename LockedPageT, typename LockT = std::tuple_element_t<1, LockedPageT>>
  std::optional<LockedPageT> try_get_page_locked(const page_id_t page_id) noexcept
  {
    auto &           bucket = locate_bucket(page_id);
    std::scoped_lock bucket_lock{bucket._bucket_lock};

    page_descriptor_t * desc = try_get_page_descriptor(bucket, page_id);
    if(desc == nullptr)
      return std::nullopt;

    return {{*desc->_raw_page, LockT{desc->_page_lock}}};
  }

public:
  page_cache(const page_id_t capacity)
    : _capacity{utils::validated(
        capacity,
        is_valid_capacity,
        std::invalid_argument("page_cache's capacity should be a natural number and a power of 2!"))}
    , _descriptor_pool{capacity}
    , _page_pool{capacity}
    , _buckets{std::make_unique<bucket_t[]>(capacity)}
    , _size_info{0, static_cast<page_id_t>(capacity * static_cast<double>(evict_threshold) / 100.)}
  {
  }

  void evict_page(const page_id_t page_id) noexcept
  {
    auto &           bucket = locate_bucket(page_id);
    std::scoped_lock bucket_lock{bucket._bucket_lock};

    auto desc = try_extract_page_descriptor(bucket, page_id);
    if(desc == std::nullopt)
      // Already evicted => nothing to do
      return;
    const bool needs_flushing = desc->value()->_dirty;

    std::scoped_lock page_lock{desc->value()->_page_lock};
    if(needs_flushing)
    {
      // todo: call page_io_service? no, actually
      printf("flushing page %u!\n", page_id);
      CKVS_ASSERT(false);
    }
  }

  void add_page(const page_id_t page_id, page_t * const raw_page) noexcept
  {
    auto new_desc = _descriptor_pool.acquire();
    if(!new_desc)
    {
      CKVS_ASSERT(false);
      // todo: go evict stuff.
    }
    new_desc->_raw_page = raw_page;
    new_desc->_page_id  = page_id;

    _size_info._size.fetch_add(1, std::memory_order_relaxed);
    auto &           bucket = locate_bucket(page_id);
    std::scoped_lock bucket_lock{bucket._bucket_lock};
    append_page_descriptor(bucket, *new_desc);
  }

  void mark_page_dirty(const page_id_t page_id) noexcept
  {
    auto &           bucket = locate_bucket(page_id);
    std::scoped_lock bucket_lock{bucket._bucket_lock};

    page_descriptor_t * desc = try_get_page_descriptor(bucket, page_id);
    CKVS_ASSERT(desc != nullptr);
    desc->_dirty = true;
  }

  std::optional<r_locked_page_t> try_get_page_shared(const page_id_t page_id) noexcept
  {
    return try_get_page_locked<r_locked_page_t>(page_id);
  }

  std::optional<w_locked_page_t> try_get_page_exclusive(const page_id_t page_id) noexcept
  {
    return try_get_page_locked<w_locked_page_t>(page_id);
  }
};
}
