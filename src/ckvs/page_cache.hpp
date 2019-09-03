#pragma once

#include "utils/common.hpp"
#include "utils/validated_val.hpp"
#include "utils/traits.hpp"
#include "utils/random.hpp"
#include "utils/enum_masked_ptr.hpp"

#include "lockfree_pool.hpp"

#include "page_descriptor.hpp"

#include "detail/page_cache_detail.hpp"

#include <optional>
#include <tuple>
#include <variant>

#include "paged_file.hpp"

namespace ckvs {

template <typename PageT, typename PageIdT, size_t EnableEvictAtCapacityPercents = 90>
class page_cache
{
public:
  using page_t        = PageT;
  using page_id_t     = PageIdT;
  using r_page_lock_t = std::shared_lock<std::shared_mutex>;
  using w_page_lock_t = std::unique_lock<std::shared_mutex>;

  class shared_page_lock
  {
    page_descriptor * _descriptor;

  public:
    template <typename T>
    T * get_page_as() noexcept
    {
      static_assert(sizeof(T) == sizeof(page_t) && is_serializable_v<T>);
      return reinterpret_cast<T *>(_descriptor->_raw_page);
    }

    shared_page_lock(page_descriptor * descriptor) noexcept : _descriptor{descriptor} {}

    ~shared_page_lock() noexcept
    {
      if(!_descriptor)
        return;
      _descriptor->_references.fetch_sub(1, std::memory_order_acq_rel);
    }

    shared_page_lock(shared_page_lock && rhs) noexcept
    {
      _descriptor     = rhs._descriptor;
      rhs._descriptor = nullptr;
    }

    void pin() noexcept
    {
      page_descriptor_flags new_flags;
      auto                  flags = _descriptor->acquire_flags();
      do
      {
        new_flags = flags | page_descriptor_flags::pinned;
      } while(!_descriptor->try_update_flags(flags, new_flags));
    }

    void unpin() noexcept
    {
      page_descriptor_flags new_flags;
      auto                  flags = _descriptor->acquire_flags();
      do
      {
        new_flags = flags & ~page_descriptor_flags::pinned;
      } while(!_descriptor->try_update_flags(flags, new_flags));
    }

    void mark_dirty() noexcept
    {
      page_descriptor_flags new_flags;
      auto                  flags = _descriptor->acquire_flags();
      do
      {
        new_flags = flags | page_descriptor_flags::dirty;
      } while(!_descriptor->try_update_flags(flags, new_flags));
    }

    void lock() noexcept
    {
      // We're contending for an exclusive lock with IO-thread's load routine here, so we must yield the lock if
      // IO thread was too slow to acquire it.
      size_t nWins_against_io_loader = std::numeric_limits<size_t>::max();
      auto   flags                   = _descriptor->acquire_flags();
      do
      {
        if(++nWins_against_io_loader > 0)
        {
          _descriptor->_page_lock.unlock();
          std::this_thread::yield();
          _mm_pause();
        }
        _descriptor->_page_lock.lock();
        auto flags = _descriptor->acquire_flags();
      } while(!!(flags & page_descriptor_flags::being_loaded));

      CKVS_ASSERT(nWins_against_io_loader < 3); // That means we have to slow harder or use conditional_variable
      CKVS_ASSERT(!(flags & page_descriptor_flags::being_flushed) && !(flags & page_descriptor_flags::being_loaded));
    }

    void unlock() noexcept { _descriptor->_page_lock.unlock(); }

    void unlock_shared() noexcept { _descriptor->_page_lock.unlock_shared(); }

    void lock_shared() noexcept
    {
      // We're contending for a shared lock with IO-thread's flush routine here, so we don't care about it
      _descriptor->_page_lock.lock_shared();
    }
  };

  static inline const size_t evict_threshold = EnableEvictAtCapacityPercents;

private:
  using bucket_t        = detail::bucket<page_descriptor>;
  using bucket_lock_t   = typename bucket_t::lock_t;
  using descriptor_iter = typename bucket_t::descriptor_iterator;

  // don't let those grow past a single cache-line size
  static_assert(sizeof(bucket_t) == std::hardware_destructive_interference_size);
  static_assert(sizeof(page_descriptor) == std::hardware_destructive_interference_size);

  page_id_t                                 _capacity;
  std::unique_ptr<bucket_t[]>               _buckets;
  lockfree_pool<page_descriptor, page_id_t> _descriptor_pool;
  std::unique_ptr<page_t[]>                 _pages;

  alignas(std::hardware_destructive_interference_size) struct size_info
  {
    std::atomic<page_id_t> _size;
    size_t                 _evict_page_threshold;
    size_info(const page_id_t size, const size_t evict_page_threshold) noexcept
      : _size{size}, _evict_page_threshold{evict_page_threshold}
    {
    }
    size_info(size_info && rhs) noexcept
      : _size{rhs._size.load(std::memory_order_relaxed)}, _evict_page_threshold{rhs._evict_page_threshold}
    {
    }
  } _size_info;
  paged_file & _paged_file;

  static bool is_valid_capacity(const page_id_t cap) noexcept { return ((cap & (cap - 1)) == 0) || cap == 0; }

  inline bool nearly_full() noexcept
  {
    return _size_info._size.load(std::memory_order_relaxed) > _size_info._evict_page_threshold;
  }

  inline page_t * locate_page(page_descriptor * desc) const noexcept { return &_pages[_descriptor_pool.idx(desc)]; }

  inline bucket_t & locate_bucket(const page_id_t page_id) noexcept
  {
    const page_id_t bid = std::hash<page_id_t>{}(page_id) & (_capacity - 1);
    return _buckets[bid];
  }

  void append_page_descriptor(bucket_t & b, page_descriptor & new_desc) noexcept
  {
    if(std::empty(b._descriptors))
    {
      b._descriptors.push_front(new_desc);
      return;
    }

    auto last = std::begin(b._descriptors);
    for(auto it = last; it != std::end(b._descriptors); last = ++it)
      continue;

    b._descriptors.insert_after(last, new_desc);
  }

  bool evict_descriptor_or_flush_page(page_descriptor_flags flags, bucket_t & b, descriptor_iter & it) noexcept
  {
    // We're currently holding the bucket locket, so no one can add being_[loaded/flushed] or increase ref-count,
    // since that requires obtain the bucket lock. The page_descriptor cannot be accessed from paged_file's IO thread
    // either, since submitted pages have being_[loaded/flushed] flag set. Finally, _references == 0 means it doesn't
    // have any alive promises.
#if defined(DEBUG) || true
    const bool sanity_check = it->_page_lock.try_lock();
    CKVS_ASSERT(sanity_check);
    it->_page_lock.unlock();
#endif
    CKVS_ASSERT(it->_references == 0);
    CKVS_ASSERT(!(flags & page_descriptor_flags::being_flushed) || !(flags & page_descriptor_flags::being_loaded));
    CKVS_ASSERT(!(flags & page_descriptor_flags::pinned));

    // Since we assume to do evict at this point, we only need to decide whether to evict it instantly or we must flush it first. Notice, that it's possible for someone to get a read-only access during flushing.
    if(!!(flags & page_descriptor_flags::dirty))
    {
      page_descriptor_flags new_flags;
      do
      {
        new_flags = (flags | page_descriptor_flags::being_flushed) & ~page_descriptor_flags::dirty;
      } while(!it->try_update_flags(flags, new_flags));
      _paged_file.request(&*it);
      return false;
    }
    else
    {
      // Page isn't dirty, so it's safe to free everything
      it = b._descriptors.erase(it);
      _descriptor_pool.release(&*it);

      _size_info._size.fetch_sub(1, std::memory_order_relaxed);
      return true;
    }
  }

  bool try_evict_descriptor(descriptor_iter & it, bucket_t & b) noexcept
  {
    if(it->_references.load(std::memory_order_acquire) != 0)
      return false;

    auto flags = it->acquire_flags();

    if(!!(flags & page_descriptor_flags::pinned) || !!(flags & page_descriptor_flags::being_flushed) ||
       !!(flags & page_descriptor_flags::being_loaded))
      return false;

    // Mark descriptor as stale, but not evict it yet
    if(!!(flags & page_descriptor_flags::recently_accessed))
    {
      page_descriptor_flags new_flags;
      do
      {
        new_flags = flags & ~page_descriptor_flags::recently_accessed;
      } while(!it->try_update_flags(flags, new_flags));
      return false;
    }

    return evict_descriptor_or_flush_page(flags, b, it);
  }

  bool try_evict_from_bucket(bucket_t & b) noexcept
  {
    for(auto it = std::begin(b._descriptors); it != std::end(b._descriptors); ++it)
    {
      if(try_evict_descriptor(it, b))
        return true;
    }
    return false;
  }

  page_descriptor * obtain_new_page_descriptor() noexcept
  {
    page_descriptor * new_descriptor = _descriptor_pool.acquire();
    // We must evict stuff until we can get a new_descriptor
    while(!new_descriptor)
    {
      // Pick a random bucket to reduce lock contention
      page_id_t bucket_id = static_cast<page_id_t>(utils::fast_thread_local_rand(0, _capacity - 1));
      std::unique_lock<bucket_lock_t> bucket_lock{_buckets[bucket_id]._bucket_lock, std::try_to_lock};
      if(!bucket_lock.owns_lock())
        continue;
      if(!try_evict_from_bucket(_buckets[bucket_id]))
        continue;
      new_descriptor = _descriptor_pool.acquire();
    }
    _size_info._size.fetch_add(1, std::memory_order_relaxed);

    return new_descriptor;
  }

  descriptor_iter find_page_descriptor(bucket_t & b, const page_id_t page_id) noexcept
  {
    const bool      try_evict = nearly_full();
    descriptor_iter found     = std::end(b._descriptors);
    for(auto it = std::begin(b._descriptors); it != std::end(b._descriptors); ++it)
    {
      // If found the needed descriptor => update LRU state and possibly proceed with evicting
      if(found == std::end(b._descriptors) && it->_page_id == page_id)
      {
        found      = it;
        auto flags = found->acquire_flags();
        if(!(flags & page_descriptor_flags::recently_accessed))
        {
          page_descriptor_flags new_flags;
          do
          {
            new_flags = flags | page_descriptor_flags::recently_accessed;
          } while(!found->try_update_flags(flags, new_flags));
        }

        if(!try_evict)
          return found;
      }
      else if(try_evict)
        try_evict_descriptor(it, b);
    }
    return found;
  }

  shared_page_lock get_page_shared_mutex(const page_id_t page_id) noexcept
  {
    auto &           bucket = locate_bucket(page_id);
    std::scoped_lock bucket_lock{bucket._bucket_lock};

    page_descriptor * desc = &*find_page_descriptor(bucket, page_id);

    if(!desc)
    {
      desc = obtain_new_page_descriptor();
      append_page_descriptor(bucket, *desc);
      desc->_page_id              = page_id;
      desc->_raw_page             = reinterpret_cast<std::byte *>(locate_page(desc));
      page_descriptor_flags flags = desc->acquire_flags();
      page_descriptor_flags new_flags;
      do
      {
        new_flags = flags | page_descriptor_flags::being_loaded;
      } while(!desc->try_update_flags(flags, new_flags));
      _paged_file.request(desc);
    }

    desc->_references.fetch_add(1, std::memory_order_acq_rel);
    return {desc};
  }

public:
  page_cache(const page_id_t capacity, paged_file & paged_file)
    : _capacity{utils::validated(
        capacity,
        is_valid_capacity,
        std::invalid_argument("page_cache's capacity should be a natural number and a power of 2!"))}
    , _descriptor_pool{capacity}
    , _pages{std::make_unique<page_t[]>(capacity)}
    , _buckets{std::make_unique<bucket_t[]>(capacity)}
    , _size_info{0, static_cast<page_id_t>(capacity * static_cast<double>(evict_threshold) / 100.)}
    , _paged_file{paged_file}
  {
    constexpr size_t align = alignof(page_t);
    // msvc compiler bug: can't figure alignof here.
    //static_assert(align % paged_file::minimal_required_page_alignment == 0);
    CKVS_ASSERT(align % paged_file::minimal_required_page_alignment == 0);
  }

  void evict_free_page(const page_id_t page_id) noexcept
  {
    auto &           bucket = locate_bucket(page_id);
    std::scoped_lock bucket_lock{bucket._bucket_lock};
    descriptor_iter  desc = find_page_descriptor(bucket, page_id);
    CKVS_ASSERT(desc != std::end(bucket._descriptors));
    auto flags = desc->acquire_flags();
    evict_descriptor_or_flush_page(flags, bucket, desc);
  }

  shared_page_lock get_shared_page_lock(const page_id_t page_id) noexcept { return get_page_shared_mutex(page_id); }
};
}
