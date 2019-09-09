#pragma once

#include "utils/common.hpp"
#include "utils/validated_val.hpp"
#include "utils/traits.hpp"
#include "utils/random.hpp"
#include "utils/enum_masked_ptr.hpp"
#include "utils/backoff.hpp"

#include "lockfree_pool.hpp"

#include "page_descriptor.hpp"

#include "detail/page_cache_detail.hpp"

#include <optional>
#include <tuple>
#include <variant>

#include "paged_file.hpp"

namespace ckvs {

template <typename PageT, typename PageIdT, size_t EnableEvictAtCapacityPercents = 80>
class page_cache
{
public:
  using page_t        = PageT;
  using page_id_t     = PageIdT;
  using r_page_lock_t = std::shared_lock<std::shared_mutex>;
  using w_page_lock_t = std::unique_lock<std::shared_mutex>;

  // "lazy" serves as a reminder that it's not RAII friendly => must lock it manually
  template <typename bool Exclusive>
  class shared_lazy_page_lock : public utils::noncopyable
  {
    enum LockedFlag { Locked };
    using ptr_and_lock_state_t = utils::enum_masked_ptr<page_descriptor, LockedFlag, LockedFlag::Locked>;

    friend class page_cache;

  public:
    page_cache *         _cache;
    ptr_and_lock_state_t _descriptor;

    shared_lazy_page_lock() noexcept : _descriptor{nullptr} {}

    shared_lazy_page_lock(page_cache * debug_cache, page_descriptor * descriptor, const bool adopt = false) noexcept
      : _descriptor{descriptor}, _cache{debug_cache}
    {
      if(adopt)
        _descriptor.set<LockedFlag::Locked>();
    }

    shared_lazy_page_lock(shared_lazy_page_lock && rhs) noexcept
    {
      _descriptor     = rhs._descriptor;
      rhs._descriptor = nullptr;
      _cache          = rhs._cache;
    }

    ~shared_lazy_page_lock() noexcept
    {
      if(!_descriptor)
        return;
      if(_descriptor.is_set<LockedFlag::Locked>())
      {

        if constexpr(Exclusive)
          _descriptor->unlock();
        else
          _descriptor->unlock_shared();
      }
      _descriptor->dec_ref();
    }

    shared_lazy_page_lock & operator=(shared_lazy_page_lock && rhs) noexcept
    {
      if(_descriptor && _descriptor.is_set<LockedFlag::Locked>())
      {
        if constexpr(Exclusive)
          _descriptor->unlock();
        else
          _descriptor->unlock_shared();
        _descriptor->dec_ref();
      }

      _descriptor     = rhs._descriptor;
      rhs._descriptor = nullptr;
      _cache          = rhs._cache;
      return *this;
    }

    bool owns_lock() const noexcept { return _descriptor.is_set<LockedFlag::Locked>(); }

    template <typename T>
    T * get_page_as() noexcept
    {
      // uncomment when done tweaking the padding etc.
      //static_assert(sizeof(T) == sizeof(page_t) && is_serializable_v<T>, "this isn't a valid page type!");
      return reinterpret_cast<T *>(_descriptor->raw_page());
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

    // This is possibly a destructive operation, since we either upgrade to exclusive lock, or lose it altogether(to prevent deadlocking)
    std::optional<shared_lazy_page_lock<true>> upgrade_or_consume() noexcept
    {
      CKVS_ASSERT(_descriptor && _descriptor.is_set<LockedFlag::Locked>());

      auto                  flags = _descriptor->acquire_flags();
      page_descriptor_flags new_flags;
      do
      {
        if(!!(flags & page_descriptor_flags::wants_ugprade_lock))
        {
          // Self-destruct!
          *this = shared_lazy_page_lock<false>{};
          return std::nullopt;
        }
        new_flags = flags | page_descriptor_flags::wants_ugprade_lock;
      } while(!_descriptor->try_update_flags(flags, new_flags));

      _descriptor->unlock_shared();
      _descriptor.clear<LockedFlag::Locked>();
      const bool locked = _descriptor->try_lock();
      do
      {
        new_flags = flags & ~page_descriptor_flags::wants_ugprade_lock;
      } while(!_descriptor->try_update_flags(flags, new_flags));

      if(!locked)
      {
        *this = shared_lazy_page_lock<false>{};
        return std::nullopt;
      }

      shared_lazy_page_lock<true> result;
      result._descriptor = static_cast<page_descriptor *>(_descriptor);
      _descriptor        = nullptr;
      result._cache      = _cache;
      result._descriptor.set<shared_lazy_page_lock<true>::LockedFlag::Locked>();
      return std::optional<shared_lazy_page_lock<true>>{std::move(result)};
    }

    void lock() noexcept
    {
      // We're contending for lock with IO-thread's load/flush routine here, so we must yield the lock if
      // IO thread was too slow to acquire it.

      page_descriptor_flags flags = _descriptor->acquire_flags();
      utils::backoff<7>     upgrade_backoff;
      // Everyone waits till the upgrader upgrades!
      while(!!(flags & page_descriptor_flags::wants_ugprade_lock))
      {
        upgrade_backoff();
        flags = _descriptor->acquire_flags();
      }

      utils::backoff<5> io_backoff;
      uint64_t          io_counter = std::numeric_limits<uint64_t>::max();
      bool              locked     = false;
      do
      {
        if(locked)
        {
          if constexpr(Exclusive)
            _descriptor->unlock();
          else
            _descriptor->unlock_shared();
        }
        const size_t very_long_time = 500;
        if(io_backoff.counter() > very_long_time)
        {
          // Start trying to rethrow IO exception
          _cache->_paged_file.idle();
        }
        io_counter = io_backoff();
        if constexpr(Exclusive)
          locked = _descriptor->try_lock();
        else
          locked = _descriptor->try_lock_shared();
        flags = _descriptor->acquire_flags();
      } while(
        // If the page is being loaded, always give way to IO
        !!(flags & page_descriptor_flags::being_loaded) || // Or if it's being flushed, we can only get a shared lock
        (!!(flags & page_descriptor_flags::being_flushed) && Exclusive) || !locked);

      _descriptor.set<LockedFlag::Locked>();

      CKVS_ASSERT((!(flags & page_descriptor_flags::being_flushed) || !Exclusive) &&
                  !(flags & page_descriptor_flags::being_loaded));
    }

    void unlock() noexcept
    {
      _descriptor.clear<LockedFlag::Locked>();
      if constexpr(Exclusive)
        _descriptor->unlock();
      else
        _descriptor->unlock_shared();
    }
  };

  static inline const size_t evict_threshold = EnableEvictAtCapacityPercents;

private:
  using bucket_t      = detail::bucket<page_descriptor>;
  using bucket_lock_t = typename bucket_t::lock_t;

  // Don't let those grow past a single cache-line size
  static_assert(sizeof(bucket_t) == std::hardware_destructive_interference_size);
  static_assert(sizeof(page_descriptor) == std::hardware_destructive_interference_size);

  page_id_t                                 _capacity;
  lockfree_pool<page_descriptor, page_id_t> _descriptor_pool;
  std::unique_ptr<bucket_t[]>               _buckets;
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

  static bool is_valid_capacity(const page_id_t cap) noexcept
  {
    return ((cap & (cap - 1)) == 0)
           // boost lockfree stack restriction :<
           && cap < std::numeric_limits<uint16_t>::max();
  }

  inline bool is_beyond_threshold() noexcept
  {
    return _size_info._size.load(std::memory_order_relaxed) > _size_info._evict_page_threshold;
  }

  inline page_t * locate_page(page_descriptor * desc) const noexcept { return &_pages[_descriptor_pool.idx(desc)]; }

  inline bucket_t & locate_bucket(const page_id_t page_id) noexcept
  {
    const page_id_t bid = std::hash<page_id_t>{}(page_id) & (_capacity - 1);
    return _buckets[bid];
  }

  void append_page_descriptor(bucket_t & b, page_descriptor * new_desc) noexcept
  {
    auto it = b._descriptors;
    if(!it)
    {
      b._descriptors = new_desc;
      return;
    }
    while(it->next() != nullptr)
      it = it->next();
    it->set_next(new_desc);
  }

  bool try_request_flush(page_descriptor_flags flags, page_descriptor * it) noexcept
  {
    // We're currently holding the bucket lock, so no one can add being_[loaded/flushed] or increase ref-count,
    // since that requires obtain the bucket lock. The page_descriptor cannot be accessed from paged_file's IO thread
    // either, since submitted pages have being_[loaded/flushed] flag set.
    CKVS_ASSERT(!(flags & page_descriptor_flags::being_flushed) || !(flags & page_descriptor_flags::being_loaded));
    CKVS_ASSERT(!(flags & page_descriptor_flags::pinned));
#if defined(DEBUG)
    const bool sanity_check = it->try_lock_shared();
    CKVS_ASSERT(sanity_check);
    it->unlock_shared();
#endif

    // Since we assume to evict at this point, we only need to decide whether to evict it instantly or flush it first. Note that it's possible to acquire read-only access during flushing.
    if(!!(flags & page_descriptor_flags::dirty))
    {
      page_descriptor_flags new_flags;
      do
      {
        new_flags = (flags | page_descriptor_flags::being_flushed) & ~page_descriptor_flags::dirty;
      } while(!it->try_update_flags(flags, new_flags));
      _paged_file.request(it);
      return true;
    }
    else
    {
      // Page isn't dirty, so it's safe to free everything
      return false;
    }
  }

  bool is_evict_possible(const page_descriptor_flags flags) noexcept
  {
    return !(flags & page_descriptor_flags::pinned) && !(flags & page_descriptor_flags::being_flushed) &&
           !(flags & page_descriptor_flags::being_loaded);
  }

  page_descriptor * search(bucket_t & b, const page_id_t page_id, const bool try_evict) noexcept
  {
    page_descriptor * found = nullptr;
    page_descriptor * prev  = nullptr;
    page_descriptor * it    = b._descriptors;

    while(it != nullptr)
    {
      // If found the needed descriptor => update LRU state and possibly proceed with evicting
      if(it->page_id() == page_id && !found)
      {
        found      = it;
        auto flags = found->acquire_flags();
        if(!(flags & page_descriptor_flags::recently_accessed) && !(flags & page_descriptor_flags::pinned))
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
      {
        bool can_evict = !it->referenced();
        auto flags     = it->acquire_flags();
        can_evict      = can_evict && is_evict_possible(flags);

        if(!!(flags & page_descriptor_flags::recently_accessed))
        {
          page_descriptor_flags new_flags;
          do
          {
            new_flags = flags & ~page_descriptor_flags::recently_accessed;
          } while(!it->try_update_flags(flags, new_flags));
          can_evict = false;
        }

        // If we've made it here, we can start the eviction process:
        // - request flush page_file if it's dirty
        // - if not dirty, remove it from the list and return to the pool
        can_evict = can_evict && !try_request_flush(flags, it);
        if(can_evict)
        {
          auto after_it = it->next();
          CKVS_ASSERT(after_it != it);
          if(prev)
            prev->set_next(after_it);
          if(b._descriptors == it)
            b._descriptors = after_it;
          _descriptor_pool.release(it);
          it = after_it;
          _size_info._size.fetch_sub(1, std::memory_order_relaxed);
          continue;
        }
      }
      prev = it;
      it   = it->next();
    }
    return found;
  }
  page_descriptor * obtain_new_page_descriptor() noexcept
  {
    CKVS_ASSERT(_size_info._size.load(std::memory_order_relaxed) <= _capacity);
    page_descriptor * new_descriptor = _descriptor_pool.acquire();
    // We must evict stuff until we can get a new_descriptor
    while(!new_descriptor)
    {
      // Pick a random bucket to reduce lock contention
      page_id_t bucket_id = static_cast<page_id_t>(utils::fast_thread_local_rand(0, _capacity - 1));
      std::unique_lock<bucket_lock_t> bucket_lock{_buckets[bucket_id]._bucket_lock, std::try_to_lock};
      if(!bucket_lock.owns_lock())
        continue;
      // Do a fake search in the bucket, possibly freeing descriptors in the process
      (void)search(_buckets[bucket_id], {}, true);
      new_descriptor = _descriptor_pool.acquire();
    }
    _size_info._size.fetch_add(1, std::memory_order_relaxed);
    return new_descriptor;
  }

  template <bool Exclusive>
  shared_lazy_page_lock<Exclusive> get_page_lock(const page_id_t page_id) noexcept
  {
    auto &           bucket = locate_bucket(page_id);
    std::scoped_lock bucket_lock{bucket._bucket_lock};

    page_descriptor * desc = search(bucket, page_id, is_beyond_threshold());
    if(!desc)
    {
      desc = obtain_new_page_descriptor();
      new(desc) page_descriptor{page_id, reinterpret_cast<std::byte *>(locate_page(desc))};
      append_page_descriptor(bucket, desc);
      page_descriptor_flags flags = desc->acquire_flags();
      page_descriptor_flags new_flags;
      do
      {
        new_flags = flags | page_descriptor_flags::being_loaded;
      } while(!desc->try_update_flags(flags, new_flags));
      desc->inc_ref();
      _paged_file.request(desc);
    }
    else
      desc->inc_ref();
    return {this, desc};
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
    // msvc compiler bug: can't figure alignof here.
    //static_assert(alignof(page_t) % paged_file::minimal_required_page_alignment == 0);
  }

  // not thread-safe
  void shutdown() noexcept
  {
    while(!_paged_file.idle())
      std::this_thread::sleep_for(std::chrono::milliseconds{10});

    for(size_t bid = 0; bid < _capacity; ++bid)
    {
      for(auto it = _buckets[bid]._descriptors; it != nullptr; it = it->next())
      {
        page_descriptor_flags flags = it->acquire_flags();
        page_descriptor_flags new_flags;
        // We only care about dirty stuff at this point
        do
        {
          new_flags = flags & ~page_descriptor_flags::being_loaded & ~page_descriptor_flags::being_flushed &
                      ~page_descriptor_flags::pinned & ~page_descriptor_flags::recently_accessed;
        } while(!it->try_update_flags(flags, new_flags));
      }
      // Trigger flushing requests
      (void)search(_buckets[bid], std::numeric_limits<page_id_t>::max(), true);
    }
    while(!_paged_file.idle())
      std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }

  template <bool Exclusive>
  shared_lazy_page_lock<Exclusive> get_lazy_page_lock(const page_id_t page_id) noexcept
  {
    return get_page_lock<Exclusive>(page_id);
  }
};
}
