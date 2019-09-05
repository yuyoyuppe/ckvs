#pragma once

#include <shared_mutex>
#include <thread>
#include <atomic>

#include "utils/common.hpp"

#if defined(LOGGING)
#pragma warning(disable : 4477)
#pragma warning(disable : 4313)
#endif

namespace ckvs {

enum class page_descriptor_flags : uint8_t {
  pinned             = 0x01,
  dirty              = 0x02,
  being_flushed      = 0x04,
  being_loaded       = 0x08,
  recently_accessed  = 0x10,
  wants_ugprade_lock = 0x20,

  reserved1 = 0x40,
  reserved2 = 0x80,
};

//#define LOGGING
class alignas(std::hardware_destructive_interference_size) page_descriptor : public utils::noncopyable
{
  std::atomic_uint16_t _references = 0; // We don't support more than uint16_max simultaneous refs
  std::atomic<uint8_t> _flags      = 0;
  uint32_t             _page_id    = 0;
  std::byte *          _raw_page   = nullptr;
  page_descriptor *    _next       = nullptr;
  std::shared_mutex    _page_lock;

public:
  page_descriptor() = default;
  inline page_descriptor(const uint32_t page_id, std::byte * raw_page) noexcept : _page_id{page_id}, _raw_page{raw_page}
  {
  }

  inline page_descriptor * next() const noexcept { return _next; }

  inline void set_next(page_descriptor * const next) noexcept
  {
    CKVS_ASSERT(next != this);
    _next = next;
  }

  inline bool referenced() const noexcept { return _references.load(std::memory_order_acquire) != 0; }
  inline void inc_ref() noexcept { _references.fetch_add(1, std::memory_order_acq_rel); }
  inline void dec_ref() noexcept { _references.fetch_sub(1, std::memory_order_acq_rel); }

  inline std::byte * raw_page() const { return _raw_page; }
  inline uint32_t    page_id() const noexcept { return _page_id; }

  inline bool try_lock_shared()
  {
    bool result = _page_lock.try_lock_shared();
#if defined(LOGGING)
    if(result)
      printf("[%zu] shared_locked. #%u\n", std::this_thread::get_id(), _page_id);
#endif
    return result;
  }
  inline bool try_lock() noexcept
  {
    bool result = _page_lock.try_lock();
#if defined(LOGGING)
    if(result)
      printf("[%zu] locked #%u\n", std::this_thread::get_id(), _page_id);
#endif
    return result;
  }
  inline void lock_shared()
  {
#if defined(LOGGING)
    printf("[%zu] shared_locked #%u\n", std::this_thread::get_id(), _page_id);
#endif
    _page_lock.lock_shared();
  }
  inline void lock()
  {
#if defined(LOGGING)
    printf("[%zu] locked #%u\n", std::this_thread::get_id(), _page_id);
#endif
    _page_lock.lock();
  }
  inline void unlock_shared()
  {
#if defined(LOGGING)
    printf("[%zu] shared_unlocked #%u\n", std::this_thread::get_id(), _page_id);
#endif
    _page_lock.unlock_shared();
  }
  inline void unlock()
  {
#if defined(LOGGING)
    printf("[%zu] unlocked #%u\n", std::this_thread::get_id(), _page_id);
#endif
    _page_lock.unlock();
  }

  inline page_descriptor_flags acquire_flags()
  {
    return static_cast<page_descriptor_flags>(_flags.load(std::memory_order_acquire));
  }
  inline bool try_update_flags(page_descriptor_flags & cur_flags, const page_descriptor_flags new_flags)
  {
    return _flags.compare_exchange_weak(
      reinterpret_cast<uint8_t &>(cur_flags), static_cast<uint8_t>(new_flags), std::memory_order_acq_rel);
  }

  ~page_descriptor() noexcept
  {
    CKVS_ASSERT(!referenced());
    CKVS_ASSERT(static_cast<uint8_t>(acquire_flags()) == 0);
  }
};

inline page_descriptor_flags operator|(const page_descriptor_flags lhs, const page_descriptor_flags rhs)
{
  return static_cast<page_descriptor_flags>(static_cast<uint8_t>(lhs) | static_cast<uint8_t>(rhs));
}

inline page_descriptor_flags operator&(const page_descriptor_flags lhs, const page_descriptor_flags rhs)
{
  return static_cast<page_descriptor_flags>(static_cast<uint8_t>(lhs) & static_cast<uint8_t>(rhs));
}

inline page_descriptor_flags operator~(const page_descriptor_flags rhs)
{
  return static_cast<page_descriptor_flags>(~static_cast<uint8_t>(rhs));
}

inline bool operator!(const page_descriptor_flags rhs) { return static_cast<uint8_t>(rhs) == 0; }

}
