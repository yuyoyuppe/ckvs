#pragma once

#include <shared_mutex>
#include <thread>
#include <boost/intrusive/slist.hpp>
#include <atomic>

namespace ckvs {
namespace bi      = boost::intrusive;
using link_mode_t = bi::link_mode<
#if defined(DEBUG)
  bi::link_mode_type::safe_link
#else
  bi::link_mode_type::normal_link
#endif
  >;

enum class page_descriptor_flags : uint8_t {
  pinned            = 0x01,
  dirty             = 0x02,
  being_flushed     = 0x04,
  being_loaded      = 0x08,
  recently_accessed = 0x10, // todo: could be slow. maybe remove?

  reserved1 = 0x20,
  reserved2 = 0x40,
  reserved3 = 0x80,
};

struct alignas(std::hardware_destructive_interference_size) page_descriptor : bi::slist_base_hook<link_mode_t>
{
  std::atomic_uint16_t _references; // we don't support more than uint16_max threads
  std::atomic<uint8_t> _flags;
  uint32_t             _page_id = 0;
  std::byte *          _raw_page;
  std::shared_mutex    _page_lock;

  inline page_descriptor_flags acquire_flags()
  {
    return static_cast<page_descriptor_flags>(_flags.load(std::memory_order_acquire));
  }

  inline bool try_update_flags(page_descriptor_flags & cur_flags, const page_descriptor_flags new_flags)
  {
    return _flags.compare_exchange_weak(
      reinterpret_cast<uint8_t &>(cur_flags), static_cast<uint8_t>(new_flags), std::memory_order_acq_rel);
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
