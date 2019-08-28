#pragma once

#include "../utils/spinlock.hpp"

#include <shared_mutex>

#include <boost/intrusive/slist.hpp>
#include <boost/thread/null_mutex.hpp>

namespace ckvs { namespace detail {
namespace bi      = boost::intrusive;
using link_mode_t = bi::link_mode<
#if defined(DEBUG)
  bi::link_mode_type::safe_link
#else
  bi::link_mode_type::normal_link
#endif
  >;

template <typename PageIdT, typename PageT>
struct alignas(std::hardware_destructive_interference_size) page_descriptor : bi::slist_base_hook<link_mode_t>
{
  using lock_t = std::shared_mutex;

  bool     _dirty             = false;
  bool     _recently_accessed = true;
  uint32_t _page_id           = 0;
  PageT *  _raw_page;
  lock_t   _page_lock;
};
template <typename DescriptorT>
struct alignas(std::hardware_destructive_interference_size) bucket
{
  using lock_t = utils::spinlock;

  lock_t                                               _bucket_lock;
  bi::slist<DescriptorT, bi::constant_time_size<true>> _descriptors;
};
}}