#pragma once

#include "../utils/spinlock.hpp"

#include <shared_mutex>

#include <boost/intrusive/slist.hpp>
#include <boost/thread/null_mutex.hpp>

namespace ckvs { namespace detail {
namespace bi = boost::intrusive;

template <typename DescriptorT>
struct alignas(std::hardware_destructive_interference_size) bucket
{
  using lock_t = utils::spinlock;
  lock_t                                               _bucket_lock;
  bi::slist<DescriptorT, bi::constant_time_size<true>> _descriptors;
  using descriptor_iterator = typename bi::slist<DescriptorT, bi::constant_time_size<true>>::iterator;
};
}}