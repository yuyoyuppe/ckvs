#pragma once

#include "../utils/spinlock.hpp"

#include <shared_mutex>

namespace ckvs { namespace detail {

template <typename DescriptorT>
struct alignas(std::hardware_destructive_interference_size) bucket
{
  using lock_t = utils::spinlock;
  lock_t        _bucket_lock;
  DescriptorT * _descriptors = nullptr;
};
}}