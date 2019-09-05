#pragma once

#include "page_descriptor.hpp"

#include "utils/common.hpp"
#include "utils/validated_val.hpp"

#include <cinttypes>
#include <filesystem>
#include <functional>
#include <limits>
#include <chrono>

namespace ckvs {

class paged_file
{
  class impl;
  std::unique_ptr<impl, decltype(&utils::default_delete<impl>)> _impl;

public:
  constexpr static inline uint64_t invalid_size = std::numeric_limits<size_t>::max();
  constexpr static inline std::chrono::duration<long long, std::milli> io_sleep_time = std::chrono::milliseconds{10ull};

  constexpr static inline uint32_t extend_granularity              = 1000;
  constexpr static inline uint32_t minimal_required_page_alignment = 512;

  paged_file(std::filesystem::path && absolute_path, const uint64_t page_size);

  bool idle() const noexcept;
  void request(page_descriptor * r);
  // Unsafe to submit concurrently with page requests, only use during thread-unsafe compaction etc.
  void shrink(const uint64_t nPages);
};

}
