#pragma once

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
  using truncate_callback_t = std::function<void(const size_t new_number_of_pages)>;
  class page_request
  {
    size_t                            _page_id;
    void *                            _buffer;
    static inline constexpr uintptr_t highest_ptr_bit = uintptr_t{1} << (sizeof(_buffer) * CHAR_BIT - 1ull);

  public:
    using completion_callback_t = std::function<void(const size_t page_id)>;

    bool is_read_request() const noexcept { return (highest_ptr_bit & reinterpret_cast<uintptr_t>(_buffer)) == 0; }

    bool is_write_request() const noexcept { return (highest_ptr_bit & reinterpret_cast<uintptr_t>(_buffer)) != 0; }

    size_t page_id() const noexcept { return _page_id; }

    std::byte * buffer() const noexcept
    {
      return reinterpret_cast<std::byte *>(reinterpret_cast<uintptr_t>(_buffer) & ~highest_ptr_bit);
    }
    inline static page_request make_read_request(size_t page_id, std::byte * src_buffer) noexcept
    {
      page_request res;
      res._page_id = page_id;
      res._buffer  = src_buffer;
      return res;
    }

    inline static page_request make_write_request(size_t page_id, std::byte * dst_buffer) noexcept
    {
      page_request res;
      res._page_id = page_id;
      res._buffer  = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(dst_buffer) | highest_ptr_bit);
      return res;
    }
  };

  paged_file(std::filesystem::path &&            absolute_path,
             const size_t                        page_size,
             page_request::completion_callback_t read_callback,
             page_request::completion_callback_t write_callback,
             truncate_callback_t                 truncate_callback);

  constexpr static inline size_t                           invalid_size  = std::numeric_limits<size_t>::max();
  constexpr static inline std::chrono::duration<long long> io_sleep_time = std::chrono::seconds{1};
  size_t                                                   size_in_pages();
  void                                                     shrink(const size_t nPages);
  void                                                     extend(const size_t nPages);
  void                                                     request(page_request r);
};

}
