#include "paged_file.hpp"
#include "ring.hpp"

#include <condition_variable>
#include <mutex>

#define LLFIO_STATIC_LINK
#define LLFIO_HEADERS_ONLY 1
#define NTKERNEL_ERROR_CATEGORY_INLINE 0
#define LLFIO_EXPERIMENTAL_STATUS_CODE 1
//#define LLFIO_LOGGING_LEVEL 6
#pragma warning(push)
#pragma warning(disable : 4273)
#pragma warning(disable : 4005)
#include <llfio.hpp>
#pragma warning(pop)

#include <variant>
#include <csignal>
#include <boost/predef.h>

namespace ckvs {
namespace fs    = std::filesystem;
namespace llfio = LLFIO_V2_NAMESPACE;
using namespace utils;

struct truncation_request
{
  int64_t _diff_in_pages = 0;
};

using io_worker_request_t = std::variant<truncation_request, paged_file::page_request>;
using file_handle_t       = llfio::async_file_handle;
using async_scratch_t     = std::aligned_storage_t<256>;

static inline void empty_read_rb(file_handle_t *, file_handle_t::io_result<file_handle_t::buffers_type> && res)
{
  CKVS_ASSERT(res.has_value());
}
static inline void empty_write_rb(file_handle_t *, file_handle_t::io_result<file_handle_t::const_buffers_type> && res)
{
  CKVS_ASSERT(res.has_value());
}

class paged_file::impl
{
  static_assert(CHAR_BIT == 8, "obscure architectures are not supported");
  static constexpr bool little_endian =
#if defined(BOOST_ENDIAN_LITTLE_WORD)
    true;
#else
    false;
#endif
  static_assert(little_endian, "only little endian architectures are binary compatible");

  friend class paged_file;

  using lock_t                                 = std::mutex;
  static constexpr size_t max_pending_requests = 32;

  std::atomic_bool   _io_thread_has_error = false;
  std::exception_ptr _io_thread_error     = nullptr;

  std::thread _io_thread;

  lock_t                  _io_work_signal_mutex;
  bool                    _io_thread_shutdown_request = false;
  bool                    _io_thread_has_work         = false;
  std::condition_variable _io_has_work_signal;

  paged_file::page_request::completion_callback_t _read_cb;
  paged_file::page_request::completion_callback_t _write_cb;
  paged_file::truncate_callback_t                 _truncate_cb;

  ring<io_worker_request_t, max_pending_requests, std::mutex> _pending_requests;

  size_t             _page_size;
  std::atomic_size_t _size_in_pages = paged_file::invalid_size;

  llfio::result<file_handle_t::io_state_ptr> io_handle_read_write(file_handle_t &            file_handle,
                                                                  paged_file::page_request & req,
                                                                  async_scratch_t &          scratch)
  {
    if(req.is_read_request())
    {
      file_handle_t::buffer_type dst_buf{req.buffer(), _page_size};
      return file_handle.async_read(
        {{dst_buf}, _page_size * req.page_id()}, empty_read_rb, {reinterpret_cast<char *>(&scratch), sizeof(scratch)});
    }
    else
    {
      file_handle_t::const_buffer_type src_buf{req.buffer(), _page_size};
      return file_handle.async_write(
        {{src_buf}, _page_size * req.page_id()}, empty_write_rb, {reinterpret_cast<char *>(&scratch), sizeof(scratch)});
    }
  }

  void io_handle_truncation(file_handle_t & file_handle, truncation_request & req)
  {
    const size_t extent = file_handle.maximum_extent().value();
    CKVS_ASSERT(static_cast<int64_t>(extent / _page_size) + req._diff_in_pages >= 0);
    llfio::truncate(file_handle, extent + _page_size * req._diff_in_pages).value();
    _size_in_pages = extent / _page_size + req._diff_in_pages;
  }

public:
  void io_thread_worker(fs::path absolute_path)
  {
    size_t                                                        nRead_requests  = 0;
    size_t                                                        nWrite_requests = 0;
    size_t                                                        nRequests       = 0;
    std::array<paged_file::page_request, max_pending_requests>    page_requests;
    std::array<file_handle_t::io_state_ptr, max_pending_requests> async_results;

    std::array<async_scratch_t, max_pending_requests> scratch;
    llfio::io_service                                 svc;
    file_handle_t                                     file_handle = llfio::async_file(svc,
                                                  {},
                                                  absolute_path.c_str(),
                                                  llfio::file_handle::mode::write,
                                                  llfio::file_handle::creation::if_needed,
                                                  llfio::file_handle::caching::none)
                                  .value();
    _size_in_pages = file_handle.maximum_extent().value() / _page_size;

    while(!_io_thread_shutdown_request)
    {
      truncation_request reduced_trunc{};
      {
        std::unique_lock<lock_t> lock{_io_work_signal_mutex};
        if(!_io_has_work_signal.wait_for(lock, paged_file::io_sleep_time, [&] { return _io_thread_has_work; }))
        {
          if(_io_thread_shutdown_request)
            break;
          else
            continue;
        }

        while((nWrite_requests + nRead_requests) != max_pending_requests)
        {
          io_worker_request_t req;
          if(!_pending_requests.try_pop(req))
            break;
          // Sort page requests by type and move them to page_requests, as if it's a double-ended queue.
          // Write requests are pushed_front() and read requests are pushed_back().
          // To get a new total extent diff, we just reduce all truncation requests to a single value.
          std::visit(overloaded{[&](paged_file::page_request & req) {
                                  if(req.is_write_request())
                                    page_requests[nWrite_requests++] = std::move(req);
                                  else
                                    page_requests[max_pending_requests - ++nRead_requests] = std::move(req);
                                },
                                [&](truncation_request & req) { reduced_trunc._diff_in_pages += req._diff_in_pages; }},
                     req);
        }
      }
      _io_thread_has_work = !_pending_requests.empty();
      if(reduced_trunc._diff_in_pages != 0)
      {
        io_handle_truncation(file_handle, reduced_trunc);
        reduced_trunc._diff_in_pages = 0;
        _truncate_cb(_size_in_pages);
      }

      nRequests = nRead_requests + nWrite_requests;
      if(!nRequests)
        continue;
      // We need to loop here, since async_* funcs could fail with resource_unavailable_try_again if there's no async resources available at the moment.
      while(nRequests)
      {
        auto try_submit_request = [&](const size_t req_idx) {
          // If already launched this request => don't have to do anything
          if(async_results[req_idx] != nullptr)
            return;

          auto ret = io_handle_read_write(file_handle, page_requests[req_idx], scratch[req_idx]);

          if(!ret.has_error())
          {
            // file_handle_t::io_state_ptr should be held during the entire requst processing time
            async_results[req_idx] = std::move(ret.assume_value());
            --nRequests;
          }
          else
            CKVS_ASSERT(ret.error() == llfio::errc::resource_unavailable_try_again);
        };
        // Submit read/write callbacks
        for(size_t i = 0; i < nWrite_requests; ++i)
          try_submit_request(i);
        for(size_t i = max_pending_requests - nRead_requests; i < max_pending_requests; ++i)
          try_submit_request(i);

        // Loop while until all submitted io requests are completed
        while(svc.run().value())
          continue;
      }
      // Now we can batch-call all read/write callbacks and reset all state for a new interation
      for(size_t i = max_pending_requests - nRead_requests; i < max_pending_requests; ++i)
      {
        _read_cb(page_requests[i].page_id());
        async_results[i] = nullptr;
      }
      for(size_t i = 0; i < nWrite_requests; ++i)
      {
        _write_cb(page_requests[i].page_id());
        async_results[i] = nullptr;
      }
      nRequests = nWrite_requests = nRead_requests = 0;
    }
  }

  void notify_io_thread()
  {
    {
      std::unique_lock lock{_io_work_signal_mutex};
      _io_thread_has_work = true;
    }
    _io_has_work_signal.notify_one();
  }

  void try_rethrow_io_thread_ex()
  {
    if(_io_thread_has_error.load(std::memory_order_acquire))
      std::rethrow_exception(_io_thread_error);
  }

  impl(fs::path &&                         absolute_path,
       const size_t                        page_size,
       page_request::completion_callback_t read_callback,
       page_request::completion_callback_t write_callback,
       truncate_callback_t                 truncate_callback)
    : _read_cb{std::move(read_callback)}
    , _write_cb{std::move(write_callback)}
    , _truncate_cb{std::move(truncate_callback)}
    , _page_size{page_size}
  {
    _io_thread = std::thread{[this, ap = std::move(absolute_path)]() mutable {
      try
      {
        io_thread_worker(std::move(ap));
      }
      catch(...)
      {
        _io_thread_error = std::current_exception();
        _io_thread_has_error.store(true, std::memory_order_release);
      }
    }};
  }

  ~impl()
  {
    if(_io_thread.joinable())
    {
      _io_thread_shutdown_request = true;
      _io_thread.join();
    }
  }
};


bool is_valid_page_size(const size_t page_size) { return page_size % llfio::utils::page_size() == 0; }


paged_file::paged_file(fs::path &&                         absolute_path,
                       const size_t                        page_size,
                       page_request::completion_callback_t read_callback,
                       page_request::completion_callback_t write_callback,
                       truncate_callback_t                 truncate_callback)
  : _impl{nullptr, nullptr}
{

  if(!is_valid_page_size(page_size))
    throw std::invalid_argument("paged_file: page_size should be a multiple of OS page size");

  _impl = decltype(_impl){new paged_file::impl{std::move(absolute_path),
                                               page_size,
                                               std::move(read_callback),
                                               std::move(write_callback),
                                               std::move(truncate_callback)},
                          &default_delete<paged_file::impl>};
}

size_t paged_file::size_in_pages()
{
  _impl->try_rethrow_io_thread_ex();
  return _impl->_size_in_pages;
}

void paged_file::shrink(const size_t nPages)
{
  _impl->try_rethrow_io_thread_ex();

  while(!_impl->_pending_requests.try_push(truncation_request{-static_cast<int64_t>(nPages)}))
    continue;
  _impl->notify_io_thread();
}


void paged_file::extend(size_t nPages)
{
  _impl->try_rethrow_io_thread_ex();

  while(!_impl->_pending_requests.try_push(truncation_request{static_cast<int64_t>(nPages)}))
    continue;
  _impl->notify_io_thread();
}

void paged_file::request(paged_file::page_request r)
{
  _impl->try_rethrow_io_thread_ex();

  while(!_impl->_pending_requests.try_push(r))
    continue;
  _impl->notify_io_thread();
}

}