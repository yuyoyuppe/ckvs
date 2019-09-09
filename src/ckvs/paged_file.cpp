#include "paged_file.hpp"
#include "bounded_queue.hpp"

#include <condition_variable>
#include <mutex>
#include <future>

#define LLFIO_STATIC_LINK
#define LLFIO_HEADERS_ONLY 1
#define NTKERNEL_ERROR_CATEGORY_INLINE 0
#define LLFIO_EXPERIMENTAL_STATUS_CODE 1
#pragma warning(push) // let the llfio compile in peace
#pragma warning(disable : 4273)
#pragma warning(disable : 4005)
#include <llfio.hpp>
#pragma warning(pop)

#include <variant>
#include <csignal>

namespace ckvs {
namespace fs    = std::filesystem;
namespace llfio = LLFIO_V2_NAMESPACE;
using namespace utils;

struct truncation_request
{
  int64_t _diff_in_pages = 0;
};

// could use highest bit to save some bytes.
using io_worker_request_t = std::variant<truncation_request, page_descriptor *>;
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
  friend class paged_file;

  using lock_t                                 = std::mutex;
  static constexpr size_t max_pending_requests = 16;

  std::atomic_bool   _io_thread_has_error = false;
  std::exception_ptr _io_thread_error     = nullptr;

  std::thread        _io_thread;
  std::promise<bool> _io_thread_ready;

  lock_t                  _io_work_signal_mutex;
  bool                    _io_thread_shutdown_request = false;
  bool                    _io_thread_has_work         = false;
  std::condition_variable _io_has_work_signal;

  bounded_queue<io_worker_request_t, max_pending_requests, std::mutex> _pending_requests;

  uint64_t _page_size;
  bool     is_write_request(page_descriptor * desc) const noexcept
  {
    auto flags = desc->acquire_flags();
    CKVS_ASSERT(!!(flags & page_descriptor_flags::being_flushed) || !!(flags & page_descriptor_flags::being_loaded));
    return !!(flags & page_descriptor_flags::being_flushed);
  }

  inline bool is_async_res_ok(const llfio::result<file_handle_t::io_state_ptr> & res)
  {
    return !res.has_error() && !res.has_exception() && !res.has_failure();
  }

  template <typename VariantBufferT>
  inline llfio::result<file_handle_t::io_state_ptr> io_handle_read_write(file_handle_t &   file_handle,
                                                                         page_descriptor * desc,
                                                                         async_scratch_t & scratch,
                                                                         VariantBufferT &  buf)
  {
    CKVS_ASSERT((reinterpret_cast<uintptr_t>(desc->raw_page()) & (minimal_required_page_alignment - 1u)) == 0 &&
                desc->raw_page() != nullptr);
    if(is_write_request(desc))
    {
      // We are flushing the page, so it's ok to let read it
      desc->lock_shared();
      using selected_buf_t = file_handle_t::const_buffer_type;
      buf                  = selected_buf_t{desc->raw_page(), _page_size};
      llfio::result<file_handle_t::io_state_ptr> result =
        file_handle.async_write({{std::get<selected_buf_t>(buf)}, _page_size * desc->page_id()},
                                empty_write_rb,
                                {reinterpret_cast<char *>(&scratch), sizeof(scratch)});
      if(!is_async_res_ok(result))
        desc->unlock_shared();
      return result;
    }
    else
    {
      // There's no valid page data yet, so we must lock exclusive. We don't try&assert, since we're contending with paged shared_lazy_page_lock.lock()
      desc->lock();
      using selected_buf_t = file_handle_t::buffer_type;
      buf                  = selected_buf_t{desc->raw_page(), _page_size};
      llfio::result<file_handle_t::io_state_ptr> result =
        file_handle.async_read({{std::get<selected_buf_t>(buf)}, _page_size * desc->page_id()},
                               empty_read_rb,
                               {reinterpret_cast<char *>(&scratch), sizeof(scratch)});
      if(!is_async_res_ok(result))
        desc->unlock();
      return result;
    }
  }

  void io_handle_truncation(file_handle_t & file_handle, truncation_request & req)
  {
    const uint64_t extent = file_handle.maximum_extent().value();
    CKVS_ASSERT(static_cast<int64_t>(extent / _page_size) + req._diff_in_pages >= 0);
    llfio::truncate(file_handle, extent + _page_size * req._diff_in_pages).value();
  }

  void io_thread_worker(fs::path absolute_path)
  {
    size_t                                                        nRead_requests  = 0;
    size_t                                                        nWrite_requests = 0;
    size_t                                                        nRequests       = 0;
    std::array<page_descriptor *, max_pending_requests>           page_requests;
    std::array<file_handle_t::io_state_ptr, max_pending_requests> async_results;
    std::array<std::variant<file_handle_t::const_buffer_type, file_handle_t::buffer_type>, max_pending_requests> buffers;

    std::array<async_scratch_t, max_pending_requests> scratch;
    llfio::io_service                                 svc;
    file_handle_t                                     file_handle = llfio::async_file(svc,
                                                  {},
                                                  absolute_path.c_str(),
                                                  llfio::file_handle::mode::write,
                                                  llfio::file_handle::creation::if_needed,
                                                  llfio::file_handle::caching::none)
                                  .value();

    uint64_t           current_size_in_pages = file_handle.maximum_extent().value() / _page_size;
    truncation_request reduced_truncation_amount;
    if(current_size_in_pages < extend_granularity)
    {
      reduced_truncation_amount._diff_in_pages = extend_granularity - current_size_in_pages;
      io_handle_truncation(file_handle, reduced_truncation_amount);
    }
    _io_thread_ready.set_value(true);
    for(;;)
    {
      reduced_truncation_amount = {};
      {
        std::unique_lock<lock_t> lock{_io_work_signal_mutex};
        if(!_io_has_work_signal.wait_for(
             lock, paged_file::io_sleep_time, [&] { return _io_thread_has_work || !_pending_requests.empty(); }))
        {
          if(_io_thread_shutdown_request && !_io_thread_has_work)
          {
            _io_thread_has_work = !_pending_requests.empty();
            if(!_io_thread_has_work)
              break;
          }
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
          std::visit(overloaded{[&](page_descriptor * desc) {
                                  CKVS_ASSERT(desc->page_id() <= extend_granularity ||
                                              desc->page_id() - extend_granularity < current_size_in_pages);
                                  if(is_write_request(desc))
                                    page_requests[nWrite_requests++] = desc;
                                  else
                                    page_requests[max_pending_requests - ++nRead_requests] = desc;
                                },
                                [&](truncation_request & req) {
                                  reduced_truncation_amount._diff_in_pages += req._diff_in_pages;
                                }},
                     req);
        }
      }
      _io_thread_has_work = !_pending_requests.empty();

      // Automatically extend as needed, if we've requested a read of nonexistent page
      for(size_t i = max_pending_requests - nRead_requests; i < max_pending_requests; ++i)
      {
        if(page_requests[i]->page_id() >= current_size_in_pages)
        {
          // Make sure something reasonable was requested
          reduced_truncation_amount._diff_in_pages += extend_granularity;
        }
      }

      if(reduced_truncation_amount._diff_in_pages != 0)
      {
        io_handle_truncation(file_handle, reduced_truncation_amount);
        current_size_in_pages += reduced_truncation_amount._diff_in_pages;
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

          auto ret = io_handle_read_write(file_handle, page_requests[req_idx], scratch[req_idx], buffers[req_idx]);

          if(is_async_res_ok(ret))
          {
            // file_handle_t::io_state_ptr should be held during the entire requst processing time
            async_results[req_idx] = std::move(ret.assume_value());
            --nRequests;
          }
          else
          {
            CKVS_ASSERT(ret.error() == llfio::errc::resource_unavailable_try_again);
          }
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
#if defined(TOTAL_REQUEST_CHECK)
      for(size_t i = max_pending_requests - nRead_requests; i < max_pending_requests; ++i)
      {
        for(size_t j = 0; j < nWrite_requests; ++j)
          CKVS_ASSERT(page_requests[i]->page_id() != page_requests[j]->page_id());
      }

      for(size_t i = max_pending_requests - nRead_requests; i < max_pending_requests; ++i)
      {
        for(size_t j = max_pending_requests - nRead_requests; j < max_pending_requests; ++j)
          if(i != j)
            CKVS_ASSERT(page_requests[i]->page_id() != page_requests[j]->page_id());
      }

      for(size_t i = 0; i < nWrite_requests; i++)
        for(size_t j = 0; j < nWrite_requests; ++j)
          if(i != j)
            CKVS_ASSERT(page_requests[i]->page_id() != page_requests[j]->page_id());
#endif
      for(size_t i = max_pending_requests - nRead_requests; i < max_pending_requests; ++i)
      {
        auto                  flags = page_requests[i]->acquire_flags();
        page_descriptor_flags new_flags;
        do
        {
          new_flags = flags & ~page_descriptor_flags::being_loaded;
        } while(!page_requests[i]->try_update_flags(flags, new_flags));
        page_requests[i]->unlock();
        async_results[i] = nullptr;
      }
      for(size_t i = 0; i < nWrite_requests; ++i)
      {
        auto                  flags = page_requests[i]->acquire_flags();
        page_descriptor_flags new_flags;
        do
        {
          new_flags = flags & ~page_descriptor_flags::being_flushed;
        } while(!page_requests[i]->try_update_flags(flags, new_flags));
        page_requests[i]->unlock_shared();
        async_results[i] = nullptr;
      }
      nRequests = nWrite_requests = nRead_requests = 0;
    }
  }

public:
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

  impl(fs::path && absolute_path, const uint64_t page_size) : _page_size{page_size}
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
    _io_thread_ready.get_future().wait();
  }

  ~impl()
  {
    if(_io_thread.joinable())
    {
      _io_thread_shutdown_request = true;
      _io_thread.join();
    }
    CKVS_ASSERT(_pending_requests.empty());
  }
};


bool is_valid_page_size(const uint64_t page_size) { return page_size % llfio::utils::page_size() == 0; }


paged_file::paged_file(fs::path && absolute_path, const uint64_t page_size) : _impl{nullptr, nullptr}
{
  if(!is_valid_page_size(page_size))
    throw std::invalid_argument("paged_file: page_size should be a multiple of OS page size");

  _impl = decltype(_impl){new paged_file::impl{std::move(absolute_path), page_size}, &default_delete<paged_file::impl>};
}

bool paged_file::idle() const noexcept
{
  _impl->try_rethrow_io_thread_ex();
  return (!_impl->_io_thread_has_work && _impl->_pending_requests.empty()) || _impl->_io_thread_has_error;
}

void paged_file::shrink(const uint64_t nPages)
{
  while(!_impl->_pending_requests.try_push(truncation_request{-static_cast<int64_t>(nPages)}))
    _impl->try_rethrow_io_thread_ex();
  _impl->notify_io_thread();
}

void paged_file::request(page_descriptor * r)
{
  while(!_impl->_pending_requests.try_push(r))
    _impl->try_rethrow_io_thread_ex();
  _impl->notify_io_thread();
}

}