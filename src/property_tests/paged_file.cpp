#include <array>
#include <unordered_map>
#include <set>
#include <algorithm>
#include <thread>
#include <vector>
#include <numeric>
#include <atomic>

#include <paged_file.hpp>
#include <utils/random.hpp>

#include <emmintrin.h>
#include <mutex>
#include <ring.hpp>

using namespace ckvs;
using namespace utils;

constexpr size_t page_size = 8192 * 2;
constexpr size_t max_pages = 15000;
struct page_t
{
  size_t _id;
  alignas(512) std::array<uint8_t, page_size> _data{};
};
using page_id_t = uint32_t;

#include <fstream>
void syntetic_write_measure(const size_t nPages, const char * path, std::default_random_engine & gen, std::ostream & os)
{
  os << "ofstream completed " << profiled([&] {
    std::vector<page_t> pages{nPages};
    std::ofstream       of{path};
    std::vector<size_t> page_ids(nPages);
    for(size_t i = 0; i < nPages; ++i)
      page_ids[i] = i;
    std::shuffle(begin(page_ids), end(page_ids), gen);
    for(const auto page_id : page_ids)
    {
      auto & page = pages[page_id];
      for(auto & d : page._data)
        d = 'a';
      of.seekp(page_size * page_id, std::ios_base::beg);
      of.write(reinterpret_cast<const char *>(page._data.data()), page_size);
    }
  }) << " sec\n";
}

void paged_file_test_for_path(const size_t                 nPages,
                              const char *                 path,
                              std::default_random_engine & gen,
                              std::ostream &               os)
{
  const size_t nThreads = std::thread::hardware_concurrency();

  std::vector<page_t> pages{nPages};
  for(size_t i = 0; i < nPages; ++i)
    pages[i]._id = i;
  std::vector<uint8_t> char_to_fill_for_thread;
  try
  {
    std::atomic_size_t page_read_left  = nPages;
    std::atomic_size_t page_write_left = nPages;

    const auto read_cb     = [&](const size_t) { --page_read_left; };
    const auto write_cb    = [&](const size_t) { --page_write_left; };
    const auto truncate_cb = [&](const size_t new_nPages) { os << "now # " << new_nPages << " pages\n"; };

    auto   pf              = std::make_unique<paged_file>(path, page_size, read_cb, write_cb, truncate_cb);
    size_t paged_file_size = paged_file::invalid_size;
    while(paged_file_size == paged_file::invalid_size)
    {
      paged_file_size = pf->size_in_pages();
      _mm_pause();
    }
    if(paged_file_size < nPages)
      pf->extend(nPages - paged_file_size);
    else if(paged_file_size > nPages)
      pf->shrink(paged_file_size - nPages);
    while(pf->size_in_pages() != nPages)
      _mm_pause();

    std::vector<std::thread> threads;
    auto                     page_ranges = *split_to_random_parts(pages, nThreads, gen);
    char_to_fill_for_thread.resize(nThreads);
    for(auto & c : char_to_fill_for_thread)
      c = static_cast<unsigned char>(gen() + 1);
    for(size_t i = 0; i < nThreads; ++i)
    {
      threads.emplace_back([&page_ranges, i, &pf, &char_to_fill_for_thread, &page_write_left, &os] {
        for(auto & page : page_ranges[i])
        {
          for(auto & d : page._data)
            d = char_to_fill_for_thread[i];
          pf->request(
            paged_file::page_request::make_write_request(page._id, reinterpret_cast<std::byte *>(page._data.data())));
        }
      });
    }
    auto start = std::chrono::high_resolution_clock::now();
    for(auto & t : threads)
      if(t.joinable())
        t.join();
    while(page_write_left)
      _mm_pause();
    pf = nullptr;
    auto res =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start).count() /
      1000.;
    os << "async completed in " << res << " sec\n";

    pf = std::make_unique<paged_file>(path, page_size, read_cb, write_cb, truncate_cb);
    do
    {
      paged_file_size = pf->size_in_pages();
    } while(paged_file_size == paged_file::invalid_size);
    CKVS_ASSERT(paged_file_size == nPages);
    for(auto & p : pages)
      p._data = {};

    threads.clear();
    for(size_t i = 0; i < nThreads; ++i)
    {
      threads.emplace_back([&page_ranges, i, &pf, &char_to_fill_for_thread, &page_read_left, &os] {
        for(auto & page : page_ranges[i])
        {
          pf->request(
            paged_file::page_request::make_read_request(page._id, reinterpret_cast<std::byte *>(page._data.data())));
        }
      });
    }
    for(auto & t : threads)
      if(t.joinable())
        t.join();
    while(page_read_left)
      _mm_pause();

    for(size_t i = 0; i < nThreads; ++i)
      for(auto & p : page_ranges[i])
      {
        auto & d = p._data;
        CKVS_ASSERT(d[0] == char_to_fill_for_thread[i]);
        CKVS_ASSERT(d[page_size - 1] == char_to_fill_for_thread[i]);
        // Faster checking a few random chars
        for(size_t n = 0; n < 16; ++n)
        {
          size_t random_idx = gen() % page_size;
          CKVS_ASSERT(d[random_idx] == char_to_fill_for_thread[i]);
        }
      }
  }
  catch(std::exception & ex)
  {
    os << "exception: " << ex.what() << '\n';
    CKVS_ASSERT(false);
  }
  catch(...)
  {
    os << "unknown exception!\n";
    CKVS_ASSERT(false);
  }
}


void paged_file_test(const size_t, std::default_random_engine & gen, std::ostream & os)
{
  const size_t nPages = gen() % max_pages;
#if defined(ENABLE_SSD_TEST)
  os << "===HDD==\n";
  paged_file_test_for_path(nPages, HDD_PAGED_FILE_PATH, gen, os);
  syntetic_write_measure(nPages, HDD_PAGED_FILE_PATH, gen, os);
#endif

#if defined(ENABLE_SSD_TEST)
  os << "===SSD==\n";
  paged_file_test_for_path(nPages, SSD_PAGED_FILE_PATH, gen, os);
  syntetic_write_measure(nPages, SSD_PAGED_FILE_PATH, gen, os);
#endif

#if defined(ENABLE_RAM_TEST)
  os << "===RAM==\n";
  paged_file_test_for_path(nPages, RAM_PAGED_FILE_PATH, gen, os);
  syntetic_write_measure(nPages, RAM_PAGED_FILE_PATH, gen, os);
#endif
}