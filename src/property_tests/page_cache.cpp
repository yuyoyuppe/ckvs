#include <page_cache.hpp>
#include <array>
#include <unordered_map>
#include <algorithm>

#include <utils/random.hpp>

using namespace ckvs;
using namespace utils;

using page_t    = std::array<char, 64>;
using page_id_t = uint32_t;
using pool_t    = lockfree_pool<page_t>;

void page_cache_test(const size_t /*iteration*/, std::default_random_engine & gen, std::ostream & /*os*/)
{
  const page_id_t nThreads = std::thread::hardware_concurrency() - 1;

  std::uniform_int_distribution<> nat_gen{1, 9};
  const page_id_t                 page_io_capacity = std::max(nThreads, static_cast<page_id_t>(4 << nat_gen(gen)));
  const page_id_t       to_fill = std::max(nThreads, static_cast<page_id_t>(page_io_capacity / 10. * nat_gen(gen)));
  lockfree_pool<page_t> toy_file_view{page_io_capacity};

  page_cache<page_t, page_id_t> page_cache{page_io_capacity};

  std::uniform_int_distribution<page_id_t> id_gen{1, std::numeric_limits<page_id_t>::max()};

  std::unordered_map<page_id_t, page_t *> pages;

  for(page_id_t i = 0; i < to_fill; ++i)
  {
    const auto id       = id_gen(gen);
    const auto raw_page = toy_file_view.acquire();
    sprintf(raw_page->data(), "%u", id);
    page_cache.add_page(id, raw_page);
    pages[id] = raw_page;
    {
      auto maybe_page = page_cache.try_get_page_shared(id);
      CKVS_ASSERT(maybe_page != std::nullopt);

      page_id_t read_back_id = 0;
      sscanf(std::get<0>(*maybe_page).data(), "%u", &read_back_id);
      CKVS_ASSERT(read_back_id == id);
    }
  }

  auto parts = split_to_random_parts(pages, nThreads, gen);
  if(!parts)
    return;
  auto page_ranges = *parts;

  std::vector<std::thread> threads;
  for(size_t i = 0; i < nThreads; ++i)
  {
    threads.emplace_back([&page_ranges, &page_cache, i] {
      for(auto [page_id, raw_page] : page_ranges[i])
      {
        bool still_in_cache = true;
        {
          if(page_id & 1)
            still_in_cache = page_cache.try_get_page_shared(page_id) != std::nullopt;
          else
            still_in_cache = page_cache.try_get_page_exclusive(page_id) != std::nullopt;
        }
        if(still_in_cache)
          page_cache.evict_page(page_id);
      }
    });
  }
  for(auto & t : threads)
    if(t.joinable())
      t.join();

  for(auto [page_id, raw_page] : pages)
  {
    auto maybe_page = page_cache.try_get_page_shared(page_id);
    CKVS_ASSERT(maybe_page == std::nullopt);
  }
}