
#include <utils/common.hpp>
#include <utils/random.hpp>

#include <lockfree_pool.hpp>


#include <vector>
#include <array>
#include <thread>
#include <algorithm>

using namespace ckvs;
using namespace utils;

void lockfree_pool_test(const size_t iteration, std::default_random_engine & gen, std::ostream &)
{
  using elem_t        = std::array<int, 128>;
  const size_t nPages = std::max(gen() % iteration + 1, 1000ull);

  {
    lockfree_pool<elem_t> pool{nPages};

    std::vector<elem_t *> elems{nPages};

    for(size_t i = 0; i < nPages; ++i)
    {
      elems[i] = pool.acquire();
      CKVS_ASSERT(elems[i] != nullptr);
    }
    CKVS_ASSERT(pool.acquire() == nullptr);

    for(size_t i = 0; i < nPages; ++i)
      pool.release(elems[i]);

    for(size_t i = 0; i < nPages; ++i)
    {
      elems[i] = pool.acquire();
      CKVS_ASSERT(elems[i] != nullptr);
    }
  }

  lockfree_pool<elem_t> pool{nPages};

  std::vector<elem_t *> elems{nPages};
  const size_t          nThreads = std::thread::hardware_concurrency() - 1;

  auto elem_ranges = *split_to_random_parts(elems, nThreads, gen);

  CKVS_ASSERT(std::size(elem_ranges) == nThreads);
  std::vector<std::thread> threads;
  for(size_t i = 0; i < nThreads; ++i)
  {
    threads.emplace_back([&elem_ranges, &pool, i] {
      for(auto & elem : elem_ranges[i])
      {
        elem = pool.acquire();
        pool.release(elem);
      }
    });
  }
  for(auto & t : threads)
    if(t.joinable())
      t.join();

  for(size_t i = 0; i < nPages; ++i)
  {
    elems[i] = pool.acquire();
    CKVS_ASSERT(elems[i] != nullptr);
  }

  for(size_t i = 0; i < nPages; ++i)
    pool.release(elems[i]);
}
