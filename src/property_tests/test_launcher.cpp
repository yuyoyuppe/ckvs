#include <sstream>
#include <iostream>
#include <random>

#include <utils/common.hpp>
#include <utils/mem.hpp>

using test_signature_t = void(const size_t iteration, std::default_random_engine & gen, std::ostream & os);

test_signature_t slotted_page_test, bp_tree_test, lockfree_pool_test, page_cache_test, paged_file_test,
  flat_numeric_ckvs_test;

struct test_run_params
{
  test_signature_t * _func;
  size_t             _nIterations; // todo: max seconds, not that.
};

const bool dbg =
#if defined(DEBUG)
  true;
#
#else
  false;
#endif

const test_run_params tests[] = {{flat_numeric_ckvs_test, 1},
                                 {bp_tree_test, dbg ? 20 : 40},
                                 {slotted_page_test, dbg ? 500 : 3000},
                                 {lockfree_pool_test, dbg ? 30 : 1200},
                                 {page_cache_test, dbg ? 500 : 5000},
                                 {paged_file_test, 3}};

using namespace ckvs;

int main(int argc, char ** argv)
{
  leak_reporter leaks_scope;

  uint32_t seed = 0;
  if(argc >= 2)
    std::istringstream(argv[1]) >> seed;
  if(!seed)
    seed = std::random_device{}();

  std::ostream   null{nullptr};
  std::ostream & os =
#if !defined(VERBOSE_TEST)
    null;
#else
    std::cout;
#endif
  std::default_random_engine gen{seed};
  std::cout << "Launching property-based testing, seed: " << seed << "\n";
  size_t i = 0;
  for(auto [test, iters] : tests)
  {
    std::cout << "Running test #" << i++ << " ...\n";
    const double total_time_sec = utils::profiled([&, test = std::move(test), iters = std::move(iters)] {
      for(size_t iter = 1; iter <= iters; ++iter)
        test(iter, gen, os);
    });
    std::cout << "OK -- " << total_time_sec << "sec\n";
  }
  std::cout << "All tests passed." << std::endl;
  return 0;
}