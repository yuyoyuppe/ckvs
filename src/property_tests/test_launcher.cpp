#include <sstream>
#include <iostream>
#include <random>

#include <utils/common.hpp>
#include <utils/mem.hpp>

using test_signature_t = void(const size_t iteration, std::default_random_engine & gen, std::ostream & os);

test_signature_t hckvs, slotted_page_test, bp_tree_test, lockfree_pool_test, page_cache_test, paged_file_test,
  flat_numeric_ckvs_persistence_test, flat_numeric_ckvs_contention_test, flat_numeric_ckvs_tiny_order_test,
  slotted_ckvs_test, flat_numeric_ckvs_query_test;

struct test_run_params
{
  test_signature_t * _func;
  size_t             _nIterations;
};

const bool dbg =
#if defined(DEBUG)
  true;
#else
  false;
#endif

const test_run_params tests[] = {
  //{flat_numeric_ckvs_contention_test, dbg ? 3 : 7},
  //{flat_numeric_ckvs_persistence_test, dbg ? 2 : 5},
  //{flat_numeric_ckvs_tiny_order_test, dbg ? 50 : 10},
  {bp_tree_test, dbg ? 10 : 30},
  //{flat_numeric_ckvs_custom_test, 1},
  //{slotted_ckvs_test, 1}
};

using namespace ckvs;

#include <csignal>
// Catch the abort calls
void signal_handler(int) { std::exit(EXIT_FAILURE); }

int main(int argc, char ** argv)
{
  //leak_reporter leaks_scope;
  std::signal(SIGABRT, signal_handler);

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
    const size_t iter_offset    = 8;
    const double total_time_sec = utils::quick_profile([&, test = std::move(test), iters = std::move(iters)] {
      for(size_t iter = 1 + iter_offset; iter <= iters + iter_offset; ++iter)
        test(iter, gen, os);
    });
    std::cout << "OK -- " << total_time_sec << "sec\n";
  }
  std::cout << "All tests passed." << std::endl;
  return 0;
}