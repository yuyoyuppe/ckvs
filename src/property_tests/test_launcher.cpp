#include <sstream>
#include <iostream>
#include <random>

#include "utils/common.hpp"

using test_signature_t = void(const size_t iteration, std::default_random_engine & gen, std::ostream & os);

test_signature_t slotted_page_test, bp_tree_test, lockfree_pool_test;

struct test_run_params
{
  test_signature_t * _func;
  size_t             _nIterations;
};

const test_run_params tests[] = {{bp_tree_test, 40}, {slotted_page_test, 3000}, {lockfree_pool_test, 1000}};

using namespace ckvs;

int main(int argc, char ** argv)
{
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
    std::cout << "Running test #" << i++ << " ...";
    const double total_time_sec = utils::profiled([&, test = test, iters = iters] {
      for(size_t iter = 1; iter < iters; ++iter)
        test(iter, gen, os);
    });
    std::cout << "OK -- " << total_time_sec << "sec\n";
  }
  std::cout << "All tests passed." << std::endl;
  return 0;
}