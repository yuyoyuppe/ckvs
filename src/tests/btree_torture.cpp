#include "ckvs.h"

#include <iostream>
#include <vector>
#include <random>
#include <sstream>

void quick_regress(const size_t n_vals, std::ostream & os)
{
  BTree<4> tree;

  std::default_random_engine gen{std::random_device{}()};

  std::vector<key_t> vals;
  vals.resize(n_vals);
  for(int i = 0; i < size(vals); ++i)
    vals[i] = i + 1;
  shuffle(begin(vals), end(vals), gen);
  for(const auto val : vals)
  {
    tree.insert(val, new key_t{val * val});
    const auto res = tree.find(val);
    assert(res != nullptr);
  }
  for(const auto val : vals)
  {
    const auto res = tree.find(val);
    assert(res != nullptr);
    assert(*res == val * val);
  }
  tree.debug_inspect(os);
}

int main()
{
  {
    std::ostringstream oss;

    for(int i = 1; i < 256; ++i)
    {
      oss.seekp(0);
      oss.clear();
      quick_regress(i, oss);
      oss << std::ends;
    }
  }
  BTree<4> tree;
  const std::vector<key_t> vals = {1, 8, 6, 7, 10, 3, 9, 2, 12, 4, 5, 11};
  for(const auto val : vals)
  {
    tree.insert(val, new key_t{ val * val });
    const auto res = tree.find(val);
    assert(res != nullptr);
  }
  tree.remove(12);
  tree.debug_inspect(std::cout);
  tree.remove(11);
  std::cout << "after deletion:\n";
  tree.debug_inspect(std::cout);
  return 0;
}