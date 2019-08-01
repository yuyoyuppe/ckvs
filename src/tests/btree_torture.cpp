#include "ckvs.h"

#include <iostream>
#include <vector>
#include <random>

void stl_test()
{
  std::array<int, 10> a = {1, 3, 5, 7, 9};
  for(auto i : a)
    std::cout << i << ", ";
  std::cout << std::endl;

  for(int i = -3; i < 5; ++i)
  {
    if(i % 2 == 1)
      continue;
    auto actual_end = std::begin(a);
    while(*actual_end++ != 0)
      continue;
    const auto where = std::lower_bound(std::begin(a), actual_end, i);
    std::cout << i << " -> ";
    if(where == std::end(a))
    {
      std::cout << "past end.";
      *(where - 1) = std::move(i);
    }
    else
    {
      std::cout << *where;
      std::move_backward(where, actual_end - 1, actual_end);
      *where = std::move(i);
    }
    std::cout << '\n';
  }
  for(auto i : a)
    std::cout << i << ", ";
  std::cout << std::endl;
}

int main()
{
  BTree tree;

  std::default_random_engine gen{std::random_device{}()};
  std::uniform_int_distribution<> rnd(1, 10);

  std::array<key_t, order - 1> vals;
  for(int i = 0; i < size(vals); ++i)
    vals[i] = rnd(gen);
  for(const auto val : vals)
  {
    tree.insert(val, val * 10);
    const auto res = tree.find(val);
    if(!res.has_value())
      __debugbreak();
  }
  for(const auto i: vals)
  {
    const auto res = tree.find(i);
    if(res.has_value())
    {
      std::cout << i << " -> " << *res << '\n';
    }
    else
    {
      std::cout << i << " not found\n";
    }
  }

  //stl_test();
  return 0;
}