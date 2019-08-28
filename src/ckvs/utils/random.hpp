#pragma once

#include "common.hpp"
#include "variant.hpp"

#include <set>
#include <optional>
#include <vector>
#include <random>

#include <boost/range.hpp>

// Mostly stuff for simple property-based testing

namespace ckvs { namespace utils {

template <typename ContainterT,
          typename IterT   = decltype(std::begin(std::declval<ContainterT &>())),
          typename ResultT = std::vector<boost::iterator_range<IterT>>>
auto split_to_random_parts(ContainterT && v, const size_t nParts, std::default_random_engine & gen)
  -> std::optional<ResultT>
{
  std::optional<ResultT> result{std::nullopt};
  if(nParts > std::size(v))
    return result;

  result = ResultT{};
  std::set<size_t> random_cuts;

  std::uniform_int_distribution<size_t> rnd{1ull, std::size(v) - 1};
  while(std::size(random_cuts) != nParts - 1)
    random_cuts.emplace(rnd(gen));

  result->reserve(nParts);

  auto to_iter = [&v](const size_t idx) {
    auto result = std::begin(v);
    std::advance(result, idx);
    return result;
  };

  size_t prev_cut = 0;
  for(const auto cut_point : random_cuts)
  {
    result->emplace_back(to_iter(prev_cut), to_iter(cut_point));
    prev_cut = cut_point;
  }
  result->emplace_back(to_iter(prev_cut), std::end(v));

  return result;
}

template <
  typename T,
  typename = std::enable_if_t<std::is_arithmetic_v<T>>,
  typename DisT =
    std::conditional_t<std::is_floating_point_v<T>, std::uniform_real_distribution<T>, std::uniform_int_distribution<T>>>
void random_value(T & val, std::default_random_engine & gen)
{
  DisT dis(std::numeric_limits<T>::min(), std::numeric_limits<T>::max() - 1);
  val = dis(gen);
}

inline void random_value(const size_t                 min_length,
                         const size_t                 max_length,
                         std::string &                s,
                         std::default_random_engine & gen)
{
  const size_t len = std::uniform_int_distribution<size_t>{min_length, max_length}(gen);

  std::uniform_int_distribution<short> char_gen{32, 126};
  s.resize(len);
  for(char & c : s)
    while(!isalnum(c = static_cast<char>(char_gen(gen))))
      continue;
}

template <typename VariantT>
void random_variant(VariantT & var, const size_t max_binary_size, std::default_random_engine & gen)
{
  default_init_variant(var, gen() % std::variant_size_v<VariantT>);
  std::visit(overloaded{[&](auto && val) { random_value(val, gen); },
                        [&](std::string & str) { random_value(1ull, max_binary_size, str, gen); }},
             var);
}

}}