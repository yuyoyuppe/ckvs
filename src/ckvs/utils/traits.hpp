#pragma once

#include <tuple>
#include <type_traits>
#include <limits>
#include <cinttypes>

namespace ckvs { namespace utils {
namespace detail {

template <typename... Unsigneds>
constexpr size_t pick_least_unsigned_type_idx(std::tuple<Unsigneds...> &&, size_t u)
{
  static_assert((std::is_unsigned_v<Unsigneds> && ...));

  const size_t limits_vals[] = {static_cast<size_t>(std::numeric_limits<Unsigneds>::max())...};
  const size_t type_sizes[]  = {sizeof(Unsigneds)...};

  const auto invalid_size = std::numeric_limits<size_t>::max();

  size_t selected_idx  = invalid_size;
  auto   selected_size = invalid_size;
  for(size_t i = 0; i < std::size(limits_vals); ++i)
  {
    if(u <= limits_vals[i] && selected_size > type_sizes[i])
    {
      selected_idx  = i;
      selected_size = type_sizes[i];
    }
  }
  return selected_idx;
}
}


// similar to https://www.boost.org/doc/libs/1_47_0/libs/integer/doc/html/boost_integer/integer.html#boost_integer.integer.sized but allows supplementing arbitrary type list to select from.
template <size_t Value, typename... T>
class least_unsigned
{
  using tuple_t = std::tuple<T...>;

public:
  using type = std::tuple_element_t<detail::pick_least_unsigned_type_idx(tuple_t{}, Value), tuple_t>;
};

template <size_t Value, typename... T>
using least_unsigned_t = typename least_unsigned<Value, T...>::type;

template <class T>
constexpr bool is_serializable_v = std::is_standard_layout_v<T> && std::is_trivially_copyable_v<T>;


}}