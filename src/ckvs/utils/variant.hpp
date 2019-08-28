#pragma once

#include <variant>

#include "common.hpp"

namespace ckvs { namespace utils {
namespace detail {
template <typename VariantT, size_t... Indices>
void default_init_variant_detail(VariantT & v, const size_t alt_idx, std::index_sequence<Indices...>)
{
  (... ||
   (Indices == alt_idx && ((v = std::variant_alternative_t<Indices, std::remove_reference_t<VariantT>>{}), true)));
}
}

template <typename... T>
void default_init_variant(std::variant<T...> & v, const size_t alt_idx)
{
  detail::default_init_variant_detail(v, alt_idx, std::make_index_sequence<sizeof...(T)>{});
}
template <typename... T>
std::string_view as_string_view(const std::variant<T...> & var)
{
  return std::visit(overloaded{[](auto && v) { return as_string_view(v); },
                               [](const std::string & s) -> std::string_view { return {s}; }},
                    var);
}
}}
