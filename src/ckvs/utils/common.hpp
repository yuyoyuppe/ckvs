#pragma once

#include <string>
#include <variant>
#include <random>
#include <string_view>
#include <cinttypes>
#include <chrono>

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

template <typename... Ts>
struct overloaded : Ts...
{
  using Ts::operator()...;
};

template <typename... Ts>
overloaded(Ts...)->overloaded<Ts...>;

template <typename T, typename = std::enable_if_t<std::is_trivially_copyable_v<T>>>
std::string_view as_string_view(const T & v)
{
  return {reinterpret_cast<const char *>(&v), sizeof(T)};
}

template <typename... T>
std::string_view as_string_view(const std::variant<T...> & var)
{
  return std::visit(overloaded{[](auto && v) { return as_string_view(v); },
                               [](const std::string & s) -> std::string_view { return {s}; }},
                    var);
}

template <typename Func>
double profiled(Func && f, bool one_shot = true, double runtime_in_seconds = 1.)
{
  int    run_count = 0;
  auto   start     = std::chrono::high_resolution_clock::now();
  double runtime   = 0;
  while(++run_count && runtime < runtime_in_seconds)
  {
    f();
    runtime =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start).count() /
      1000.;
    if(one_shot)
      break;
  }
  return runtime / run_count;
}

constexpr double abs(const double number) { return number > 0. ? number : -number; }

constexpr size_t ceil(const double val)
{
  double fract = abs(val - static_cast<size_t>(val));
  return static_cast<size_t>(val - fract) + 1;
}

constexpr size_t floor(const double val)
{
  double fract = abs(val - static_cast<size_t>(val));
  return static_cast<size_t>(val - fract);
}

constexpr double approx_log(size_t base, size_t number)
{
  size_t a = 0;
  size_t b = 0;

  while(number >>= 1)
    b++;
  while(base >>= 1)
    a++;
  return static_cast<double>(b) / a;
}

}}


#if defined(ASSERTS)
#define CKVS_ASSERT(cond)                                                                                              \
  if(!(cond))                                                                                                          \
  {                                                                                                                    \
    printf("CKVS_ASSERT failed at %s:%d\n", __FILE__, __LINE__);                                                       \
    __debugbreak();                                                                                                    \
    std::abort();                                                                                                      \
  }
#else
#define CKVS_ASSERT(condition) ((void)(condition))
#endif