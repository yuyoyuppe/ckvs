#pragma once

#include <string>
#include <random>
#include <string_view>
#include <cinttypes>
#include <cstddef>
#include <chrono>

template <typename T>
struct show_type
{
  show_type() { static_assert(sizeof(T) > 2000000); }
};

namespace ckvs { namespace utils {

struct pinned
{
  pinned()               = default;
  pinned(const pinned &) = delete;
  pinned(pinned &&)      = delete;

  pinned & operator=(const pinned &) = delete;
  pinned & operator=(pinned &&) = delete;
};

struct noncopyable
{
  noncopyable & operator=(const noncopyable &) = delete;
  noncopyable(const noncopyable &)             = delete;

  noncopyable() = default;

  noncopyable(noncopyable &&) = default;
  noncopyable & operator=(noncopyable &&) = default;
};


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

template <typename T>
inline void default_delete(T * p) noexcept
{
  delete p;
}

template <typename Func>
double quick_profile(Func && f, bool one_shot = true, double runtime_in_seconds = 1.)
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

constexpr uint64_t operator"" _magic_sig(const char * s, size_t len)
{
  uint64_t result = 0;
  for(size_t i = 0; i < len; ++i)
    result |= static_cast<uint64_t>(s[i]) << (8 * i);
  return result;
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
