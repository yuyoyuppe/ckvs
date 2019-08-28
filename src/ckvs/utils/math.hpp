#pragma once

#include <cinttypes>

namespace ckvs { namespace utils {

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