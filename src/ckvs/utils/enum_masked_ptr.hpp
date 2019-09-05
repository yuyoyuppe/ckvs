#pragma once

#include <inttypes.h>
#include <limits.h>
#include <cstddef>
#include <type_traits>
#include <array>

namespace ckvs { namespace utils {
// Packs enum flag values into ptr, forgets flags on assignment
template <typename T, typename Enum, Enum... EnumValues>
class enum_masked_ptr
{
  uintptr_t _ptr;

  static constexpr uintptr_t nth_highest_bit(const size_t n)
  {
    return uintptr_t{1} << (sizeof(_ptr) * CHAR_BIT - size_t{n} - 1);
  }

  static constexpr size_t enum_val_idx(const Enum val, const std::array<Enum, sizeof...(EnumValues)> vals)
  {
    using int_t = std::underlying_type_t<Enum>;
    for(size_t i = 0; sizeof...(EnumValues); ++i)
    {
      if(static_cast<int_t>(val) == static_cast<int_t>(vals[i]))
        return i;
    }
    return 0;
  }

  static constexpr uintptr_t calc_clear_mask(const size_t nHighestBits)
  {
    uintptr_t mask = 0;
    for(size_t i = 0; i < nHighestBits; ++i)
      mask |= nth_highest_bit(i);
    return ~mask;
  }

  static inline constexpr uintptr_t clear_mask = calc_clear_mask(sizeof...(EnumValues));

  static_assert(sizeof(void *) == 8 && sizeof...(EnumValues) <= 16 ||
                sizeof(void *) == 4 && sizeof...(EnumValues) == 1);

  T * cleared() const noexcept { return reinterpret_cast<T *>(_ptr & clear_mask); }

public:
  enum_masked_ptr() : _ptr{reinterpret_cast<uintptr_t>(nullptr)} {}

  explicit enum_masked_ptr(T * ptr) : _ptr{reinterpret_cast<uintptr_t>(ptr)} {}

  explicit operator T *() const noexcept { return cleared(); }

  operator bool() const noexcept { return cleared() != nullptr; }

  T & operator*() noexcept { return *cleared(); }

  T * operator->() noexcept { return cleared(); }

  template <Enum val, size_t idx = enum_val_idx(val, {EnumValues...})>
  bool is_set() const noexcept
  {
    return _ptr & nth_highest_bit(idx);
  }

  template <Enum val, size_t idx = enum_val_idx(val, {EnumValues...})>
  void clear() noexcept
  {
    _ptr &= ~nth_highest_bit(idx);
  }

  template <Enum val, size_t idx = enum_val_idx(val, {EnumValues...})>
  void set() noexcept
  {
    _ptr |= nth_highest_bit(idx);
  }

  friend inline bool operator==(const enum_masked_ptr lhs, const enum_masked_ptr rhs) noexcept
  {
    return lhs._ptr == rhs._ptr;
  }

  friend inline bool operator!=(const enum_masked_ptr lhs, const enum_masked_ptr rhs) noexcept
  {
    return lhs._ptr != rhs._ptr;
  }

  enum_masked_ptr & operator=(const enum_masked_ptr & lhs) = default;

  inline enum_masked_ptr & operator=(T * ptr) noexcept
  {
    _ptr = reinterpret_cast<uintptr_t>(ptr);
    return *this;
  }
};
}}