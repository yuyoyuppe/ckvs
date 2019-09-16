#pragma once
#include "utils/common.hpp"
#include "slotted_storage.hpp"

#include <cstring>

namespace ckvs {
template <size_t page_size, typename Config>
struct alignas(utils::page_align) bp_tree_node_page
{
  using node_t = typename Config::node_t;
  node_t                                                                           _node;
  std::aligned_storage_t<Config::order>                                            _metadata;
  slotted_storage<page_size - sizeof(_node) - sizeof(_metadata) - alignof(node_t)> _slotted_storage;
};

template <size_t page_size, bool IsKey, typename Config>
struct slotted_value_handle
{
  using index_t = typename Config::index_t;
  using page_t  = bp_tree_node_page<page_size, Config>;
  using node_t  = typename Config::node_t;

  index_t  _index = {};
  page_t * _page  = nullptr;

  slotted_value_handle(node_t * const node, const index_t idx) noexcept
    : _page{reinterpret_cast<page_t *>(node)}, _index{idx}
  {
  }
  slotted_value_handle()                             = default;
  slotted_value_handle(const slotted_value_handle &) = default;

  slotted_value_handle & operator=(const slotted_value_handle &) = default;
};

template <typename T, typename = void>
struct slotted_value_view
{
  template <typename T = T>
  slotted_value_view(T = {})
  {
    static_assert(false, "this type is not supported!");
  }
};

template <typename T>
struct slotted_value_view<
  T,
  std::enable_if_t<std::is_integral_v<T> || std::is_same_v<float, T> || std::is_same_v<double, T>>>
{
  T        _value;
  uint32_t _timestamp{};

  slotted_value_view(const T value) : _value{value} {}

  utils::span<const char> as_span() const { return {reinterpret_cast<const char *>(&_value), sizeof(T)}; }

  void set_expiration(const uint32_t timestamp) { _timestamp = timestamp; }

  bool has_expiration() const { return _timestamp != 0; }
};

template <typename T>
struct slotted_value_view<T, std::enable_if_t<std::is_same_v<T, utils::span<const char>>>>
{
  T        _value;
  uint32_t _timestamp = {};

  slotted_value_view(const T value) : _value{value} {}

  utils::span<const char> as_span() const { return _value; }

  void set_expiration(const uint32_t timestamp) { _timestamp = timestamp; }

  bool has_expiration() const { return _timestamp != 0; }
};

template <typename T>
slotted_value_view(const T)->slotted_value_view<T>;

enum class val_type : uint8_t { Integer, Float, Double, Binary };

template <typename T, typename = void>
struct to_val_type
{
};

template <typename T>
struct to_val_type<T, std::enable_if_t<std::is_integral_v<T>>>
{
  constexpr static inline val_type value = val_type::Integer;
};

template <typename T>
struct to_val_type<T, std::enable_if_t<std::is_same_v<float, T>>>
{
  constexpr static inline val_type value = val_type::Float;
};

template <typename T>
struct to_val_type<T, std::enable_if_t<std::is_same_v<double, T>>>
{
  constexpr static inline val_type value = val_type::Double;
};

template <typename T>
struct to_val_type<T, std::enable_if_t<std::is_same_v<utils::span<const char>, T>>>
{
  constexpr static inline val_type value = val_type::Binary;
};

using timestamp_t = uint32_t;
struct metadata
{
  val_type _key_type : 2;
  val_type _payload_type : 2;
  uint8_t  _key_overflow : 1;
  uint8_t  _payload_overflow : 1;
  uint8_t  _has_expiration : 1;
  uint8_t  _not_removed : 1;

  inline bool is_removed() const { return !_not_removed; }

  inline bool has_expiration() const { return _has_expiration != 0; }

  inline bool is_key_small_type() const { return _key_type != val_type::Binary; }

  inline bool is_payload_small_type() const { return _payload_type != val_type::Binary; }

  inline bool is_key_inline() const { return is_key_small_type() && !has_expiration(); }

  inline bool is_payload_inline() const { return is_payload_small_type() && !has_expiration(); }

  inline bool key_has_overflow() const { return _key_overflow != 0; }

  inline bool payload_has_overflow() const { return _payload_overflow != 0; }
};

static_assert(sizeof(metadata) == sizeof(uint8_t));

template <size_t page_size, bool IsKey, typename ValueT, typename PageT>
struct slotted_value
{
  using value_t = ValueT;
  using page_t  = PageT;
  static_assert(sizeof(page_t) == page_size);

  value_t _value;

  slotted_value() = default;

  explicit slotted_value(value_t value) noexcept : _value{value} {}

  inline page_t & my_page() const
  {
    return *reinterpret_cast<page_t *>(reinterpret_cast<uintptr_t>(this) & ~(utils::page_align - 1));
  }

  inline size_t my_idx() const
  {
    if constexpr(IsKey)
      return reinterpret_cast<uintptr_t>(this) - reinterpret_cast<uintptr_t>(&my_page()._node._keys[0]);
    else
      return reinterpret_cast<uintptr_t>(this) - reinterpret_cast<uintptr_t>(&my_page()._node._slots[0]._payload);
  }

  inline metadata & my_metadata() noexcept { return reinterpret_cast<metadata *>(&my_page()._metadata)[my_idx()]; }

  inline metadata & my_metadata() const { return const_cast<slotted_value *>(this)->my_metadata(); }

  inline value_t & my_value() noexcept
  {
    value_t *  v;
    auto &     page           = my_page();
    const bool node_has_links = page._node.has_links();
    if constexpr(!IsKey)
      CKVS_ASSERT(!node_has_links);

    const bool has_expiration = metadata().has_expiration();
    const bool small_type     = IsKey ? metadata().is_key_small_type() : metadata().is_payload_small_type();
    if constexpr(IsKey)
      v = &page._node._keys[my_idx()];
    else
      v = &page._node._slots[my_idx()]._payload;
    // Key is always inlined if it has small type
    if(small_type && (IsKey || !has_expiration))
      return *v;

    auto timestamp_and_val = page._slotted_storage.get_mut_span(static_cast<uint16_t>(*v));

    v = reinterpret_cast<value_t *>(timestamp_and_val.data() + sizeof(timestamp_t));
    return *v;
  }

  inline const value_t & my_value() const noexcept { return const_cast<slotted_value *>(this)->my_value(); }

  inline bool operator==(const slotted_value & rhs) const noexcept
  {
    const auto & lhs_md = my_metadata();
    const auto & rhs_md = rhs.my_metadata();
    if(lhs_md.is_removed() || rhs_md.is_removed())
      return false;
    if constexpr(IsKey)
    {
      if(lhs_md._key_type != rhs_md._key_type)
        return false;
    }
    else
    {
      if(lhs_md._payload_type != rhs_md._payload_type)
        return false;
    }
    // Small key type
    if(lhs_md.is_key_small_type())
      return my_value() == rhs.my_value();

    CKVS_ASSERT(false);

    return false;
  }

  inline bool operator!=(const slotted_value & rhs) const noexcept { return !(*this == rhs); }

  inline bool operator<(const slotted_value & rhs) const noexcept { return my_value() < rhs.my_value(); }

  inline slotted_value & operator=(const slotted_value & rhs)
  {
    _value        = rhs._value;
    my_metadata() = rhs.my_metadata();
    return *this;
  }

  template <typename Config>
  inline slotted_value & operator=(const slotted_value_handle<page_size, IsKey, Config> & /*rhs*/)
  {
    CKVS_ASSERT(false); // todo: implement something similar to slotted_value_view
    return *this;
  }

  template <typename Config>
  friend bool operator<(const slotted_value_handle<page_size, IsKey, Config> & lhs, const slotted_value & rhs)
  {
    if constexpr(IsKey)
      return lhs._page->_node._keys[lhs._index] < rhs;
    else
      return lhs._page->_node._slots[lhs._index]._payload < rhs;
  }

  template <typename T>
  friend bool operator<(const slotted_value_view<T> & lhs, const slotted_value & rhs)
  {
    utils::span<const char> rhs_span{reinterpret_cast<const char *>(&rhs.my_value()), sizeof(value_t)};
    return lhs.as_span() < rhs_span;
  }

  template <typename T>
  friend bool operator==(const slotted_value_view<T> & lhs, const slotted_value & rhs)
  {
    auto &   md = rhs.my_metadata();
    val_type sv_type;
    if constexpr(IsKey)
      sv_type = md._key_type;
    else
      sv_type = md._payload_type;

    if(sv_type != val_type::Integer)
      return false;
    return lhs._value == rhs.my_value();
  }

  template <typename T>
  slotted_value & operator=(const slotted_value_view<T> & rhs)
  {
    auto &     md        = my_metadata();
    auto &     page      = my_page();
    const auto view_type = to_val_type<T>::value;
    if constexpr(IsKey)
      md._key_type = view_type;
    else
      md._payload_type = view_type;

    const bool view_has_expiration = rhs.has_expiration();
    if constexpr(IsKey) // Expiration should be stored in a payload only
    {
      CKVS_ASSERT(!view_has_expiration);
    }
    const utils::span<const char> rhs_span = rhs.as_span();
    if(view_has_expiration || view_type == val_type::Binary)
    {
      const uint16_t offset    = view_has_expiration ? sizeof(rhs._timestamp) : 0;
      const bool     has_space = page._slotted_storage.has_space_for(rhs_span, offset);
      if(has_space)
      {
        const uint16_t slot_id = page._slotted_storage.add_slot(rhs_span, offset);
        if(view_has_expiration)
        {
          auto mut_span = page._slotted_storage.get_mut_span(slot_id);
          std::memcpy(mut_span.data(), &rhs._timestamp, sizeof(rhs._timestamp));
        }
        if constexpr(sizeof(slot_id) < sizeof(value_t))
          my_value() = {};
        std::memcpy(&my_value(), &slot_id, sizeof(slot_id));
      }
      else
      {
        if constexpr(IsKey)
          md._key_overflow = 1;
        else
          md._payload_overflow = 1;

        CKVS_ASSERT(false); // todo: request new page support!
      }
    }
    else
    {
      // We must clean the value storage if we're storing smaller type. e.g. int8_t inside a uint64_t.
      if constexpr(sizeof(T) < sizeof(value_t))
        my_value() = {};
      std::memcpy(&my_value(), rhs_span.data(), rhs_span.length());
    }

    return *this;
  }
};

template <template <size_t, bool, typename...> typename TemplateT, size_t page_size, bool IsKey, typename... OtherParams>
struct apply_slotted_params
{
  template <typename Config>
  using type = TemplateT<page_size, IsKey, OtherParams..., Config>;
};

}