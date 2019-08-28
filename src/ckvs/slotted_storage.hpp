#pragma once
#include <cinttypes>
#include <string_view>
#include <limits>
#include <type_traits>
#include <string_view>
#include <algorithm>

namespace ckvs {

// todo: use smallest possible sizes for description when the rest is stabilized
template <uint16_t size>
class slotted_storage
{
  struct slot_description_t
  {
    uint16_t _offset;
    uint16_t _size;

    static slot_description_t invalid() noexcept
    {
      return slot_description_t{std::numeric_limits<uint16_t>::max(), std::numeric_limits<uint16_t>::max()};
    }

    bool operator==(const slot_description_t other) const noexcept
    {
      return _offset == other._offset && _size == other._size;
    }

    bool operator!=(const slot_description_t other) const noexcept { return !(*this == other); }
  };

public:
  static_assert(size % 8 == 0);
  static_assert(size >= 16);

  using span_t                      = std::string_view;
  static constexpr size_t slot_size = sizeof(slot_description_t);
  using slot_id_t                   = uint16_t;

private:
  uint16_t  _slot_values_end_offset = 0;
  slot_id_t _used_slots             = 0;

  static constexpr uint16_t storage_size = size - sizeof(_slot_values_end_offset) - sizeof(_used_slots);

  uint8_t _storage[storage_size];

  slot_description_t & locate_slot_description(const slot_id_t slot_id) noexcept
  {
    return reinterpret_cast<slot_description_t *>(_storage)[slot_id];
  }
  const slot_description_t & locate_slot_description(const slot_id_t slot_id) const noexcept
  {
    return const_cast<slotted_storage *>(this)->locate_slot_description(slot_id);
  }

  uint16_t free_space() const noexcept
  {
    const auto usable_space = static_cast<int32_t>(storage_size) -
                              static_cast<int32_t>(_used_slots * sizeof(slot_description_t)) - _slot_values_end_offset;
    return static_cast<uint16_t>(std::max(0, usable_space));
  }

  span_t::value_type * slot_ptr(const slot_description_t desc) noexcept
  {
    return reinterpret_cast<span_t::value_type *>(_storage) + storage_size - desc._offset;
  }

  const span_t::value_type * slot_ptr(const slot_description_t desc) const noexcept
  {
    return const_cast<slotted_storage *>(this)->slot_ptr(desc);
  }

  std::pair<slot_description_t *, slot_id_t> try_find_vacant_slot() noexcept
  {
    for(slot_id_t i = 0; i < _used_slots; ++i)
    {
      auto & desc = locate_slot_description(i);
      if(desc == slot_description_t::invalid())
        return {&desc, i};
    }
    return {};
  }

  std::pair<slot_description_t *, slot_id_t> acquire_new_slot() noexcept
  {
    if(const auto maybe_slot = try_find_vacant_slot(); maybe_slot.first != nullptr)
      return maybe_slot;

    const slot_id_t slot_id = _used_slots++;
    auto &          desc    = locate_slot_description(slot_id);
    return {&desc, slot_id};
  }

public:
  slot_id_t number_of_slots() const noexcept { return _used_slots; }

  span_t get_span(const slot_id_t slot_id) const noexcept
  {
    const bool in_bounds = slot_id < _used_slots;
    CKVS_ASSERT(in_bounds);

    const auto description = locate_slot_description(slot_id);
    CKVS_ASSERT(description != slot_description_t::invalid());
    return span_t{slot_ptr(description), description._size};
  }

  bool has_space_for(const span_t slot_value) const noexcept
  {
    const size_t value_size = slot_value.size();
    const size_t desc_size  = sizeof(slot_description_t);
    if(value_size >= std::numeric_limits<uint16_t>::max() - desc_size)
      return false;
    return static_cast<uint16_t>(value_size) + desc_size <= free_space();
  }

  uint16_t add_slot(const span_t slot_value) noexcept
  {
    CKVS_ASSERT(has_space_for(slot_value));
    CKVS_ASSERT(slot_value.size());

    auto [desc, slot_id]      = acquire_new_slot();
    const uint16_t value_size = static_cast<uint16_t>(slot_value.size());
    desc->_size               = value_size;
    desc->_offset             = _slot_values_end_offset += value_size;
    slot_value.copy(slot_ptr(*desc), value_size);
    return slot_id;
  }

  void remove_slot(const slot_id_t slot_id) noexcept
  {
    auto &     remv_desc_ref = locate_slot_description(slot_id);
    const auto remv_desc     = remv_desc_ref;
    remv_desc_ref            = slot_description_t::invalid();

    // Move slot values to prevent gaps
    const auto slots_to_move_start = _storage + storage_size - _slot_values_end_offset;
    const auto slots_to_move_end   = reinterpret_cast<uint8_t *>(slot_ptr(remv_desc));
    const auto slots_new_end       = slots_to_move_end + remv_desc._size;
    std::copy_backward(slots_to_move_start, slots_to_move_end, slots_new_end);
    _slot_values_end_offset -= remv_desc._size;

    bool compacting = true;
    for(slot_id_t i = _used_slots; i != 0; --i)
    {
      auto & desc = locate_slot_description(i - 1);
      // Remove invalid slots from the end of slot descriptions
      if(compacting && desc == slot_description_t::invalid())
        --_used_slots;
      else
        compacting = false;

      // Update offsets of the valid slots
      if(desc != slot_description_t::invalid() && desc._offset > remv_desc._offset)
        desc._offset -= remv_desc._size;
    }
  }

  void merge_with(slotted_storage & /*other*/) noexcept
  {
    // todo: figure out how to do overflow first, then this
    CKVS_ASSERT(false);
  }
};
}
