
#include "utils/common.hpp"
#include "utils/random.hpp"
#include "utils/traits.hpp"

#include "slotted_page.hpp"

#include <iostream>
#include <unordered_map>
#include <algorithm>

using namespace ckvs;
using namespace utils;

constexpr size_t page_size         = 1024;
constexpr size_t max_bin_slot_size = 512;
constexpr size_t max_variants      = 256;

using page_t = SlottedPage<page_size>;

using page_value_variant_t = std::variant<uint64_t, uint32_t, float, double, std::string>;
static_assert(4 == sizeof(float) && 8 == sizeof(double));
static_assert(std::is_same_v<uint8_t, least_unsigned_t<255, uint16_t, uint8_t, uint32_t>> &&
              std::is_same_v<uint16_t, least_unsigned_t<256, uint16_t, uint8_t, uint32_t>>);

void slotted_page_test(const size_t iteration_n, std::default_random_engine & gen, std::ostream &)
{
  std::vector<page_value_variant_t> page_values;

  std::unordered_map<uint16_t, page_value_variant_t *> page_values_ids;
  page_values.resize(std::min(iteration_n, max_variants));

  page_t page;

  for(size_t i = 0; i < size(page_values); ++i)
  {
    random_variant(page_values[i], std::min(max_bin_slot_size, iteration_n), gen);
    const auto var_view = as_string_view(page_values[i]);
    while(!page.has_space_for(var_view) && !page_values_ids.empty())
    {
      auto it                       = begin(page_values_ids);
      const auto [slot_id, var_ptr] = *it;
      const auto existing_view      = page.get_span(slot_id);
      CKVS_ASSERT(existing_view == as_string_view(*var_ptr));
      page.remove_slot(slot_id);
      page_values_ids.erase(it);
    }
    CKVS_ASSERT(page.has_space_for(var_view));
    const auto slot_id       = page.add_slot(var_view);
    page_values_ids[slot_id] = &page_values[i];
    const auto existing_view = page.get_span(slot_id);
    CKVS_ASSERT(var_view == existing_view);
  }
  for(const auto [slot_id, var_ptr] : page_values_ids)
  {
    const auto existing_view = page.get_span(slot_id);
    CKVS_ASSERT(existing_view == as_string_view(*var_ptr));
    page.remove_slot(slot_id);
  }
  CKVS_ASSERT(page.number_of_slots() == 0);
}
