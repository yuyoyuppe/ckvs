#pragma once

#include <algorithm>

#include "../utils/common.hpp"

namespace ckvs { namespace detail {
enum class node_kind : uint8_t { Internal, Root, Leaf, RootLeaf };

template <typename NodeT, typename PayloadT>
union slot
{
  PayloadT _payload;
  NodeT *  _child;

  inline slot & operator=(NodeT * child) noexcept
  {
    _child = child;
    return *this;
  }

  inline slot & operator=(const PayloadT payload) noexcept
  {
    _payload = payload;
    return *this;
  }

  explicit slot(PayloadT v) noexcept : _payload(v) {}

  explicit slot(NodeT * v) noexcept : _child(v) {}

  slot() = default;

  inline bool eq_children(const slot & rhs) const { return _child == rhs._child; }

  inline bool eq_payloads(const slot & rhs) const { return _payload == rhs._payload; }
};

template <typename Config>
struct node
{
  using config    = Config;
  using index_t   = typename config::index_t;
  using payload_t = typename config::payload_t;
  using key_t     = typename config::key_t;
  using slot_t    = slot<node, payload_t>;

  static constexpr size_t order = config::order;

  node_kind _kind;
  index_t   _nKeys;
  key_t     _keys[order - 1];
  slot_t    _slots[order];

  node(const node_kind kind) noexcept : _kind(kind) {}

  bool has_links() const noexcept { return _kind < node_kind::Leaf; }

  bool is_root() const noexcept { return _kind == node_kind::Root || _kind == node_kind::RootLeaf; };

  key_t distribute_payload(node *          greater_dst,
                           const index_t   idx,
                           const key_t     new_key,
                           const payload_t new_value) noexcept
  {
    CKVS_ASSERT(has_links() == greater_dst->has_links());

    _nKeys = greater_dst->_nKeys = order / 2;

    const index_t split_start_idx = config::middle_idx + 1;

    key_t sparse_key;
    // If the new KV-pair goes to greater_dst, copy the greater half while reserving KV-slot for it.
    if(idx > config::middle_idx)
    {
      sparse_key = idx == split_start_idx ? new_key : _keys[split_start_idx];
      std::copy(_keys + split_start_idx, _keys + idx, greater_dst->_keys);
      greater_dst->_keys[idx - split_start_idx] = new_key;
      std::copy(_keys + idx, _keys + config::node_max_keys, greater_dst->_keys + 1 + (idx - split_start_idx));

      std::copy(_slots + split_start_idx, _slots + idx, greater_dst->_slots);
      greater_dst->_slots[idx - split_start_idx] = new_value;
      std::copy(_slots + idx, _slots + config::node_max_keys + 1, greater_dst->_slots + 1 + (idx - split_start_idx));
    }
    // Otherwise, just copy the greater half + the biggest KV from our half, since we must remove it to create
    // place for the new KV.
    else
    {
      sparse_key = _keys[config::middle_idx];
      std::copy(_keys + config::middle_idx, _keys + config::node_max_keys, greater_dst->_keys);
      std::copy(_slots + config::middle_idx, _slots + config::node_max_keys + 1, greater_dst->_slots);
      if(idx == config::middle_idx)
      {
        _keys[idx]  = new_key;
        _slots[idx] = new_value;
      }
      else
      {
        --_nKeys;
        insert_hinted(new_key, slot_t{new_value}, idx, idx);
      }
    }
    return sparse_key;
  }

  key_t distribute_children(node *        greater_dst,
                            const index_t insert_idx,
                            const key_t   new_key,
                            node *        new_right_child) noexcept
  {
    CKVS_ASSERT(has_links());
    CKVS_ASSERT(_nKeys == config::node_max_keys);
    CKVS_ASSERT(greater_dst->has_links());

    // Whether new_key goes to this node or dst determines which indices we should shift
    const bool insert_here = insert_idx < config::middle_idx;

    const key_t sparse_key = config::middle_idx == insert_idx ? new_key : _keys[config::middle_idx - insert_here];

    // Since for x keys we have x+1 slots, we can't distribute evenly
    _nKeys              = config::middle_idx;
    greater_dst->_nKeys = order - _nKeys - 1;

    // We must copy the greater half of all KVs to the new greater_dst node. Since we out of space to store the
    // new KV-pair, we use imaginary node of order + 1 which could hold all our KVs as well as the new KV-pair.
    // To iterate its greater half, we map its indices to ours/dst and substitute the new KV accordingly.
    const index_t split_start_idx = config::middle_idx + 1;
    for(index_t img_node_idx = split_start_idx; img_node_idx <= order; ++img_node_idx)
    {
      const index_t dst_idx = img_node_idx - split_start_idx;

      const bool    shift_slots           = !insert_here && img_node_idx > insert_idx;
      const bool    use_new_child         = !insert_here && img_node_idx == insert_idx + 1;
      const index_t node_val_idx          = img_node_idx - shift_slots - insert_here;
      greater_dst->_slots[dst_idx]._child = use_new_child ? new_right_child : _slots[node_val_idx]._child;

      if(img_node_idx == order)
        continue;

      const bool    shift_key_indices = !insert_here && img_node_idx >= insert_idx;
      const bool    use_new_key       = !insert_here && img_node_idx == insert_idx;
      const index_t node_key_idx      = img_node_idx - shift_key_indices - insert_here;
      greater_dst->_keys[dst_idx]     = use_new_key ? new_key : _keys[node_key_idx];
    }

    // Since we copied our last KV pair to dst to be able to fit the new KV pair, we need to remove it from this
    if(insert_here)
    {
      const index_t max_key_idx = _nKeys - 1;
      remove(_keys[max_key_idx], _slots[_nKeys], max_key_idx);
      insert_hinted(new_key, slot_t{new_right_child}, insert_idx, insert_idx + 1);
    }

    return sparse_key;
  }

  index_t find_key_index(const key_t key) const noexcept
  {
    return static_cast<index_t>(std::upper_bound(_keys, _keys + _nKeys, key) - _keys);
  }

  void insert_detail(const key_t key, const slot_t & slot_value, const index_t key_idx, const index_t value_idx) noexcept
  {
    const bool links = has_links();

    CKVS_ASSERT(links ? _nKeys <= config::node_max_keys : _nKeys < config::node_max_keys);

    std::copy_backward(_keys + key_idx, _keys + _nKeys, _keys + _nKeys + 1);
    _keys[key_idx] = key;

    std::copy_backward(_slots + value_idx, _slots + _nKeys + links, _slots + _nKeys + 1 + links);
    _slots[value_idx] = slot_value;

    ++_nKeys;
  }

  void insert_hinted(const key_t    key,
                     const slot_t & slot_value,
                     const index_t  key_idx_hint,
                     const index_t  value_idx_hint) noexcept
  {
    insert_detail(key, slot_value, key_idx_hint, value_idx_hint);
  }

  void insert(const key_t key, const slot_t & slot_value) noexcept
  {
    const index_t key_idx = find_key_index(key);
    insert_detail(key, slot_value, key_idx, key_idx + 1);
  }

  void remove(const key_t key, const slot_t & slot_value, index_t key_idx_hint) noexcept
  {
    CKVS_ASSERT(key_idx_hint - has_links() < _nKeys);

    if(_keys[key_idx_hint] != key)
      key_idx_hint = find_key_index(key);

    std::copy(_keys + key_idx_hint + 1, _keys + _nKeys, _keys + key_idx_hint);

    const bool hint_correct =
      has_links() ? _slots[key_idx_hint].eq_children(slot_value) : _slots[key_idx_hint].eq_payloads(slot_value);
    const index_t slot_idx = hint_correct ? key_idx_hint : key_idx_hint + 1;
    CKVS_ASSERT(has_links() ? _slots[slot_idx].eq_children(slot_value) : _slots[key_idx_hint].eq_payloads(slot_value));

    std::copy(_slots + slot_idx + 1, _slots + _nKeys + 1, _slots + slot_idx);
    --_nKeys;
  }

  void drain_from(node * greater_partial_node) noexcept
  {
    CKVS_ASSERT(has_links() == greater_partial_node->has_links());

    CKVS_ASSERT(!_nKeys || _keys[_nKeys - 1] < greater_partial_node->_keys[0]);

    for(index_t i = 0; i <= greater_partial_node->_nKeys; ++i)
    {
      _slots[_nKeys + i] = greater_partial_node->_slots[i];

      if(i == greater_partial_node->_nKeys)
        continue;
      _keys[_nKeys + i] = greater_partial_node->_keys[i];
    }
    _nKeys += greater_partial_node->_nKeys;
    CKVS_ASSERT(_nKeys <= config::node_max_keys);

    greater_partial_node->_nKeys = 0;
    if(has_links())
      return;

    _slots[config::node_max_keys]._child = greater_partial_node->_slots[config::node_max_keys]._child;
  }

  key_t steal_smallest(node * greater_node) noexcept
  {
    const index_t slot_idx = 0;
    const index_t key_idx  = 0;

    const auto  stolen_slot = greater_node->_slots[slot_idx];
    const auto  stolen_key  = greater_node->_keys[key_idx];
    const key_t sparse_key  = greater_node->_keys[!has_links()];
    greater_node->remove(stolen_key, stolen_slot, key_idx);

    const index_t value_idx_hint = _nKeys + has_links();
    insert_hinted(stolen_key, stolen_slot, _nKeys, value_idx_hint);

    return sparse_key;
  }

  key_t steal_greatest(node * lesser_node) noexcept
  {
    const index_t slot_idx = lesser_node->_nKeys - !has_links();
    const index_t key_idx  = lesser_node->_nKeys - 1;

    const auto stolen_slot = lesser_node->_slots[slot_idx];
    const auto stolen_key  = lesser_node->_keys[key_idx];
    lesser_node->remove(stolen_key, stolen_slot, key_idx);
    insert_hinted(stolen_key, stolen_slot, 0, 0);

    return stolen_key;
  }

  node * next_sibling() const noexcept
  {
    CKVS_ASSERT(!has_links() && !is_root());
    return _slots[config::node_max_keys]._child;
  }

  bool has_enough_slots() const noexcept
  {
    return _nKeys >= config::middle_idx + !has_links() || is_root() && _nKeys > 0;
  }

  bool can_coalesce_with(const node & sibling) const noexcept
  {
    CKVS_ASSERT(sibling._kind == _kind);
    return sibling._nKeys + _nKeys <= config::node_max_keys - has_links();
  }
};
}}