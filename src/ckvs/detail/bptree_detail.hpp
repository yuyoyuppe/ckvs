#pragma once

#include <algorithm>

#include "../utils/common.hpp"
#include "../utils/shared_null_mutex.hpp"

namespace ckvs { namespace detail {
enum class node_kind : uint8_t { Removed, Internal, Root, Leaf, RootLeaf };

template <typename NodeHandleT, typename PayloadT>
union slot
{
  PayloadT    _payload;
  NodeHandleT _child;

  explicit slot(PayloadT v) noexcept : _payload(v) {}
  explicit slot(NodeHandleT v) noexcept : _child(v) {}
  slot() = default;

  inline slot & operator=(NodeHandleT child) noexcept
  {
    _child = child;
    return *this;
  }

  inline slot & operator=(const PayloadT payload) noexcept
  {
    _payload = payload;
    return *this;
  }
  //inline bool operator==(const PayloadT & rhs) const { return _payload == rhs; }
  //inline bool operator==(const NodeHandleT & rhs) const { return _child == rhs; }

  inline bool eq_children(const NodeHandleT & rhs) const { return _child == rhs; }
  inline bool eq_payloads(const PayloadT & rhs) const { return _payload == rhs; }
};

template <typename Config>
struct node : utils::noncopyable
{
  using config           = Config;
  using key_t            = typename config::key_t;
  using payload_t        = typename config::payload_t;
  using key_handle_t     = typename config::key_handle_t;
  using payload_handle_t = typename config::payload_handle_t;
  using node_handle_t    = typename config::node_handle_t;
  using index_t          = typename config::index_t;
  using slot_t           = slot<node_handle_t, payload_t>;

  static constexpr size_t order = config::order;

  node_kind _kind;
  index_t   _nKeys;
  key_t     _keys[order - 1];
  slot_t    _slots[order];

  node(const node_kind kind) noexcept : _kind{kind}, _nKeys{0} {}

  bool has_links() const noexcept { return _kind == node_kind::Root || _kind == node_kind::Internal; }
  bool is_root() const noexcept { return _kind == node_kind::Root || _kind == node_kind::RootLeaf; };
  bool safe_for_insert() const noexcept { return _nKeys != config::node_max_keys; };
  bool safe_for_remove() const noexcept { return has_enough_slots_with(_nKeys - 1); }
  bool has_enough_slots() const noexcept { return has_enough_slots_with(_nKeys); }

  node_handle_t next_sibling() const noexcept
  {
    CKVS_ASSERT(!has_links() && !is_root());
    return _slots[config::node_max_keys]._child;
  }

  bool has_enough_slots_with(const index_t nKeys) const noexcept
  {
    CKVS_ASSERT(nKeys <= config::node_max_keys);
    return nKeys >= config::middle_idx + !has_links() || is_root() && nKeys > 0;
  }

  bool can_coalesce_with(const node & sibling) const noexcept
  {
    CKVS_ASSERT(sibling._kind == _kind);
    return sibling._nKeys + _nKeys <= config::node_max_keys - has_links();
  }

  template <typename KeyConverable, typename PayloadConvertable>
  void distribute_payload(node &                   greater_dst,
                          const index_t            idx,
                          const KeyConverable      new_key,
                          const PayloadConvertable new_value) noexcept
  {
    CKVS_ASSERT(!has_links() && !greater_dst.has_links());

    _nKeys = greater_dst._nKeys = order / 2;

    const index_t split_start_idx = config::middle_idx + 1;

    // If the new KV-pair goes to greater_dst, copy the greater half while reserving KV-slot for it.
    if(idx > config::middle_idx)
    {
      std::copy(_keys + split_start_idx, _keys + idx, greater_dst._keys);
      greater_dst._keys[idx - split_start_idx] = new_key;

      std::copy(_keys + idx, _keys + config::node_max_keys, greater_dst._keys + 1 + (idx - split_start_idx));

      // todo: use trait obj which will default to copy/copy_backwards and do a bulkcopy if some static
      // payload_t::bulkcopy(src_start_idx, src_end_idx, dst_idx, tree_ptr)
      size_t dst_idx = 0;
      size_t src_idx = split_start_idx;
      while(src_idx != idx)
        greater_dst._slots[dst_idx++]._payload = _slots[src_idx++]._payload;

      greater_dst._slots[idx - split_start_idx]._payload = new_value;

      src_idx = idx;
      dst_idx = 1 + (idx - split_start_idx);
      while(src_idx != config::node_max_keys + 1)
        greater_dst._slots[dst_idx++]._payload = _slots[src_idx++]._payload;
    }
    // Otherwise, just copy the greater half + the biggest KV from our half, since we must remove it to
    // create a place for the new KV.
    else
    {
      std::copy(_keys + config::middle_idx, _keys + config::node_max_keys, greater_dst._keys);

      size_t dst_idx = 0;
      size_t src_idx = config::middle_idx;
      while(src_idx != config::node_max_keys + 1)
        greater_dst._slots[dst_idx++]._payload = _slots[src_idx++]._payload;

      if(idx == config::middle_idx)
      {
        _keys[idx]           = new_key;
        _slots[idx]._payload = new_value;
      }
      else
      {
        --_nKeys;
        insert_hinted<false>(new_key, new_value, idx, idx);
      }
    }
  }

  std::tuple<key_handle_t, bool> distribute_children(node &              greater_dst,
                                                     const index_t       insert_idx,
                                                     const key_handle_t  new_key,
                                                     const node_handle_t new_right_child) noexcept
  {
    CKVS_ASSERT(has_links());
    CKVS_ASSERT(_nKeys == config::node_max_keys);
    CKVS_ASSERT(greater_dst.has_links());

    // Whether new_key goes to this node or dst determines which indices we should shift
    const bool insert_here = insert_idx < config::middle_idx;


    // Since for x keys we have x+1 slots, we can't distribute evenly
    _nKeys             = config::middle_idx;
    greater_dst._nKeys = order - _nKeys - 1;

    // We must copy the greater half of all KVs to the new greater_dst node. Since we out of space to store the
    // new KV-pair, we use imaginary node of order + 1 which could hold all our KVs as well as the new KV-pair.
    // To iterate its greater half, we map its indices to ours/dst and substitute the new KV accordingly.
    const index_t split_start_idx = config::middle_idx + 1;
    for(index_t img_node_idx = split_start_idx; img_node_idx <= order; ++img_node_idx)
    {
      const index_t dst_idx = img_node_idx - split_start_idx;

      const bool    shift_slots          = !insert_here && img_node_idx > insert_idx;
      const bool    use_new_child        = !insert_here && img_node_idx == insert_idx + 1;
      const index_t node_val_idx         = img_node_idx - shift_slots - insert_here;
      greater_dst._slots[dst_idx]._child = use_new_child ? new_right_child : _slots[node_val_idx]._child;

      if(img_node_idx == order)
        continue;

      const bool    shift_key_indices = !insert_here && img_node_idx >= insert_idx;
      const bool    use_new_key       = !insert_here && img_node_idx == insert_idx;
      const index_t node_key_idx      = img_node_idx - shift_key_indices - insert_here;
      greater_dst._keys[dst_idx]      = use_new_key ? new_key : key_handle_t{this, node_key_idx};
    }

    // Since we copied our last KV pair to dst to be able to fit the new KV pair, we need to remove it from this, but we'll defer removal until the sparse_key is inserted in our parent, otherwise it'll dangle
    if(insert_here)
      insert_hinted<true>(new_key, new_right_child, insert_idx, insert_idx + 1);
    const bool         use_existing_sparse_key = config::middle_idx != insert_idx;
    const key_handle_t sparse_key_handle = use_existing_sparse_key ? key_handle_t{this, config::middle_idx} : new_key;
    const bool         remove_sparse_key_later = insert_here && use_existing_sparse_key;

    return {sparse_key_handle, remove_sparse_key_later};
  }

  template <typename KeyComparable>
  index_t find_key_index(const KeyComparable key_comparable) const noexcept
  {
    return static_cast<index_t>(std::upper_bound(_keys, _keys + _nKeys, key_comparable) - _keys);
  }

  template <bool Links, typename KeyAssignable, typename ValueAssignable>
  void insert_detail(const KeyAssignable   key,
                     const ValueAssignable slot_value,
                     const index_t         key_idx,
                     const index_t         value_idx) noexcept
  {
    CKVS_ASSERT(Links == has_links());
    CKVS_ASSERT(Links ? _nKeys <= config::node_max_keys : _nKeys < config::node_max_keys);

    std::copy_backward(_keys + key_idx, _keys + _nKeys, _keys + _nKeys + 1);
    _keys[key_idx] = key;

    size_t dst_idx = _nKeys + 1 + Links;
    size_t src_idx = _nKeys + Links;
    if constexpr(Links)
      while(src_idx != value_idx)
        _slots[--dst_idx]._child = _slots[--src_idx]._child;
    else
      while(src_idx != value_idx)
        _slots[--dst_idx]._payload = _slots[--src_idx]._payload;

    if constexpr(Links)
      _slots[value_idx]._child = slot_value;
    else
      _slots[value_idx]._payload = slot_value;
    ++_nKeys;
  }
  template <bool Links, typename KeyAssignable, typename ValueAssignable>
  void insert_hinted(const KeyAssignable   key,
                     const ValueAssignable slot_value,
                     const index_t         key_idx_hint,
                     const index_t         value_idx_hint) noexcept
  {
    insert_detail<Links>(key, slot_value, key_idx_hint, value_idx_hint);
  }

  template <bool Links, typename KeyAssignable, typename ValueAssignable>
  void insert(const KeyAssignable key, const ValueAssignable & slot_value) noexcept
  {
    const index_t key_idx = find_key_index(key);
    insert_detail<Links>(key, slot_value, key_idx, key_idx + 1);
  }

  template <bool Links, typename KeyAssignable, typename ValueAssignable>
  void remove(const KeyAssignable key, const ValueAssignable & slot_value, index_t key_idx_hint) noexcept
  {
    CKVS_ASSERT(Links == has_links());
    CKVS_ASSERT(key_idx_hint - Links < _nKeys);

    if(_keys[key_idx_hint] != key)
      key_idx_hint = find_key_index(key);

    std::copy(_keys + key_idx_hint + 1, _keys + _nKeys, _keys + key_idx_hint);

    bool hint_correct;
    if constexpr(Links)
      hint_correct = _slots[key_idx_hint].eq_children(slot_value);
    else
      hint_correct = _slots[key_idx_hint].eq_payloads(slot_value);
    const index_t slot_idx = hint_correct ? key_idx_hint : key_idx_hint + 1;
    if constexpr(Links)
    {
      CKVS_ASSERT(_slots[slot_idx]._child == slot_value);
    }
    else
    {
      CKVS_ASSERT(_slots[key_idx_hint]._payload == slot_value);
    }

    size_t dst_idx = slot_idx;
    size_t src_idx = slot_idx + 1;

    if constexpr(Links)
      while(src_idx != _nKeys + 1)
        _slots[dst_idx++]._child = _slots[src_idx++]._child;
    else
      while(src_idx != _nKeys + 1)
        _slots[dst_idx++]._payload = _slots[src_idx++]._payload;

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

  template <bool Links>
  key_handle_t steal_smallest(node & greater_node) noexcept
  {
    CKVS_ASSERT(Links == has_links());
    const index_t slot_idx = 0;
    const index_t key_idx  = 0;

    const slot_t       slot_to_steal = greater_node._slots[slot_idx];
    const key_handle_t key_to_steal{&greater_node, key_idx};
    const index_t      value_idx_hint = _nKeys + Links;
    if constexpr(Links)
      insert_hinted<Links>(key_to_steal, slot_to_steal._child, _nKeys, value_idx_hint);
    else
      insert_hinted<Links>(key_to_steal, slot_to_steal._payload, _nKeys, value_idx_hint);

    if constexpr(Links)
    {
      greater_node.remove<Links>(key_to_steal, slot_to_steal._child, key_idx);
      return {this, static_cast<index_t>(value_idx_hint - 1)};
    }
    else
    {
      greater_node.remove<Links>(key_to_steal, slot_to_steal._payload, key_idx);
      return {&greater_node, 0};
    }
  }

  template <bool Links>
  key_handle_t steal_greatest(node & lesser_node) noexcept
  {
    CKVS_ASSERT(Links == has_links());
    const index_t slot_idx = lesser_node._nKeys - !Links;
    const index_t key_idx  = lesser_node._nKeys - 1;

    const slot_t       slot_to_steal = lesser_node._slots[slot_idx];
    const key_handle_t key_to_steal{&lesser_node, key_idx};
    if constexpr(Links)
      insert_hinted<Links>(key_to_steal, slot_to_steal._child, 0, 0);
    else
      insert_hinted<Links>(key_to_steal, slot_to_steal._payload, 0, 0);

    if constexpr(Links)
      lesser_node.remove<Links>(key_to_steal, slot_to_steal._child, key_idx);
    else
      lesser_node.remove<Links>(key_to_steal, slot_to_steal._payload, key_idx);

    return {this, 0};
  }
};

enum class traverse_policy { search, insert, insert_optimistic, remove, remove_optimistic };

template <traverse_policy Policy>
struct traversal_policy_traits
{
};

template <>
struct traversal_policy_traits<traverse_policy::search>
{
  constexpr static inline bool lock_non_leaf_exclusive = false;
  constexpr static inline bool lock_leaf_exclusive     = false;
  template <typename Config>
  static inline bool safe_to_unlock(const node<Config> &) noexcept
  {
    return true;
  }
  constexpr static inline traverse_policy policy_if_unsafe = traverse_policy::search;
};

template <>
struct traversal_policy_traits<traverse_policy::insert>
{
  constexpr static inline bool lock_non_leaf_exclusive = true;
  constexpr static inline bool lock_leaf_exclusive     = true;
  template <typename Config>
  static inline bool safe_to_unlock(const node<Config> & node) noexcept
  {
    return node.safe_for_insert();
  }
  constexpr static inline traverse_policy policy_if_unsafe = traverse_policy::insert;
};

// Optimistic policies are currently disabled, since they're highly unstable(crashes!)
template <>
struct traversal_policy_traits<traverse_policy::insert_optimistic>
{
  constexpr static inline bool lock_non_leaf_exclusive = false;
  constexpr static inline bool lock_leaf_exclusive     = true;
  template <typename Config>
  static inline bool safe_to_unlock(const node<Config> & node) noexcept
  {
    return node.safe_for_insert();
  }
  constexpr static inline traverse_policy policy_if_unsafe = traverse_policy::insert;
};

template <>
struct traversal_policy_traits<traverse_policy::remove>
{
  constexpr static inline bool lock_non_leaf_exclusive = true;
  constexpr static inline bool lock_leaf_exclusive     = true;
  template <typename Config>
  static inline bool safe_to_unlock(const node<Config> & node) noexcept
  {
    return node.safe_for_remove();
  }
  constexpr static inline traverse_policy policy_if_unsafe = traverse_policy::remove;
};

template <>
struct traversal_policy_traits<traverse_policy::remove_optimistic>
{
  constexpr static inline bool lock_non_leaf_exclusive = false;
  constexpr static inline bool lock_leaf_exclusive     = true;
  template <typename Config>
  static inline bool safe_to_unlock(const node<Config> & node) noexcept
  {
    return node.safe_for_remove();
  }
  constexpr static inline traverse_policy policy_if_unsafe = traverse_policy::remove;
};

template <typename T>
struct default_node_handle
{
  T * _ptr;

  bool operator==(const default_node_handle & rhs) const noexcept { return _ptr == rhs._ptr; }
  bool operator!=(const default_node_handle & rhs) const noexcept { return _ptr != rhs._ptr; }

  static inline default_node_handle invalid() noexcept { return default_node_handle{nullptr}; }
};

template <typename NodeT, typename NodeHandleT, typename LockT>
struct locked_node : utils::noncopyable
{
  using lock_t = LockT;

  NodeT *     _node   = nullptr;
  NodeHandleT _handle = typename NodeHandleT::invalid();
  LockT       _lock;
  locked_node() noexcept {}

  locked_node(NodeT * node, NodeHandleT handle, LockT && lock) noexcept
    : _node{node}, _handle{std::move(handle)}, _lock(std::move(lock))
  {
  }
  locked_node(locked_node &&) = default;
  locked_node & operator=(locked_node &&) = default;
};

template <typename RlockedNodeT, typename WlockedNodeT>
using maybe_locked_node_t = std::variant<std::monostate, RlockedNodeT, WlockedNodeT>;

template <typename RlockedNodeT, typename WlockedNodeT, size_t N>
using parents_chain_t = std::array<maybe_locked_node_t<RlockedNodeT, WlockedNodeT>, N>;

template <typename RlockedNodeT, typename WlockedNodeT, size_t N>
struct root_to_leaf_path : utils::noncopyable
{
  parents_chain_t<RlockedNodeT, WlockedNodeT, N> _parents;
  size_t                                         _height;
};

template <typename RlockedNodeT, typename WlockedNodeT, size_t N>
struct parents_trace : utils::noncopyable
{
  parents_chain_t<RlockedNodeT, WlockedNodeT, N> * _parents;
  size_t                                           _parent_index;

  parents_trace(parents_chain_t<RlockedNodeT, WlockedNodeT, N> & parents, const size_t parent_index) noexcept
    : _parents{&parents}, _parent_index{parent_index}
  {
  }

  parents_trace next_parent(const bool allow_invalid) noexcept
  {
    CKVS_ASSERT(_parent_index != 0 || allow_invalid);
    return {*_parents, _parent_index - 1};
  }
  static parents_trace trace_from_path(root_to_leaf_path<RlockedNodeT, WlockedNodeT, N> & path) noexcept
  {
    return {path._parents, path._height - 1};
  }
  maybe_locked_node_t<RlockedNodeT, WlockedNodeT> & current_parent() noexcept
  {
    CKVS_ASSERT(_parent_index < N);
    return (*_parents)[_parent_index];
  }
};

template <bool IsKey, typename Config>
struct default_value_handle
{
  // Use non-optimized version to test big changes made in bptree's core logic
  //#define OPTIMIZED

  using value_t = std::conditional_t<IsKey, typename Config::key_t, typename Config::payload_t>;
  using index_t = typename Config::index_t;
  using node_t  = typename Config::node_t;
#if defined(OPTIMIZED)
  value_t _value;
#else
  node_t * _node;
  index_t  _index;
#endif

  default_value_handle()                             = default;
  default_value_handle(const default_value_handle &) = default;

  default_value_handle(node_t * node, const index_t idx)
  {
#if defined(OPTIMIZED)
    if constexpr(IsKey)
      _value = node->_keys[idx];
    else
      _value = node->_slots[idx];
#else
    _node  = node;
    _index = idx;
#endif
  }

  default_value_handle & operator=(const value_t & rhs)
  {
    _value = rhs;
    return *this;
  }
  default_value_handle & operator=(const default_value_handle & rhs) = default;

  // Usually you'd need to provide operator= overload in your key/payload class instead of this
  operator value_t() const
  {
#if defined(OPTIMIZED)
    return _value;
#else
    if constexpr(IsKey)
      return _node->_keys[_index];
    else
      return _node->_slots[_index];
#endif
  }

  friend bool operator<(const value_t lhs, const default_value_handle rhs) { return lhs < rhs._value; }
};

template <typename Config>
using default_key_handle = default_value_handle<true, Config>;
template <typename Config>
using default_payload_handle = default_value_handle<false, Config>;


template <typename Config>
struct default_bptree_extentions
{
  using node_t        = typename Config::node_t;
  using node_handle_t = typename default_node_handle<node_t>;

  using r_lock_t        = std::shared_lock<utils::shared_null_mutex>;
  using w_lock_t        = std::unique_lock<utils::shared_null_mutex>;
  using r_locked_node_t = locked_node<node_t, node_handle_t, r_lock_t>;
  using w_locked_node_t = locked_node<node_t, node_handle_t, w_lock_t>;

  node_handle_t _root;

  void update_root(const node_handle_t new_root) noexcept { _root = new_root; }

  template <bool ExclusivelyLocked,
            typename LockedNodeT = std::conditional_t<ExclusivelyLocked, w_locked_node_t, r_locked_node_t>>
  LockedNodeT get_node(const node_handle_t handle) noexcept
  {
    LockedNodeT result;
    result._node   = handle._ptr;
    result._handle = node_handle_t{handle._ptr};
    result._lock   = typename LockedNodeT::lock_t{};
    return result;
  }

  inline w_locked_node_t upgrade_to_node_exclusive(const node_handle_t handle, r_locked_node_t & locked_node) noexcept
  {
    std::shared_lock<utils::shared_null_mutex> shared;
    return {std::get<node_t &>(locked_node), handle, w_lock_t{}};
  }

  inline void delete_node(w_locked_node_t & locked_node) noexcept { delete locked_node._handle._ptr; }

  inline void delete_node(const node_handle_t n) noexcept { delete n._ptr; }

  template <typename... NodeCtorParams>
  inline w_locked_node_t new_node(NodeCtorParams &&... params) noexcept
  {
    static_assert(noexcept(node_t(std::forward<NodeCtorParams>(params)...)));
    w_locked_node_t result;
    result._node   = new node_t{std::forward<NodeCtorParams>(params)...}; // Will terminate() on bad_alloc
    result._handle = node_handle_t{result._node};
    return result;
  }
  node_handle_t get_root() noexcept
  {
    if(_root == node_handle_t::invalid())
      _root = new_node(detail::node_kind::RootLeaf)._handle;
    return _root;
  }

  inline void mark_dirty(w_locked_node_t &) noexcept {}
};

}}