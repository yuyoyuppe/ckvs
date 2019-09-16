#pragma once

#include <cinttypes>
#include <memory>
#include <queue>
#include <optional>
#include <array>
#include <tuple>
#include <variant>

#include "detail/bptree_detail.hpp"
#include "utils/traits.hpp"
#include "utils/math.hpp"
#include "utils/common.hpp"
#include "utils/backoff.hpp"

#define VERBOSE_INSPECT
inline const char * dbg_kind(ckvs::detail::node_kind nk)
{
  char * types[] = {"Removed", "Internal", "Root", "Leaf", "RootLeaf"};
  return types[static_cast<size_t>(nk)];
}

template <typename T>
inline const char * dbg_node(T & _locked)
{
  static char contents_a[2][4096]{};
  static bool ar_switch = false; // 2 dbg_node invokations per 1 printf!
  char *      contents  = contents_a[ar_switch];
  ar_switch             = !ar_switch;
  auto & node           = *_locked._node;
  char * ptr            = contents + sprintf(contents, "%p %s [ ", _locked._handle._ptr, dbg_kind(node._kind));
  for(size_t i = 0; i < node._nKeys; ++i)
    ptr += sprintf(ptr, "%zu ", node._keys[i]);
  sprintf(ptr, "]");
  return contents;
}

namespace ckvs {

template <typename KeyT,
          typename PayloadT,
          size_t Order,
          template <typename> typename KeyHandleT     = detail::default_key_handle,
          template <typename> typename PayloadHandleT = detail::default_payload_handle,
          template <typename> typename NodeHandleT    = detail::default_node_handle,
          size_t MaxCapacity                          = std::numeric_limits<uint32_t>::max()>
struct bp_tree_config
{
  static_assert(Order % 2 == 0, "split & merge currently support only even tree arity!");
  static_assert(Order >= 4, "order cannot be less than 4!");

  static constexpr size_t order = Order;

  using key_t     = KeyT;
  using payload_t = PayloadT;

  using node_t = detail::node<bp_tree_config>;

  using key_handle_t     = KeyHandleT<bp_tree_config>;
  using payload_handle_t = PayloadHandleT<bp_tree_config>;

  using node_handle_t = NodeHandleT<node_t>;
  using index_t       = utils::least_unsigned_t<order, uint8_t, uint16_t, uint32_t, uint64_t>;

  //static_assert(utils::is_serializable_v<node_t>);

  static constexpr index_t node_max_keys = order - index_t{1};
  static constexpr index_t middle_idx    = order / index_t{2} - index_t{1};

  static constexpr size_t tree_max_capacity = MaxCapacity;
  static constexpr size_t upper_bound_leaf_level =
    static_cast<size_t>(utils::ceil(utils::approx_log(node_max_keys, tree_max_capacity)));
};

template <typename Config, template <typename...> typename Extensions = detail::default_bptree_extentions>
class bp_tree : public Extensions<Config>, public utils::noncopyable
{
public:
  using config           = Config;
  using index_t          = typename config::index_t;
  using key_t            = typename config::key_t;
  using payload_t        = typename config::payload_t;
  using key_handle_t     = typename config::key_handle_t;
  using payload_handle_t = typename config::payload_handle_t;
  using node_t           = typename config::node_t;
  using node_handle_t    = typename config::node_handle_t;
  using slot_t           = typename node_t::slot_t;

  using extensions = Extensions<Config>;
  using r_lock_t   = typename extensions::r_lock_t;
  using w_lock_t   = typename extensions::w_lock_t;

  using r_locked_node_t = typename extensions::r_locked_node_t;
  using w_locked_node_t = typename extensions::w_locked_node_t;

  using parents_trace_t = detail::parents_trace<r_locked_node_t, w_locked_node_t, config::upper_bound_leaf_level>;
  using root_to_leaf_path_t =
    detail::root_to_leaf_path<r_locked_node_t, w_locked_node_t, config::upper_bound_leaf_level>;

  static_assert(!std::is_same_v<r_lock_t, w_lock_t>,
                "R/Wlock types should be different, since they're stored inside a tuple");

  static constexpr size_t  order         = config::order;
  static constexpr index_t node_max_keys = config::node_max_keys;

  template <typename LockedNodeT>
  struct traverse_result : utils::noncopyable
  {
    static_assert(std::is_same_v<LockedNodeT, r_locked_node_t> || std::is_same_v<LockedNodeT, w_locked_node_t>);

    LockedNodeT         _locked_leaf;
    node_handle_t       _leaf_handle;
    index_t             _slot_idx;
    root_to_leaf_path_t _path;
  };

  template <detail::traverse_policy TraversePolicy,
            typename KeyComparable,
            typename TraversePolicyTraits                = detail::traversal_policy_traits<TraversePolicy>,
            detail::traverse_policy BackupPolicy         = TraversePolicyTraits::policy_if_unsafe,
            bool                    LockLeafExclusive    = TraversePolicyTraits::lock_leaf_exclusive,
            bool                    LockNonLeafExclusive = TraversePolicyTraits::lock_non_leaf_exclusive,
            typename LeafLockT                           = std::conditional_t<LockLeafExclusive, w_lock_t, r_lock_t>>
  auto traverse(const KeyComparable key) noexcept
  {
    static_assert(!(!LockLeafExclusive && LockNonLeafExclusive), "weird policy, are you sure that's what you want?");
  start:
    using non_leaf_locked_node_t = std::conditional_t<LockNonLeafExclusive, w_locked_node_t, r_locked_node_t>;
    using leaf_locked_node_t     = std::conditional_t<LockLeafExclusive, w_locked_node_t, r_locked_node_t>;

    constexpr bool has_backup_policy = BackupPolicy != TraversePolicy;

    node_handle_t       node_handle = get_root();
    index_t             leaf_idx;
    root_to_leaf_path_t path{};
    size_t              path_depth = 0;

    auto release_traced_locks = [&](const size_t offset = 0) {
      for(size_t i = offset; i < path_depth - offset; ++i)
        path._parents[i].emplace<std::monostate>();
    };

    auto unlock_parents_if_safe = [&](node_t & node) {
      const bool node_is_safe = TraversePolicyTraits::safe_to_unlock(node);
      if(path_depth != 0 && node_is_safe)
      {
        release_traced_locks(1);
      }
      return node_is_safe;
    };
    traverse_result<detail::locked_node<node_t, node_handle_t, LeafLockT>> result;
    for(;;)
    {
      non_leaf_locked_node_t locked_node = get_node<LockNonLeafExclusive>(node_handle);
      node_t &               node        = *locked_node._node;

      const bool removed_node = node._kind == detail::node_kind::Removed;
      if((!path_depth && !node.is_root()) || removed_node)
        goto start;
      unlock_parents_if_safe(node);

      CKVS_ASSERT(node_handle != node_handle_t::invalid());
      leaf_idx = node.find_key_index(key);
      CKVS_ASSERT(leaf_idx <= node._nKeys);


      if(node.has_links())
      {
        path._parents[path_depth++].emplace<non_leaf_locked_node_t>(std::move(locked_node));
        node_handle = node._slots[leaf_idx]._child;
      }
      else
      {
        CKVS_ASSERT(node._nKeys != 0 || node.is_root());
        const index_t possible_eq_idx = leaf_idx - 1u > leaf_idx ? 0 : leaf_idx - 1u; // Prevent index underflow
        if(key == node._keys[possible_eq_idx])
          leaf_idx = possible_eq_idx;
        path._height = path_depth;

        // Leaf node decision: If optimistic insert/deletion failed(assumption that we don't need to change non-leaf
        // nodes isn't true), we must unlock all nodes traversed and perform another traverse using backup policy.
        // Otherwise, update the leaf node's lock as needed.
        if constexpr(LockLeafExclusive && !LockNonLeafExclusive)
        {
          auto maybe_upgraded_node = upgrade_to_node_exclusive(locked_node);
          if(maybe_upgraded_node.has_value() && unlock_parents_if_safe(*maybe_upgraded_node->_node))
          {
            result._leaf_handle = maybe_upgraded_node->_handle;
            result._locked_leaf = std::move(*maybe_upgraded_node);
            result._slot_idx    = leaf_idx;
            result._path        = std::move(path);
            return result;
          }
          else
          {
            release_traced_locks();
            CKVS_ASSERT(has_backup_policy);
            return traverse<BackupPolicy>(key);
          }
        }
        else
        {
          const bool node_is_rootLeaf = node.is_root();
          if(!node_is_rootLeaf && path._height == 0)
          {
            goto start;
          }
          result._leaf_handle = locked_node._handle;
          result._locked_leaf = std::move(locked_node);
          result._slot_idx    = leaf_idx;
          result._path        = std::move(path);
          return result;
        }
      }
    }
    std::abort();
  }

  void insert_in_parent(node_t &            node,
                        node_handle_t       locked_node_handle,
                        parents_trace_t     trace,
                        const key_handle_t  key,
                        const node_handle_t new_neighbor_handle) noexcept
  {
    if(node.is_root())
    {
      auto     locked_root = new_node(detail::node_kind::Root);
      node_t & new_root    = *locked_root._node;
      new_root._keys[0]    = key;
      new_root._nKeys      = 1;
      new_root._slots[0]   = locked_node_handle;
      new_root._slots[1]   = new_neighbor_handle;

      update_root(locked_root._handle);
      CKVS_ASSERT(node.has_links());
      node._kind = detail::node_kind::Internal;
      return;
    }
    auto locked_parent = std::move(std::get<w_locked_node_t>(trace.current_parent()));

    node_t & parent = *locked_parent._node;
    mark_dirty(locked_parent);

    if(parent.safe_for_insert())
    {
      parent.insert<true>(key, new_neighbor_handle);
      return;
    }

    // We need to split the parent
    auto locked_new_parent = new_node(detail::node_kind::Internal);

    const auto [sparse_key_handle, remove_sparse_key_later] =
      parent.distribute_children(*locked_new_parent._node, parent.find_key_index(key), key, new_neighbor_handle);

    insert_in_parent(
      parent, locked_parent._handle, trace.next_parent(parent.is_root()), sparse_key_handle, locked_new_parent._handle);

    if(remove_sparse_key_later)
    {
      const index_t max_key_idx = parent._nKeys - 1;
      parent.remove<true>(parent._keys[max_key_idx], parent._slots[parent._nKeys]._child, max_key_idx);
    }
  }

  // Get left neighbor of a node, or right-one if the node is leftmost
  std::tuple<node_handle_t, index_t> get_neighbor(const node_handle_t node_handle, node_t & parent) const noexcept
  {
    CKVS_ASSERT(parent.has_links());
    CKVS_ASSERT(parent._nKeys > 0);

    if(parent._slots[0]._child == node_handle)
      return {parent._slots[1]._child, index_t{1}};

    for(index_t i = 0; i < parent._nKeys; ++i)
      if(parent._slots[i + 1]._child == node_handle)
        return {parent._slots[i]._child, i};

    // If a node has no neighbors, something is fatally wrong
    std::abort();
  }
  template <bool Links, typename ValueT = std::conditional_t<Links, node_handle_t, payload_t>>
  void remove_entry(w_locked_node_t &  locked_node,
                    parents_trace_t    trace,
                    const key_handle_t key,
                    const ValueT &     value,
                    const index_t      key_idx_hint) noexcept
  {
    node_t & node = *locked_node._node;
    node.remove<Links>(key, value, key_idx_hint);

    mark_dirty(locked_node);
    if(node.has_enough_slots() || (node.is_root() && !node.has_links()))
    {
      return;
    }

    auto     locked_parent = std::move(std::get<w_locked_node_t>(trace.current_parent()));
    node_t & parent        = *locked_parent._node;
    CKVS_ASSERT(parent.has_links());
    auto [neighbor_handle, parent_idx] = get_neighbor(locked_node._handle, parent);
    CKVS_ASSERT(neighbor_handle != node_handle_t::invalid());
    auto     locked_neighbor = get_node<true>(neighbor_handle);
    node_t & neighbor        = *locked_neighbor._node;
    mark_dirty(locked_neighbor);

    CKVS_ASSERT(neighbor._kind == node._kind);
    const bool node_is_leftmost = parent._slots[0]._child == locked_node._handle;

    // parent_idx must be the key between node and neighbor
    parent_idx -= node_is_leftmost;
    const key_handle_t parent_key{&parent, parent_idx};

    if(node.can_coalesce_with(neighbor))
    {
      // Always drain from a greater node

      node_t * node_p     = &node;
      node_t * neighbor_p = &neighbor;
      if(node_is_leftmost)
      {
        std::swap(node_p, neighbor_p);
        std::swap(locked_node._handle, locked_neighbor._handle);
      }

      if constexpr(Links)
        neighbor_p->_keys[neighbor_p->_nKeys++] = parent_key;
      neighbor_p->drain_from(node_p);
      // Only one child will be left in root => it becomes the new root
      if(parent.is_root() && parent.has_links() && parent._nKeys == 1)
      {
        mark_dirty(locked_parent);
        parent.remove<true>(parent_key, locked_node._handle, parent_idx);
        neighbor_p->_kind = node.has_links() ? detail::node_kind::Root : detail::node_kind::RootLeaf;
        parent._kind      = detail::node_kind::Removed;
        delete_node(locked_parent);
        update_root(locked_neighbor._handle);
      }
      else
      {
        remove_entry<true>(
          locked_parent, trace.next_parent(parent.is_root()), parent_key, locked_node._handle, parent_idx);
      }
      node_p->_kind = detail::node_kind::Removed;
      delete_node(locked_node);
      return;
    }

    // Can't coalesce => steal KV-pair from the neighbor
    const key_handle_t new_sparse_key =
      node_is_leftmost ? node.steal_smallest<Links>(neighbor) : node.steal_greatest<Links>(neighbor);

    // Exchange node's sparse_key with parent's key
    if constexpr(Links)
    {
      const index_t key_idx_to_replace = node_is_leftmost ? node._nKeys - 1 : 0;
      // The key we're about to replace is the new_sparse_key, so we must first insert the parent_key
      // in the node, insert new_sparse_key in parent, and only then remove new_sparse_key from the node.
      // Otherwise, new_sparse_key handle will dangle.
      CKVS_ASSERT((new_sparse_key == key_handle_t{&node, key_idx_to_replace}));
      CKVS_ASSERT(node.safe_for_insert());
      node.insert_hinted<true>(
        parent_key, node._slots[key_idx_to_replace]._child, key_idx_to_replace + 1, key_idx_to_replace + 1);
      parent._keys[parent_idx] = new_sparse_key;
      node.remove<true>(node._keys[key_idx_to_replace], node._slots[key_idx_to_replace]._child, key_idx_to_replace);
    }
    else
      parent._keys[parent_idx] = new_sparse_key;

    mark_dirty(locked_parent);

    CKVS_ASSERT(node._nKeys > 0);
    CKVS_ASSERT(neighbor.has_enough_slots());
    CKVS_ASSERT(parent.has_enough_slots());
    CKVS_ASSERT(node.has_enough_slots());
  }

public:
  bp_tree(bp_tree &&) = default;
  bp_tree & operator=(bp_tree &&) = default;

  void inspect(std::ostream & os) noexcept
  {
    (void)os;
    uint64_t                  lvl = 0;
    std::queue<node_handle_t> bfs;
    node_handle_t             root_handle = get_root();
    bfs.push(root_handle);
    auto     locked_root = get_node<false>(root_handle);
    node_t & root        = *locked_root._node;

    uint64_t nodes_left_on_cur_lvl = 1;
    uint64_t nodes_left_on_nxt_lvl = 0;
    CKVS_ASSERT(root.is_root());

    uint64_t non_leaf_count = 0;
    uint64_t leaf_count     = 0;

    node_handle_t next_neighbor = node_handle_t::invalid();

    while(!bfs.empty())
    {
      const auto node_handle = bfs.front();
      auto       locked_node = get_node<false>(node_handle);
      node_t &   node        = *locked_node._node;
      bfs.pop();
#if defined(VERBOSE_INSPECT)
      os << "\tlevel " << lvl << " (";
      char * types[] = {"Removed", "Internal", "Root", "Leaf", "RootLeaf"};
      os << types[static_cast<size_t>(node._kind)] << ") "
         << "\n";
#endif
      if(node.has_links())
        ++non_leaf_count;
      else
        ++leaf_count;
#if defined(VERBOSE_INSPECT)
      for(index_t i = 0; i < node_max_keys; ++i)
      {
        const bool nonempty = i < node._nKeys;
        os << '|';
        if(nonempty)
          os << node._keys[i];
        else
          os << '-';
        os << '\t';
      }
      os << '\n';
#endif
      if(!node.has_links())
      {
#if defined(VERBOSE_INSPECT)
        for(index_t i = 0; i < node._nKeys; ++i)
        {
          os << '|' << node._slots[i]._payload << '\t';
        }
#endif
        if(!node.is_root())
        {
          CKVS_ASSERT(next_neighbor == node_handle || !nodes_left_on_nxt_lvl);
          ++nodes_left_on_nxt_lvl; // correct triggering assert on the line above
          next_neighbor = node.next_sibling();
        }
#if defined(VERBOSE_INSPECT)
        os << '\n';
#endif
      }
      else
      {
        for(index_t i = 0; i <= node._nKeys; ++i)
          bfs.push(node._slots[i]._child);
        nodes_left_on_nxt_lvl += node._nKeys + 1;
      }

      if(--nodes_left_on_cur_lvl == 0)
      {
        ++lvl;
        std::swap(nodes_left_on_nxt_lvl, nodes_left_on_cur_lvl);
        next_neighbor = node_handle_t::invalid();
      }
    }
    const uint64_t total = non_leaf_count + leaf_count;
#if defined(VERBOSE_INSPECT)
    os << "Node stats: " << total << " Total, " << leaf_count << " Leafs("
       << leaf_count / static_cast<double>(total) * 100 << "%), Height = " << lvl << ".\n\n";
#endif
    CKVS_ASSERT(lvl < config::upper_bound_leaf_level);
  }

  template <typename... ExtensionsCtorParams>
  bp_tree(ExtensionsCtorParams &&... ext_params) noexcept
    : extensions{std::forward<ExtensionsCtorParams>(ext_params)...}
  {
  }

  template <typename KeyConverable, typename PayloadConvertable>
  void insert(const KeyConverable key, const PayloadConvertable payload) noexcept
  {
    auto traverse_result = traverse<detail::traverse_policy::insert>(key);

    node_t & node = *traverse_result._locked_leaf._node;
    mark_dirty(traverse_result._locked_leaf);

    CKVS_ASSERT(!node.has_links());


    if(node.safe_for_insert())
    {
      node.insert_hinted<false>(key, payload, traverse_result._slot_idx, traverse_result._slot_idx);
      return;
    }
    auto     locked_new_neighbor = new_node(detail::node_kind::Leaf);
    node_t & new_neighbor        = *locked_new_neighbor._node;
    node.distribute_payload(new_neighbor, traverse_result._slot_idx, key, payload);
    mark_dirty(locked_new_neighbor);

    new_neighbor._slots[node_max_keys]._child = node._slots[node_max_keys]._child;
    node._slots[node_max_keys]                = locked_new_neighbor._handle;
    const key_handle_t sparse_key{&new_neighbor, 0};

    if(node.is_root() && !node.has_links())
    {
      auto     locked_new_root = new_node(detail::node_kind::Root);
      node_t & new_root        = *locked_new_root._node;
      new_root._keys[0]        = sparse_key;
      new_root._nKeys          = 1;
      new_root._slots[0]       = traverse_result._locked_leaf._handle;
      new_root._slots[1]       = locked_new_neighbor._handle;

      node._kind = detail::node_kind::Leaf;
      update_root(locked_new_root._handle);
      return;
    }

    insert_in_parent(node,
                     traverse_result._locked_leaf._handle,
                     parents_trace_t::trace_from_path(traverse_result._path),
                     sparse_key,
                     locked_new_neighbor._handle);
  }

  void remove(const key_t key) noexcept
  {
    auto     traverse_result = traverse<detail::traverse_policy::remove>(key);
    node_t & node            = *traverse_result._locked_leaf._node;
    CKVS_ASSERT(!node.has_links());
    if(key != node._keys[traverse_result._slot_idx])
    {
      return;
    }

    remove_entry<false>(traverse_result._locked_leaf,
                        parents_trace_t::trace_from_path(traverse_result._path),
                        key_handle_t{&node, traverse_result._slot_idx},
                        node._slots[traverse_result._slot_idx]._payload,
                        traverse_result._slot_idx);
  }

  template <typename KeyComparable, typename Visitor>
  void visit(const KeyComparable key, Visitor func) noexcept
  {
    auto       traverse_result = traverse<detail::traverse_policy::search>(key);
    node_t &   node            = *traverse_result._locked_leaf._node;
    const bool found = node._nKeys != traverse_result._slot_idx && key == node._keys[traverse_result._slot_idx];
    func(found ? &node._slots[traverse_result._slot_idx]._payload : nullptr);
  }
};

}
