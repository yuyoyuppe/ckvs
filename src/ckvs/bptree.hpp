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

namespace ckvs {

template <typename KeyT,
          typename PayloadT,
          size_t Order,
          template <typename> typename NodeHandleT = detail::ptr_wrap,
          size_t MaxCapacity                       = std::numeric_limits<uint32_t>::max()>
struct bp_tree_config
{
  static_assert(Order % 2 == 0, "split & merge currently support only even tree arity!");
  static_assert(Order >= 4, "order cannot be less than 4!");

  static constexpr size_t order = Order;

  using key_t     = KeyT;
  using payload_t = PayloadT;
  template <typename T>
  using node_handle_t = NodeHandleT<T>;
  using index_t       = utils::least_unsigned_t<order, uint8_t, uint16_t, uint32_t, uint64_t>;

  static constexpr index_t node_max_keys = order - index_t{1};
  static constexpr index_t middle_idx    = order / index_t{2} - index_t{1};

  static constexpr size_t tree_max_capacity = MaxCapacity;
  static constexpr size_t upper_bound_leaf_level =
    static_cast<size_t>(utils::ceil(utils::approx_log(node_max_keys, tree_max_capacity)));
};

template <typename Config, template <typename...> typename Extensions = detail::default_extentions>
class bp_tree : Extensions<detail::node<Config>>
{
  using config = Config;

  using index_t         = typename config::index_t;
  using key_t           = typename config::key_t;
  using payload_t       = typename config::payload_t;
  using node_t          = detail::node<config>;
  using node_handle_t   = typename node_t::node_handle_t;
  using slot_t          = typename node_t::slot_t;
  using extensions      = Extensions<node_t>;
  using r_locked_node_t = typename extensions::r_locked_node_t;
  using w_locked_node_t = typename extensions::w_locked_node_t;
  using r_lock_t        = std::tuple_element_t<1, r_locked_node_t>;
  using w_lock_t        = std::tuple_element_t<1, w_locked_node_t>;
  static_assert(!std::is_same_v<r_lock_t, w_lock_t>,
                "R/Wlock types should be different, since they're stored inside a tuple");

  static constexpr size_t  order         = config::order;
  static constexpr index_t node_max_keys = config::node_max_keys;

  static_assert(std::is_trivially_copyable_v<node_t>, "nodes should be serializable!");

  // todo: make sure it's threadsafe. also rename to root_handle. also maybe we can
  node_handle_t _root_handle = node_handle_t::invalid();

  template <typename LockedT>
  using locked_node_with_handle_t = std::tuple<LockedT, node_handle_t>;

  using maybe_locked_node_t =
    std::variant<std::monostate, locked_node_with_handle_t<r_locked_node_t>, locked_node_with_handle_t<w_locked_node_t>>;
  using parents_chain_t = std::array<maybe_locked_node_t, config::upper_bound_leaf_level>;

  struct root_to_leaf_path
  {
    parents_chain_t _parents;
    size_t          _num_parents;
  };

  struct parents_trace
  {
    parents_chain_t & _parents;
    size_t            _parent_index;
  };

  template <typename LockedLeafT>
  using traverse_result_t = std::tuple<LockedLeafT, node_handle_t, index_t, root_to_leaf_path>;

  static parents_trace trace_from_path(root_to_leaf_path & path) noexcept
  {
    return {path._parents, path._num_parents - 1};
  }
  static parents_trace next_parent_from_trace(const parents_trace & trace, const bool allow_invalid) noexcept
  {
    CKVS_ASSERT(trace._parent_index != 0 || allow_invalid);
    return {trace._parents, trace._parent_index - 1};
  }

  template <detail::traverse_policy TraversePolicy,
            typename TraversePolicyTraits                = detail::traversal_policy_traits<TraversePolicy>,
            detail::traverse_policy BackupPolicy         = TraversePolicyTraits::policy_if_unsafe,
            bool                    LockLeafExclusive    = TraversePolicyTraits::lock_leaf_exclusive,
            bool                    LockNonLeafExclusive = TraversePolicyTraits::lock_non_leaf_exclusive,
            typename LockedLeafT                         = select_locked_node_t<LockLeafExclusive>>
  traverse_result_t<LockedLeafT> traverse(const key_t key) noexcept
  {
    using non_leaf_locked_node_t      = select_locked_node_t<LockNonLeafExclusive>;
    using some_locked_non_leaf_node_t = std::tuple<non_leaf_locked_node_t, node_handle_t>;
    constexpr bool has_backup_policy  = BackupPolicy != TraversePolicy;

    node_handle_t     node_handle = _root_handle;
    index_t           leaf_idx;
    root_to_leaf_path path{};
    size_t            path_depth = 0;
    for(;;)
    {
      non_leaf_locked_node_t locked_node = get_node<LockNonLeafExclusive>(node_handle);
      node_t &               node        = std::get<node_t &>(locked_node);
      if(path_depth != 0 && TraversePolicyTraits::safe_to_unlock(node))
        path._parents[path_depth - 1].emplace<std::monostate>();

      CKVS_ASSERT(node_handle != node_handle_t::invalid());
      leaf_idx = node.find_key_index(key);
      CKVS_ASSERT(leaf_idx <= node._nKeys);

      if(node.has_links())
      {
        path._parents[path_depth++].emplace<some_locked_non_leaf_node_t>(std::move(locked_node), node_handle);
        node_handle = node._slots[leaf_idx]._child;
      }
      else
      {
        CKVS_ASSERT(node._nKeys != 0 || node.is_root());
        const index_t possible_eq_idx = leaf_idx - 1u > leaf_idx ? 0 : leaf_idx - 1u; // Prevent index underflow
        if(key == node._keys[possible_eq_idx])
          leaf_idx = possible_eq_idx;
        path._num_parents = path_depth;

        // If optimistic insert/deletion failed(assumption that we won't need to change non-leaf nodes isn't true),
        // we must unlock all nodes traversed and perform another traverse using backup policy.
        if constexpr(has_backup_policy)
          if(!TraversePolicyTraits::safe_to_unlock(node))
          {
            // Release all locks
            for(size_t i = 0; i < path_depth; ++i)
              path._parents[i].emplace<std::monostate>();
            auto & lock = std::get<1>(locked_node);
            if(lock.owns_lock())
              lock.unlock();

            return traverse<BackupPolicy>(key);
          }

        if constexpr(LockLeafExclusive && !LockNonLeafExclusive)
          return {upgrade_to_node_exclusive(node_handle, locked_node), node_handle, leaf_idx, std::move(path)};
        else
          return {std::move(locked_node), node_handle, leaf_idx, std::move(path)};
      }
    }
    CKVS_ASSERT(false);
  }

  void insert_in_parent(node_t &            locked_node,
                        node_handle_t       locked_node_handle,
                        parents_trace       trace,
                        const key_t         key,
                        const node_handle_t new_neighbor_handle) noexcept
  {
    if(locked_node.is_root())
    {
      auto [new_root_handle, locked_root] = new_node(detail::node_kind::Root);
      node_t & new_root                   = std::get<node_t &>(locked_root);
      new_root._keys[0]                   = key;
      new_root._nKeys                     = 1;
      new_root._slots[0]                  = locked_node_handle;
      new_root._slots[1]                  = new_neighbor_handle;

      _root_handle = new_root_handle;
      update_root(new_root_handle);
      CKVS_ASSERT(locked_node.has_links());
      locked_node._kind = detail::node_kind::Internal;
      return;
    }

    auto & [locked_parent, locked_parent_handle] =
      std::get<locked_node_with_handle_t<w_locked_node_t>>(trace._parents[trace._parent_index]);
    node_t & parent = std::get<node_t &>(locked_parent);

    if(parent.safe_for_insert())
    {
      parent.insert(key, slot_t{new_neighbor_handle});
      return;
    }

    // We need to split the parent
    auto [new_parent_handle, locked_new_parent] = new_node(detail::node_kind::Internal);
    node_t & new_parent                         = std::get<node_t &>(locked_new_parent);

    const key_t sparse_key =
      parent.distribute_children(new_parent, parent.find_key_index(key), key, new_neighbor_handle);

    insert_in_parent(
      parent, locked_parent_handle, next_parent_from_trace(trace, parent.is_root()), sparse_key, new_parent_handle);
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

  void remove_entry(node_t &            node,
                    node_handle_t       locked_node_handle,
                    const parents_trace trace,
                    const key_t         key,
                    const slot_t &      value,
                    const index_t       key_idx_hint) noexcept
  {
    node.remove(key, value, key_idx_hint);
    // Only one child left in root => it becomes the new root
    if(node.is_root() && node.has_links() && node._nKeys == 0)
    {
      _root_handle = node._slots[0]._child == value._child ? node._slots[1]._child : node._slots[0]._child;
      {
        auto     locked_root = get_node<true>(_root_handle);
        node_t & root        = std::get<node_t &>(locked_root);
        root._kind           = root.has_links() ? detail::node_kind::Root : detail::node_kind::RootLeaf;
      }
      delete_node(locked_node_handle);
      update_root(_root_handle);
      return;
    }

    if(node.has_enough_slots() || (node.is_root() && !node.has_links()))
      return;

    const bool node_has_links = node.has_links();

    auto & [locked_parent, locked_parent_handle] =
      std::get<locked_node_with_handle_t<w_locked_node_t>>(trace._parents[trace._parent_index]);
    node_t & parent = std::get<node_t &>(locked_parent);
    CKVS_ASSERT(parent.has_links());

    auto [neighbor_handle, parent_idx] = get_neighbor(locked_node_handle, parent);
    CKVS_ASSERT(neighbor_handle != node_handle_t::invalid());

    auto     locked_neighbor = get_node<true>(neighbor_handle);
    node_t & neighbor        = std::get<node_t &>(locked_neighbor);

    CKVS_ASSERT(neighbor._kind == node._kind);
    const bool node_is_leftmost = parent._slots[0]._child == locked_node_handle;

    // parent_idx must be the key between node and neighbor
    parent_idx -= node_is_leftmost;
    const key_t parent_key = parent._keys[parent_idx];


    if(node.can_coalesce_with(neighbor))
    {
      // Always drain from a greater node

      node_t * node_p     = &node;
      node_t * neighbor_p = &neighbor;
      if(node_is_leftmost)
      {
        std::swap(node_p, neighbor_p);
        std::swap(locked_node_handle, neighbor_handle);
      }

      if(node_has_links)
        neighbor_p->_keys[neighbor_p->_nKeys++] = parent_key;
      neighbor_p->drain_from(node_p);
      remove_entry(parent,
                   locked_parent_handle,
                   next_parent_from_trace(trace, parent.is_root()),
                   parent_key,
                   slot_t{locked_node_handle},
                   parent_idx);
      delete_node(locked_node_handle);
      return;
    }

    // Can't coalesce => steal KV-pair from the neighbor
    const auto new_sparse_key = node_is_leftmost ? node.steal_smallest(neighbor) : node.steal_greatest(neighbor);

    // Use parents' sparse key
    if(node_has_links)
      node._keys[node_is_leftmost ? node._nKeys - 1 : 0] = parent_key;

    CKVS_ASSERT(node._nKeys > 0);

    // And finally update the parent sparse key
    parent._keys[parent_idx] = new_sparse_key;

    CKVS_ASSERT(neighbor.has_enough_slots());
    CKVS_ASSERT(parent.has_enough_slots());
    CKVS_ASSERT(node.has_enough_slots());
  }

public:
  bp_tree(const bp_tree &) = delete;
  bp_tree & operator=(const bp_tree &) = delete;

  bp_tree(bp_tree && rhs) noexcept
  {
    _root_handle     = std::move(rhs._root_handle);
    rhs._root_handle = node_handle_t::invalid();
  }
  bp_tree & operator=(bp_tree && rhs) noexcept
  {
    delete_node(_root_handle);
    _root_handle     = std::move(rhs._root_handle);
    rhs._root_handle = node_handle_t::invalid();
    return *this;
  }

  ~bp_tree()
  {
    if(_root_handle != node_handle_t::invalid())
      delete_node(_root_handle);
  }

  void inspect(std::ostream & os) noexcept
  {
    uint64_t                  lvl = 0;
    std::queue<node_handle_t> bfs;
    bfs.push(_root_handle);
    node_t & root = get_node_unsafe(_root_handle);

    uint64_t nodes_left_on_cur_lvl = 1;
    uint64_t nodes_left_on_nxt_lvl = 0;
    CKVS_ASSERT(root.is_root());

    uint64_t non_leaf_count = 0;
    uint64_t leaf_count     = 0;

    node_handle_t next_neighbor = node_handle_t::invalid();

    while(!bfs.empty())
    {
      const auto node_handle = bfs.front();
      node_t &   node        = get_node_unsafe(node_handle);
      bfs.pop();
      os << "\tlevel " << lvl << " (";
      char * types[] = {"Internal", "Root", "Leaf", "RootLeaf"};
      os << types[static_cast<size_t>(node._kind)] << ")\n";
      if(node.has_links())
        ++non_leaf_count;
      else
        ++leaf_count;
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

      if(!node.has_links())
      {
        for(index_t i = 0; i < node._nKeys; ++i)
        {
          os << '|' << node._slots[i]._payload << '\t';
        }
        if(!node.is_root())
        {
          CKVS_ASSERT(next_neighbor == node_handle || !nodes_left_on_nxt_lvl);
          ++nodes_left_on_nxt_lvl; // correct triggering assert on the line above
          next_neighbor = node.next_sibling();
        }
        os << '\n';
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
    os << "Node stats: " << total << " Total, " << leaf_count << " Leafs("
       << leaf_count / static_cast<double>(total) * 100 << "%), Height = " << lvl << ".\n\n";
    CKVS_ASSERT(lvl < config::upper_bound_leaf_level);
  }

  template <typename... ExtensionsCtorParams>
  bp_tree(ExtensionsCtorParams &&... ext_params) noexcept
    : extensions{std::forward<ExtensionsCtorParams>(ext_params)...}
  {
    auto [root_handle, _] = new_node(detail::node_kind::RootLeaf);
    _root_handle          = root_handle;
    update_root(root_handle);
  }

  void insert(const key_t key, const payload_t payload) noexcept
  {
    auto [locked_node, node_handle, idx, path] = traverse<detail::traverse_policy::insert_optimistic>(key);
    node_t & node                              = std::get<node_t &>(locked_node);
    CKVS_ASSERT(!node.has_links());

    if(node.safe_for_insert())
    {
      node.insert_hinted(key, slot_t{payload}, idx, idx);
      return;
    }

    auto [new_neighbor_handle, locked_new_neighbor] = new_node(detail::node_kind::Leaf);
    node_t & new_neighbor                           = std::get<node_t &>(locked_new_neighbor);
    node.distribute_payload(std::get<node_t &>(locked_new_neighbor), idx, key, payload);

    new_neighbor._slots[node_max_keys] = node._slots[node_max_keys];
    node._slots[node_max_keys]         = new_neighbor_handle;
    const key_t sparse_key             = new_neighbor._keys[0];

    if(node.is_root() && !node.has_links())
    {
      auto [new_root_handle, locked_root] = new_node(detail::node_kind::Root);
      node_t & new_root                   = std::get<node_t &>(locked_root);
      new_root._keys[0]                   = sparse_key;
      new_root._nKeys                     = 1;
      new_root._slots[0]                  = node_handle;
      new_root._slots[1]                  = new_neighbor_handle;

      _root_handle = new_root_handle;
      update_root(new_root_handle);
      node._kind = detail::node_kind::Leaf;
      return;
    }

    insert_in_parent(node, node_handle, trace_from_path(path), sparse_key, new_neighbor_handle);
  }

  void remove(const key_t key) noexcept
  {
    auto [locked_node, node_handle, idx, path] = traverse<detail::traverse_policy::remove_optimistic>(key);
    node_t & node                              = std::get<node_t &>(locked_node);
    CKVS_ASSERT(!node.has_links());
    if(key != node._keys[idx])
      return;

    remove_entry(node, node_handle, trace_from_path(path), key, node._slots[idx], idx);
  }

  std::optional<payload_t> find(const key_t key) noexcept
  {
    auto [locked_node, _, idx, __] = traverse<detail::traverse_policy::search>(key);
    node_t & node                  = std::get<0>(locked_node);
    return node._nKeys != idx && key == node._keys[idx] ? std::optional<payload_t>{node._slots[idx]._payload} :
                                                          std::nullopt;
  }
};

}