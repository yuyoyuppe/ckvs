#pragma once

#include <cinttypes>
#include <memory>
#include <queue>
#include <optional>
#include <array>
#include <tuple>

#include "detail/bptree_detail.hpp"
#include "utils/traits.hpp"
#include "utils/math.hpp"

namespace ckvs {

template <typename KeyT,
          typename PayloadT,
          template <typename>
          typename NodeHandleT,
          size_t Order,
          size_t MaxCapacity = std::numeric_limits<uint32_t>::max()>
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

template <typename Config, template <typename...> typename Extensions>
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

  static constexpr size_t  order         = config::order;
  static constexpr index_t node_max_keys = config::node_max_keys;

  static_assert(std::is_trivially_copyable_v<node_t>, "nodes should be serializable!");


  node_handle_t _root = node_handle_t::invalid();


  using parents_trace_t = std::array<node_t *, config::upper_bound_leaf_level>;

  struct root_to_leaf_path
  {
    parents_trace_t _parents;
    size_t          _num_parents;
  };

  struct parents_trace
  {
    const parents_trace_t & _parents;
    size_t                  _parent_index;
  };

  static parents_trace trace_from_path(const root_to_leaf_path & path) noexcept
  {
    return {path._parents, path._num_parents - 1};
  }
  static parents_trace next_parent_from_trace(const parents_trace & trace, const bool allow_invalid) noexcept
  {
    CKVS_ASSERT(trace._parent_index != 0 || allow_invalid);
    return {trace._parents, trace._parent_index - 1};
  }

  std::tuple<r_locked_node_t, index_t, root_to_leaf_path> trace_to_leaf(const key_t key) noexcept
  {
    node_handle_t     node_handle = _root;
    index_t           leaf_idx;
    root_to_leaf_path path{};
    size_t            path_depth = 0;
    for(;;)
    {
      r_locked_node_t locked_node = get_node(node_handle);
      node_t &        node        = std::get<0>(locked_node);
      CKVS_ASSERT(node_handle != node_handle_t::invalid());
      leaf_idx = node.find_key_index(key);
      CKVS_ASSERT(leaf_idx <= node._nKeys);
      if(!node.has_links())
        break;
      else
      {
        // todo: should be optionally locked nodes
        path._parents[path_depth++] = node;
        node_handle                 = node._slots[leaf_idx]._child;
      }
    }
    CKVS_ASSERT(node != node_handle_t::invalid());
    const index_t possible_eq_idx = leaf_idx - 1u > leaf_idx ? 0 : leaf_idx - 1u; // prevent underflow
    if(key == node._keys[possible_eq_idx])
      leaf_idx = possible_eq_idx;

    path._num_parents = path_depth;
    return {node, leaf_idx, path};
  }

  void insert_in_parent(node_t * node, const parents_trace trace, const key_t key, node_t * new_neighbor) noexcept
  {
    if(node->is_root())
    {
      const auto new_root = new_node(detail::node_kind::Root);
      new_root->_keys[0]  = key;
      new_root->_nKeys    = 1;
      new_root->_slots[0] = node;
      new_root->_slots[1] = new_neighbor;

      _root       = new_root;
      node->_kind = node->has_links() ? detail::node_kind::Internal : detail::node_kind::Leaf;
    }
    else
    {
      const auto parent = trace._parents[trace._parent_index];
      CKVS_ASSERT(parent != nullptr);
      if(parent->_nKeys != node_max_keys)
      {
        parent->insert(key, slot_t{new_neighbor});
        return;
      }

      // We need to split the parent
      const auto new_parent = new node_t{detail::node_kind::Internal};
      new_parent->_nKeys    = 0;

      const key_t sparse_key = parent->distribute_children(new_parent, parent->find_key_index(key), key, new_neighbor);
      insert_in_parent(parent, next_parent_from_trace(trace, parent->is_root()), sparse_key, new_parent);
    }
  }

  // Get left neighbor of a node, or right-one if the node is leftmost
  std::tuple<node_t *, index_t> get_neighbor(node_t * node, node_t * parent) const noexcept
  {
    CKVS_ASSERT(node != nullptr);
    CKVS_ASSERT(parent != nullptr);
    CKVS_ASSERT(parent->has_links());
    CKVS_ASSERT(parent->_nKeys > 0);

    if(parent->_slots[0]._child == node)
      return {parent->_slots[1]._child, index_t{1}};

    for(index_t i = 0; i < parent->_nKeys; ++i)
      if(parent->_slots[i + 1]._child == node)
        return {parent->_slots[i]._child, i};

    // If a node has no neighbors, something is fatally wrong
    std::abort();
  }

  void remove_entry(node_t *            node,
                    const parents_trace trace,
                    const key_t         key,
                    const slot_t &      value,
                    const index_t       key_idx_hint) noexcept
  {
    CKVS_ASSERT(node != nullptr);
    node->remove(key, value, key_idx_hint);

    // Only one child left in root => it becomes the new root
    if(node->is_root() && node->has_links() && node->_nKeys == 0u)
    {
      _root        = node->_slots[0]._child == value._child ? node->_slots[1]._child : node->_slots[0]._child;
      _root->_kind = _root->has_links() ? detail::node_kind::Root : detail::node_kind::RootLeaf;
      delete_node(node);
      return;
    }

    if(node->has_enough_slots() || (node->is_root() && !node->has_links()))
      return;

    const bool node_has_links = node->has_links();

    const auto parent = trace._parents[trace._parent_index];
    CKVS_ASSERT(parent->has_links());

    auto [neighbor, parent_idx] = get_neighbor(node, parent);
    CKVS_ASSERT(neighbor->_kind == node->_kind);
    const bool node_is_leftmost = parent->_slots[0]._child == node;

    // parent_idx must be the key between node and neighbor
    parent_idx -= node_is_leftmost;
    const key_t parent_key = parent->_keys[parent_idx];

    CKVS_ASSERT(neighbor != nullptr);

    if(node->can_coalesce_with(*neighbor))
    {
      // Always drain from a greater node
      if(node_is_leftmost)
        std::swap(node, neighbor);

      if(node_has_links)
        neighbor->_keys[neighbor->_nKeys++] = parent_key;
      neighbor->drain_from(node);

      remove_entry(parent, next_parent_from_trace(trace, parent->is_root()), parent_key, slot_t{node}, parent_idx);
      delete_node(node);
      return;
    }

    // Can't coalesce => steal KV-pair from the neighbor
    const auto new_sparse_key = node_is_leftmost ? node->steal_smallest(neighbor) : node->steal_greatest(neighbor);

    // Use parents' sparse key
    if(node_has_links)
      node->_keys[node_is_leftmost ? node->_nKeys - 1 : 0] = parent_key;

    CKVS_ASSERT(node->_nKeys > 0);

    // And finally update the parent sparse key
    parent->_keys[parent_idx] = new_sparse_key;

    CKVS_ASSERT(neighbor->has_enough_slots());
    CKVS_ASSERT(parent->has_enough_slots());
    CKVS_ASSERT(node->has_enough_slots());
  }

public:
  bp_tree(const bp_tree &) = delete;
  bp_tree & operator=(const bp_tree &) = delete;

  bp_tree(bp_tree && rhs) noexcept
  {
    _root     = std::move(rhs._root);
    rhs._root = node_handle_t::invalid();
  }
  bp_tree & operator=(bp_tree && rhs) noexcept
  {
    delete_node(_root);
    _root     = std::move(rhs._root);
    rhs._root = node_handle_t::invalid();
    return *this;
  }

  ~bp_tree()
  {
    if(_root != node_handle_t::invalid())
      delete_node(_root);
  }

  void inspect(std::ostream & os) noexcept
  {
    uint64_t                  lvl = 0;
    std::queue<node_handle_t> bfs;
    bfs.push(_root);
    node_t & root = get_node_unsafe(_root);

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
    _root = new_node(detail::node_kind::RootLeaf);
  }

  void insert(const key_t key, const payload_t payload) noexcept
  {
    const auto [node, idx, path] = trace_to_leaf(key);
    CKVS_ASSERT(node != nullptr);
    CKVS_ASSERT(!node->has_links());
    if(node->_nKeys != node_max_keys)
    {
      node->insert_hinted(key, slot_t{payload}, idx, idx);
      return;
    }

    const node_handle_t new_neighbor = new_node(detail::node_kind::Leaf);
    new_neighbor->_nKeys             = 0;

    node->distribute_payload(new_neighbor, idx, key, payload);

    new_neighbor->_slots[node_max_keys] = node->_slots[node_max_keys];
    node->_slots[node_max_keys]         = new_neighbor;

    const key_t sparse_key = new_neighbor->_keys[0];

    insert_in_parent(node, trace_from_path(path), sparse_key, new_neighbor);
  }

  void remove(const key_t key) noexcept
  {
    const auto [node, idx, path] = trace_to_leaf(key);
    if(node == nullptr || key != node->_keys[idx])
      return;

    CKVS_ASSERT(!node->has_links());
    remove_entry(node, trace_from_path(path), key, node->_slots[idx], idx);
  }

  std::optional<payload_t> find(const key_t key) noexcept
  {
    auto [locked_node, idx, _] = trace_to_leaf(key);
    node_t & node              = std::get<0>(locked_node);
    return node._nKeys != idx && key == node._keys[idx] ? std::optional<payload_t>{node._slots[idx]._payload} :
                                                          std::nullopt;
  }
};

}