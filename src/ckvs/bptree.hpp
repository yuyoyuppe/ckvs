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


//#define VERBOSE_INSPECT
//#define LOGGING
inline const char * dbg_kind(ckvs::detail::node_kind nk)
{
  char * types[] = {"Removed", "Internal", "Root", "Leaf", "RootLeaf"};
  return types[static_cast<size_t>(nk)];
}

template <typename T>
inline const char * dbg_node(T & _locked)
{
  static char contents_a[2][4096]{};
  static bool meh      = false;
  char *      contents = contents_a[meh];
  meh                  = !meh;
  auto & node          = *_locked._node;
  char * ptr           = contents + sprintf(contents, "#%u %s [ ", _locked._handle._id, dbg_kind(node._kind));
  for(size_t i = 0; i < node._nKeys; ++i)
    ptr += sprintf(ptr, "%zu ", node._keys[i]);
  sprintf(ptr, "]");
  return contents;
}

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

  using key_t         = KeyT;
  using payload_t     = PayloadT;
  using node_t        = detail::node<bp_tree_config>;
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
  using config = Config;

  using index_t       = typename config::index_t;
  using key_t         = typename config::key_t;
  using payload_t     = typename config::payload_t;
  using node_t        = typename config::node_t;
  using node_handle_t = typename node_t::node_handle_t;
  using slot_t        = typename node_t::slot_t;

  using extensions = Extensions<Config>;
  using r_lock_t   = typename extensions::r_lock_t;
  using w_lock_t   = typename extensions::w_lock_t;

  using r_locked_node_t = typename extensions::template select_locked_node_t<false>;
  using w_locked_node_t = typename extensions::template select_locked_node_t<true>;

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
            typename TraversePolicyTraits                = detail::traversal_policy_traits<TraversePolicy>,
            detail::traverse_policy BackupPolicy         = TraversePolicyTraits::policy_if_unsafe,
            bool                    LockLeafExclusive    = TraversePolicyTraits::lock_leaf_exclusive,
            bool                    LockNonLeafExclusive = TraversePolicyTraits::lock_non_leaf_exclusive,
            typename LeafLockT                           = std::conditional_t<LockLeafExclusive, w_lock_t, r_lock_t>>
  auto traverse(const key_t key) noexcept
  {
    static_assert(!(!LockLeafExclusive && LockNonLeafExclusive), "weird policy, are you sure that's what you want?");
  start:
    using non_leaf_locked_node_t     = select_locked_node_t<LockNonLeafExclusive>;
    using leaf_locked_node_t         = select_locked_node_t<LockLeafExclusive>;
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
    traverse_result<detail::locked_node_t<node_t, node_handle_t, LeafLockT>> result;
    for(;;)
    {
      non_leaf_locked_node_t locked_node = get_node<LockNonLeafExclusive>(node_handle);
      node_t &               node        = *locked_node._node;

      const bool removed_node = node._kind == detail::node_kind::Removed;
      if((!path_depth && !node.is_root()) || removed_node)
      {
#if defined(LOGGING)
        if(removed_node)
          printf("[%zu] traverse started, but I've got %s node! -> again\n",
                 std::this_thread::get_id(),
                 dbg_node(locked_node));
        else
          printf("[%zu] traverse started, but I have %s instead of root id %u! -> again\n",
                 std::this_thread::get_id(),
                 dbg_node(locked_node),
                 locked_node._handle._id);
#endif
        goto start;
      }
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
#if defined(LOGGING)
            printf(
              "[%zu] traverse: got %s insteaf of root -> again\n", std::this_thread::get_id(), dbg_node(locked_node));
#endif
            goto start;
          }
#if defined(LOGGING)
          static char buffer[4096]{};
          char *      ptr = buffer;
          ptr += sprintf(buffer,
                         "[%zu] traverse: returning %s with parents chain:",
                         std::this_thread::get_id(),
                         dbg_node(locked_node));
          for(size_t i = 0; i < path._height; ++i)
          {
            const bool empty = std::holds_alternative<std::monostate>(path._parents[i]);
            if(empty)
              ptr += sprintf(ptr, " [released] ->");
            else
              ptr += sprintf(ptr, " %s ->", dbg_node(std::get<non_leaf_locked_node_t>(path._parents[i])));
          }
          ptr += sprintf(ptr, "\n");
          if(ptr - buffer < 4096)
            printf(buffer);
          else
            printf("buffer too small\n");
#endif
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
                        const key_t         key,
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
      parent.insert(key, slot_t{new_neighbor_handle});
      return;
    }

    // We need to split the parent
    auto locked_new_parent = new_node(detail::node_kind::Internal);

    const key_t sparse_key =
      parent.distribute_children(*locked_new_parent._node, parent.find_key_index(key), key, new_neighbor_handle);

    insert_in_parent(
      parent, locked_parent._handle, trace.next_parent(parent.is_root()), sparse_key, locked_new_parent._handle);
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

  void remove_entry(node_t &          node,
                    w_locked_node_t & locked_node,
                    node_handle_t     locked_node_handle,
                    parents_trace_t   trace,
                    const key_t       key,
                    const slot_t &    value,
                    const index_t     key_idx_hint) noexcept
  {
#if defined(LOGGING)
    printf("[%zu] remove_entry: rem %u from %s\n", std::this_thread::get_id(), key, dbg_node(locked_node));
#endif
    node.remove(key, value, key_idx_hint);

    mark_dirty(locked_node);
    if(node.has_enough_slots() || (node.is_root() && !node.has_links()))
    {
#if defined(LOGGING)
      printf("[%zu] remove_entry: %s safe after rem -> unlocking\n", std::this_thread::get_id(), dbg_node(locked_node));
#endif
      return;
    }

    const bool node_has_links = node.has_links();
#if defined(LOGGING)
    printf("[%zu] remove_entry: %s is unsafe, locking parent...\n", std::this_thread::get_id(), dbg_node(locked_node));
#endif
    auto     locked_parent = std::move(std::get<w_locked_node_t>(trace.current_parent()));
    node_t & parent        = *locked_parent._node;
    CKVS_ASSERT(parent.has_links());
#if defined(LOGGING)
    printf("[%zu] remove_entry: got %s's parent:%s, now locking neighbor...\n",
           std::this_thread::get_id(),
           dbg_node(locked_node),
           dbg_node(locked_parent));
#endif
    auto [neighbor_handle, parent_idx] = get_neighbor(locked_node_handle, parent);
    CKVS_ASSERT(neighbor_handle != node_handle_t::invalid());
    auto     locked_neighbor = get_node<true>(neighbor_handle);
    node_t & neighbor        = *locked_neighbor._node;
#if defined(LOGGING)
    printf("[%zu] remove_entry: got %s's neighbor:%s\n",
           std::this_thread::get_id(),
           dbg_node(locked_node),
           dbg_node(locked_neighbor));
#endif
    mark_dirty(locked_neighbor);

    CKVS_ASSERT(neighbor._kind == node._kind);
    const bool node_is_leftmost = parent._slots[0]._child == locked_node_handle;

    // parent_idx must be the key between node and neighbor
    parent_idx -= node_is_leftmost;
    const key_t parent_key = parent._keys[parent_idx];

    if(node.can_coalesce_with(neighbor))
    {
#if defined(LOGGING)
      printf("[%zu] remove_entry: merging %s + %s\n",
             std::this_thread::get_id(),
             dbg_node(locked_node),
             dbg_node(locked_neighbor));
#endif
      // Always drain from a greater node

      node_t * node_p     = &node;
      node_t * neighbor_p = &neighbor;
      if(node_is_leftmost)
      {
        std::swap(node_p, neighbor_p);
        std::swap(locked_node_handle, neighbor_handle);
        std::swap(locked_node._handle, locked_neighbor._handle);
      }

      if(node_has_links)
        neighbor_p->_keys[neighbor_p->_nKeys++] = parent_key;
      neighbor_p->drain_from(node_p);
      // Only one child will be left in root => it becomes the new root
      if(parent.is_root() && parent.has_links() && parent._nKeys == 1)
      {
        mark_dirty(locked_parent);
        auto parent_slot = slot_t{locked_node_handle};
        parent.remove(parent_key, parent_slot, parent_idx);
        neighbor_p->_kind = node.has_links() ? detail::node_kind::Root : detail::node_kind::RootLeaf;
#if defined(LOGGING)
        printf("[%zu] remove_entry: updating root to %s\n", std::this_thread::get_id(), dbg_node(locked_neighbor));
#endif
        parent._kind = detail::node_kind::Removed;
        delete_node(locked_parent);
        update_root(neighbor_handle);
      }
      else
      {
#if defined(LOGGING)
        printf("[%zu] remove_entry: recursively remove %u from %s\n",
               std::this_thread::get_id(),
               parent_key,
               dbg_node(locked_parent));
#endif
        remove_entry(parent,
                     locked_parent,
                     locked_parent._handle,
                     trace.next_parent(parent.is_root()),
                     parent_key,
                     slot_t{locked_node_handle},
                     parent_idx);
      }
      node_p->_kind = detail::node_kind::Removed;
      delete_node(locked_node);
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
    mark_dirty(locked_parent);
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

  void insert(const key_t key, const payload_t payload) noexcept
  {
#if defined(LOGGING)
    printf("[%zu] begin insert %zu...\n", std::this_thread::get_id(), key);
#endif
    auto traverse_result = traverse<detail::traverse_policy::insert>(key);

    node_t & node = *traverse_result._locked_leaf._node;
#if defined(LOGGING)
    printf("[%zu] got locked %s for inserting %zu...\n",
           std::this_thread::get_id(),
           dbg_node(traverse_result._locked_leaf),
           key);
#endif
    mark_dirty(traverse_result._locked_leaf);

    CKVS_ASSERT(!node.has_links());


    if(node.safe_for_insert())
    {
#if defined(LOGGING)
      printf("[%zu] safe to insert %zu in %s node. unlocking \n",
             std::this_thread::get_id(),
             key,
             dbg_node(traverse_result._locked_leaf));
#endif
      node.insert_hinted(key, slot_t{payload}, traverse_result._slot_idx, traverse_result._slot_idx);
      return;
    }
#if defined(LOGGING)
    printf(
      "[%zu] UNSAFE to insert %zu to %s \n", std::this_thread::get_id(), key, dbg_node(traverse_result._locked_leaf));
#endif
    auto     locked_new_neighbor = new_node(detail::node_kind::Leaf);
    node_t & new_neighbor        = *locked_new_neighbor._node;
    node.distribute_payload(new_neighbor, traverse_result._slot_idx, key, payload);
    mark_dirty(locked_new_neighbor);

    new_neighbor._slots[node_max_keys] = node._slots[node_max_keys];
    node._slots[node_max_keys]         = locked_new_neighbor._handle;
    const key_t sparse_key             = new_neighbor._keys[0];

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
#if defined(LOGGING)
      printf("[%zu] created new root %s to insert %zu and unlocking %s node\n",
             std::this_thread::get_id(),
             dbg_node(locked_new_root),
             key,
             dbg_node(traverse_result._locked_leaf));
#endif
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
#if defined(LOGGING)
    printf("[%zu] remove: begin %u removal\n", std::this_thread::get_id(), key);
#endif
    auto     traverse_result = traverse<detail::traverse_policy::remove>(key);
    node_t & node            = *traverse_result._locked_leaf._node;
#if defined(LOGGING)
    printf("[%zu] remove: got %s\n", std::this_thread::get_id(), dbg_node(traverse_result._locked_leaf));
#endif
    CKVS_ASSERT(!node.has_links());
    if(key != node._keys[traverse_result._slot_idx])
    {
#if defined(LOGGING)
      printf("[%zu] remove: %u isn't there -> unlocking %s\n",
             std::this_thread::get_id(),
             key,
             dbg_node(traverse_result._locked_leaf));
#endif
      return;
    }

    remove_entry(*traverse_result._locked_leaf._node,
                 traverse_result._locked_leaf,
                 traverse_result._leaf_handle,
                 parents_trace_t::trace_from_path(traverse_result._path),
                 key,
                 node._slots[traverse_result._slot_idx],
                 traverse_result._slot_idx);
  }

  std::optional<payload_t> find(const key_t key) noexcept
  {
    auto     traverse_result = traverse<detail::traverse_policy::search>(key);
    node_t & node            = *traverse_result._locked_leaf._node;
    return node._nKeys != traverse_result._slot_idx && key == node._keys[traverse_result._slot_idx] ?
             std::optional<payload_t>{node._slots[traverse_result._slot_idx]._payload} :
             std::nullopt;
  }
};

}