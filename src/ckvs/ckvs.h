#pragma once

#include <inttypes.h>
#include <array>
#include <memory>
#include <optional>
#include <algorithm>
#include <limits>
#include <queue>

#include <assert.h>

#include <iostream>

using key_t = uint32_t;
//using payload_t = std::array<char, 32>;
using payload_t = uint32_t;

enum class NodeKind { Internal, Root, Leaf, RootLeaf };

template <typename NodeT>
union Slot
{
  payload_t * _payload;
  NodeT *     _child; // todo: rename to node?

  inline Slot & operator=(NodeT * child) noexcept
  {
    _child = child;
    return *this;
  }

  inline Slot & operator=(payload_t * payload) noexcept
  {
    _payload = payload;
    return *this;
  }
};

template <uint8_t Order>
class BTree
{
  // todo: this looks like sheet
  static constexpr uint8_t max_keys = Order - 1;
  static constexpr uint8_t middle_idx = Order / 2 - 1;

  static_assert((Order & 1) == 0, "split & merge currently support only even tree arity!");

  template <uint8_t Order = Order>
  struct Node
  {
    Node *     _parent = nullptr;
    NodeKind   _kind = NodeKind::RootLeaf;
    uint8_t    _nKeys = 0;
    key_t      _keys[Order - 1];
    Slot<Node> _slots[Order]; // todo: ensure const size across platform bitness

    ~Node()
    {
      // todo: lol, rework
      if(_kind >= NodeKind::Leaf)
      {
        for(uint_fast8_t i = 0; i < _nKeys; ++i)
          delete _slots[i]._payload;
      }
      else
      {
        for(uint_fast8_t i = 0; i < _nKeys; ++i)
          delete _slots[i]._child;
      }
    }

    uint8_t find_key_idx(const key_t key) const noexcept
    {
      return static_cast<uint8_t>(std::lower_bound(_keys, _keys + _nKeys, key) - _keys);
    }

    template <typename SlotT>
    void insert_detail(const key_t key, SlotT * slot_value, const uint8_t idx) noexcept
    {
      constexpr bool inserting_node = std::is_same_v<SlotT, Node>;
      constexpr bool inserting_payload = std::is_same_v<SlotT, payload_t>;

      static_assert(inserting_payload || inserting_node);
      assert(inserting_payload ? _nKeys < max_keys : _nKeys <= max_keys);
      assert(inserting_payload && _kind >= NodeKind::Leaf || inserting_node && _kind < NodeKind::Leaf);

      if(idx != _nKeys)
      {
        std::copy_backward(_keys + idx, _keys + _nKeys, _keys + _nKeys + 1);
        std::copy_backward(_slots + idx, _slots + _nKeys + 1, _slots + _nKeys + 2);
      }
      _keys[idx] = key;
      _slots[inserting_node ? idx + 1 : idx] = slot_value;

      ++_nKeys;
    }

    template <typename SlotT>
    void insert_hinted(const key_t key, SlotT * slot_value, const uint8_t idx_hint) noexcept
    {
      insert_detail(key, slot_value, idx_hint);
    }

    template <typename SlotT>
    void insert(const key_t key, SlotT * slot_value) noexcept
    {
      insert_detail(key, slot_value, find_key_idx(key));
    }

    void remove(const uint8_t idx) noexcept
    {
      assert(idx <= _nKeys && _kind < NodeKind::Leaf || idx < _nKeys && _kind >= NodeKind::Leaf);

      if(--_nKeys == idx)
        return;

      std::copy(_keys + idx + 1, _keys + _nKeys + 1, _keys + idx);
      std::copy(_slots + idx + 1, _slots + _nKeys + 1, _slots + idx);
    }

    Node<> * next_sibling() const noexcept
    {
      assert(_kind == NodeKind::Leaf);
      return _slots[max_keys]._child;
    }

    void swap(Node<> & rhs) noexcept
    {
      const uint8_t max_idx = std::max(rhs._nKeys, _nKeys);
      for(uint8_t i = 0; i < max_idx; ++i)
      {
        std::swap(_keys[i], rhs._keys[i]);
        std::swap(_slots[i], rhs._slots[i]);
      }
      std::swap(_slots[max_idx], rhs._slots[max_idx]);
      std::swap(_slots[max_keys], rhs._slots[max_keys]);
      std::swap(_nKeys, rhs._nKeys);
      std::swap(_kind, rhs._kind);
      std::swap(_parent, rhs._parent);
    }
  };

  Node<> * _root;

  BTree(const BTree &) = delete;
  BTree & operator=(const BTree &) = delete;

  BTree(BTree &&) = default;
  BTree & operator=(BTree &&) = default;

  std::pair<Node<> *, uint8_t> search_way(const key_t key) const noexcept
  {
    Node<> * node = _root;
    uint8_t  idx;
    for(;;)
    {
      assert(node != nullptr);
      idx = node->find_key_idx(key);
      if(node->_keys[idx] == key)
        ++idx;
      assert(idx <= node->_nKeys);
      if(node->_kind >= NodeKind::Leaf)
        break;
      else
        node = node->_slots[idx]._child;
    }
    assert(node != nullptr);
    return {node, idx};
  }

  // todo: don't use tmp node
  key_t distribute_children(
    Node<> * node, Node<> * new_node, const uint8_t idx, const key_t overflow_key, Node<> * slot_value) const noexcept
  {
#define CAST(a) reinterpret_cast<decltype(tmp._slots[0]._child)>(a)
#define CAST2(a) reinterpret_cast<decltype(node->_slots[0]._child)>(a)

    Node<Order + 1> tmp;
    
    tmp._kind = NodeKind::Internal;
    assert(node->_kind < NodeKind::Leaf);
    assert(node->_nKeys == max_keys);
    for(uint8_t i = 0; i <= max_keys; ++i)
    {
      if(i < max_keys)
        tmp._keys[i] = node->_keys[i];
      tmp._slots[i]._child = CAST(node->_slots[i]._child);
    }
    tmp._nKeys = max_keys;
    tmp.insert_hinted(overflow_key, CAST(slot_value), idx);
    
    const key_t sparse_key = tmp._keys[Order / 2];

    node->_nKeys = 0;
    for(uint8_t i = 0; i <= Order / 2; ++i)
    {
      if(i < Order / 2)
      {
        node->_keys[i] = tmp._keys[i];
        ++node->_nKeys;
      }

      node->_slots[i]._child = CAST2(tmp._slots[i]._child);
      node->_slots[i]._child->_parent = node;
    }

    const uint8_t offset = Order / 2 + 1;
    for(uint8_t i = offset; i <= Order; ++i)
    {
      if(i < max_keys + 1)
      {
        new_node->_keys[i - offset] = tmp._keys[i];
        ++new_node->_nKeys;
      }

      new_node->_slots[i - offset]._child = CAST2(tmp._slots[i]._child);
      new_node->_slots[i - offset]._child->_parent = new_node;
    }

    tmp._nKeys = 0;
    return sparse_key;
  }

  key_t distribute_payload(Node<> *      node,
                           Node<> *      new_node,
                           const uint8_t idx,
                           const key_t   overflow_key,
                           payload_t *   slot_value) const noexcept
  {
    const uint8_t split_start_idx = middle_idx + 1;
    node->_nKeys = new_node->_nKeys = Order / 2;

    key_t sparse_key = {};
    if(idx > middle_idx)
    {
      sparse_key = idx == split_start_idx ? overflow_key : node->_keys[split_start_idx];
      std::copy(node->_keys + split_start_idx, node->_keys + idx, new_node->_keys);
      new_node->_keys[idx - split_start_idx] = overflow_key;
      std::copy(node->_keys + idx, node->_keys + max_keys, new_node->_keys + 1 + (idx - split_start_idx));

      std::copy(node->_slots + split_start_idx, node->_slots + idx, new_node->_slots);
      new_node->_slots[idx - split_start_idx] = slot_value;
      std::copy(node->_slots + idx, node->_slots + max_keys + 1, new_node->_slots + 1 + (idx - split_start_idx));
    }
    else
    {
      sparse_key = node->_keys[middle_idx];
      std::copy(node->_keys + middle_idx, node->_keys + max_keys, new_node->_keys);
      std::copy(node->_slots + middle_idx, node->_slots + max_keys + 1, new_node->_slots);
      if(idx == middle_idx)
      {
        node->_keys[idx]  = overflow_key;
        node->_slots[idx] = slot_value;
      }
      else
      {
        --node->_nKeys;
        node->insert_hinted(overflow_key, slot_value, idx);
      }
    }
    return sparse_key;
  }

  void insert_in_parent(Node<> * node, const key_t key, Node<> * new_node) noexcept
  {
    if(node->_kind == NodeKind::Root || node->_kind == NodeKind::RootLeaf)
    {
      const auto new_root = new Node<>{};
      new_root->_kind     = NodeKind::Root;
      new_root->_keys[0]  = key;
      new_root->_nKeys    = 1;
      new_root->_slots[0] = node;
      new_root->_slots[1] = new_node;

      _root = new_root;

      node->_kind   = node->_kind == NodeKind::Root ? NodeKind::Internal : NodeKind::Leaf;
      node->_parent = new_root;

      new_node->_parent = new_root;
    }
    else
    {
      const auto parent = node->_parent;
      assert(parent != nullptr);
      if(parent->_nKeys != max_keys)
      {
        new_node->_parent = parent;
        parent->insert(key, new_node);
      }
      else
      {
        // we need to split the parent
        const auto idx        = parent->find_key_idx(key);
        const auto new_parent = new Node<>{};
        new_parent->_kind     = NodeKind::Internal;

        const key_t sparse_key = distribute_children(parent, new_parent, idx, key, new_node);
        insert_in_parent(parent, sparse_key, new_parent); // todo: remove recursion
      }
    }
  }

  void delete_entry(Node<> * node, const uint8_t idx) noexcept // todo:rename to remove_entry
  {
    assert(node != nullptr);

    node->remove(idx);
    // only one child left in root => new root should be it
    if(node->_kind == NodeKind::Root && (node->_slots[0]._child == nullptr || node->_slots[1]._child == nullptr))
    {
      _root = node->_slots[0]._child != nullptr? node->_slots[0]._child : node->_slots[1]._child;
      node->_nKeys = 0;
      assert(_root->_kind == NodeKind::Internal); // if it's a leaf, we need to make _root a rootleaf
      _root->_kind = NodeKind::Root;
      _root->_parent = nullptr;
      delete node;
      return;
    }
    const bool node_has_pointers = node->_kind == NodeKind::Root || node->_kind == NodeKind::Internal;
    const bool enough_children = node_has_pointers && node->_nKeys >= Order / 2;
    const bool enough_values = !node_has_pointers && node->_nKeys >= Order / 2 - 1;
    // N has enough values/pointers => nothing to do
    if(enough_children || enough_values) 
      return;
    
    assert(node->_kind == NodeKind::Leaf); // only roots have "next sibling" pointers
    Node<> * parent = node->_parent;
    assert(parent->_kind < NodeKind::Leaf);
    Node<> * sibling = node->_slots[max_keys]._child; // aka N'
    uint8_t key_idx = parent->_nKeys - 1;
    key_t key = parent->_keys[key_idx]; // aka K'
    // todo: this is ineffective way to get the previous child, but happens only when we're deleting from rightmost child
    bool node_is_predecessor = true;
    if(!sibling)
    {
      for(uint8_t i = 1; i <= parent->_nKeys; ++i)
        if(parent->_slots[i]._child == node)
        {
          sibling = parent->_slots[i - 1]._child;
          node_is_predecessor = false;
          key_idx = i - 1;
          key = parent->_keys[key_idx];
          break;
        }
    }
    assert(sibling != nullptr);

    const bool can_coalesce = (node_has_pointers || node->_nKeys != 0) && sibling->_nKeys + node->_nKeys <= max_keys;
    
    if(can_coalesce)
    {
      if(node_is_predecessor)
        node->swap(*sibling);
      // append everything from node to sibling 
      const uint8_t sibling_nkeys = node->_nKeys;
      for(uint8_t i = 0; i < sibling->_nKeys; ++i)
      {
        sibling->_keys[sibling_nkeys + i] = node->_keys[i];
        sibling->_slots[sibling_nkeys + i] = node->_slots[i];
        // we should also append 'key'. no idea how tho!
      }
      if(node->_kind == NodeKind::Leaf) // should we remember node kind before swapping?..
      {
        assert(false);
      }
      const uint8_t key_idx = node->_parent->find_key_idx(key);
      delete_entry(node->_parent, key_idx);
      delete node;
      return;
    }
    
    // can't coalesce => redistribution takes place
    
    //stealing rightmost entry from the left sibling
    if(!node_is_predecessor)
    {
      if(node_has_pointers)
      {
        Node<> * m = sibling->_slots[sibling->_nKeys]._child;
        const auto m_key = sibling->_keys[sibling->_nKeys - 1];
        sibling->remove(sibling->_nKeys);
        sibling->insert_hinted(m_key, m, 0);
        parent->_keys[key_idx] = m_key;
      }
      else
      {
        payload_t * const m = sibling->_slots[sibling->_nKeys - 1]._payload;
        const auto m_key = sibling->_keys[sibling->_nKeys - 1];
        sibling->remove(sibling->_nKeys - 1);
        node->insert_hinted(m_key, m, 0);
        parent->_keys[key_idx] = m_key;
      }
    }
    //stealing leftmost entry from the right sibling
    else
    {
      // symmetric to previous I guess :3
      assert(false);
    }
  }

public:
  void debug_inspect(std::ostream & os) const noexcept
  {
    uint64_t             lvl = 0;
    std::queue<Node<> *> bfs;
    bfs.push(_root);
    uint64_t nodes_left_on_cur_lvl = 1;
    uint64_t nodes_left_on_nxt_lvl = 0;
    assert(_root->_parent == nullptr);
    assert(_root->_kind == NodeKind::Root || _root->_kind == NodeKind::RootLeaf);

    const Node<> * next_sibling = nullptr;
    while(!bfs.empty())
    {
      Node<> * const node = bfs.front();
      bfs.pop();
      os << "\tlevel " << lvl << " (";
      char * types[] = {"Internal", "Root", "Leaf", "RootLeaf"};
      os << types[static_cast<size_t>(node->_kind)] << ")\n";

      for(uint_fast8_t i = 0; i < max_keys; ++i)
      {
        const bool nonempty = i < node->_nKeys;
        os << '|';
        if(nonempty)
          os << node->_keys[i];
        else
          os << '-';
        os << '\t';
      }
      os << '\n';

      if(node->_kind >= NodeKind::Leaf)
      {
        for(uint_fast8_t i = 0; i < Order; ++i)
        {
          const bool nonempty = i < node->_nKeys;
          os << '|';
          if(nonempty)
            os << *node->_slots[i]._payload;
          else
            os << '-';
          os << '\t';
        }
        if(node->_kind == NodeKind::Leaf)
        {
          assert(!nodes_left_on_nxt_lvl || next_sibling == node);
          next_sibling = node->next_sibling();
        }
      }
      else
      {
        for(uint_fast8_t i = 0; i <= node->_nKeys; ++i)
        {
          bfs.push(node->_slots[i]._child);
          assert(node->_slots[i]._child->_parent == node);
        }
        nodes_left_on_nxt_lvl += node->_nKeys + 1;
      }

      if(--nodes_left_on_cur_lvl == 0)
      {
        ++lvl;
        std::swap(nodes_left_on_nxt_lvl, nodes_left_on_cur_lvl);
        next_sibling = nullptr;
      }
      os << '\n';
      os << '\n';
    }
  }

  BTree() { _root = new Node<>(); }
  ~BTree() { delete _root; }

  void insert(const key_t key, payload_t * payload) noexcept
  {
    const auto [node, idx] = search_way(key);
    assert(node != nullptr);
    assert(node->_kind >= NodeKind::Leaf);
    const bool is_full = node->_nKeys == max_keys;
    if(!is_full)
    {
      node->insert_hinted(key, payload, idx);
      return;
    }

    const auto new_node = new Node<>{};
    new_node->_kind     = NodeKind::Leaf;

    distribute_payload(node, new_node, idx, key, payload);

    // set sibling ptrs
    new_node->_slots[max_keys] = node->_slots[max_keys];
    node->_slots[max_keys]     = new_node;

    const key_t sparse_key = new_node->_keys[0];
    insert_in_parent(node, sparse_key, new_node); 
  }

  void remove(const key_t key) noexcept 
  {
    const auto [node, idx] = search_way(key);
    if(node == nullptr)
      return;

    delete_entry(node, idx - 1);
  }

  const payload_t * find(const key_t key) const noexcept
  {
    const auto [node, idx] = search_way(key);
    assert(node != nullptr);

    return key == node->_keys[idx - 1] ? node->_slots[idx - 1]._payload : nullptr;
  }
};
