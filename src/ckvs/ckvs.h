#pragma once

#include <inttypes.h>
#include <array>
#include <memory>
#include <optional>
#include <algorithm>
#include <limits>

#include <assert.h>

#include <iostream>

using key_t = uint32_t;
//using value_t = std::array<char, 32>;
using value_t = uint32_t;

constexpr size_t order = 16;


enum class NodeType { Internal, Leaf };

struct Node
{
  virtual ~Node() = default;

  virtual NodeType type() const { return NodeType::Internal; }


  uint8_t               _nKeys = 0;
  key_t                 _keys[order - 1];
  std::unique_ptr<Node> _children[order];
  
  static_assert(order < std::numeric_limits<decltype(_nKeys)>::max());
};

struct LeafNode : public Node
{
  value_t _values[order];

  virtual ~LeafNode() = default;

  NodeType type() const override { return NodeType::Leaf; }
};

class BTree
{
  std::unique_ptr<Node> _root;

  BTree(const BTree &) = delete;
  BTree & operator=(const BTree &) = delete;

  BTree(BTree &&) = default;
  BTree & operator=(BTree &&) = default;

  Node * search(const key_t key) const noexcept
  {
    Node * node = _root.get();
    for(;;)
    {
      assert(node != nullptr);
      const auto first      = node->_keys;
      const auto last       = node->_keys + static_cast<ptrdiff_t>(node->_nKeys);
      const auto target_idx = static_cast<uint8_t>(std::lower_bound(first, last, key) - first);

      if(node->type() == NodeType::Internal)
      {
        assert(target_idx < node->_nKeys);
        node = node->_children[target_idx].get();
      }
      else
      {
        return node;
      }
    }
    assert(false);
    return nullptr;
  }

  void insert_value_to_node(const key_t key, const value_t value, LeafNode * node) noexcept
  {
    assert(node->_nKeys < order - 1);

    const auto       firstK = node->_keys;
    const auto       lastK  = node->_keys + static_cast<ptrdiff_t>(node->_nKeys);
    const auto       whereK = std::lower_bound(firstK, lastK, key);
    const auto idx   = static_cast<uint8_t>(std::distance(firstK, whereK));
    std::cout << "inserting " << key << " to " << (unsigned)idx << " idx\n";
    if(lastK != whereK)
    {
      std::move_backward(whereK, lastK, lastK + 1);
      const auto lastV = node->_values + static_cast<ptrdiff_t>(node->_nKeys);
      const auto whereV = node->_values + static_cast<ptrdiff_t>(idx);
      std::move_backward(whereV, lastV, lastV + 1);
    }
    node->_keys[idx] = std::move(key);
    node->_values[idx] = std::move(value);
    
    ++node->_nKeys;
  }

public:
  BTree() { _root = std::make_unique<LeafNode>(); }

  void insert(const key_t key, const value_t value) noexcept
  {
    Node * const node = search(key);
    assert(node != nullptr);
    const bool is_full = node->_nKeys == order - 1;
    if(!is_full)
    {
      assert(node->type() == NodeType::Leaf);
      insert_value_to_node(key, value, static_cast<LeafNode *>(node));
    }
    else
    {
    }
  }

  void remove(const key_t key) noexcept {}

  std::optional<key_t> find(const key_t key) const noexcept
  {
    const auto node = static_cast<LeafNode *>(search(key));
    assert(node != nullptr);
    assert(node->type() == NodeType::Leaf);

    const auto first      = node->_keys;
    const auto last       = node->_keys + node->_nKeys;
    const auto target_idx = static_cast<uint8_t>(std::lower_bound(first, last, key) - first);

    if(first != last && key == node->_keys[target_idx])
      return node->_values[target_idx];
    else
      return std::nullopt;
  }
};
