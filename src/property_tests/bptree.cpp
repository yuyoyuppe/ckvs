#include <iostream>
#include <vector>

#include <bptree.hpp>

#include "utils/random.hpp"

using namespace ckvs;

using key_t     = uint32_t;
using payload_t = uint32_t;


template <typename NodeT>
struct bp_tree_no_extensions
{
  using node_t        = NodeT;
  using node_handle_t = NodeT *;

  static inline const node_handle_t invalid_node_handle = nullptr;

  using key_t = typename node_t::key_t;

  inline node_t & get_node_from_handle_unsafe(const node_handle_t handle) { return *handle; }

  inline node_t & get_node_from_handle(const node_handle_t handle) { return get_node_from_handle_unsafe(handle); }

  inline void delete_node(const node_handle_t n) noexcept
  {
    static_assert(noexcept(std::declval<node_t>().~node_t()));
    delete n;
  }

  template <typename... NodeCtorParams>
  inline node_handle_t new_node(NodeCtorParams &&... params) noexcept
  {
    static_assert(noexcept(node_t(std::forward<NodeCtorParams>(params)...)));
    return new node_t{std::forward<NodeCtorParams>(params)...};
  }
};

using tree_variant_t = std::variant<bp_tree<bp_tree_config<key_t, payload_t, 4>, bp_tree_no_extensions>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 6>, bp_tree_no_extensions>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 34>, bp_tree_no_extensions>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 70>, bp_tree_no_extensions>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 128>, bp_tree_no_extensions>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 1024>, bp_tree_no_extensions>>;

void bp_tree_test(const size_t iteration, std::default_random_engine & gen, std::ostream & os)
{
  using key_t     = uint32_t;
  using payload_t = uint32_t;

  tree_variant_t var_tree;

  utils::default_init_variant(var_tree, gen() % std::variant_size_v<tree_variant_t>);

  std::visit(
    [&](auto & tree) {
      std::vector<key_t> vals;
      vals.resize(iteration * iteration);
      const unsigned step = gen() % size(vals) + 1u;
      for(unsigned i = 0; i < size(vals); ++i)
        vals[i] = i + step;
      for(const auto val : vals)
        os << val << ',';
      os << '\n';

      shuffle(begin(vals), end(vals), gen);
      for(const auto val : vals)
      {
        tree.insert(val, val * val);
        const auto res = tree.find(val);
        CKVS_ASSERT(res != std::nullopt);
        CKVS_ASSERT(*res == val * val);
      }
      tree.inspect(os);
      for(const auto val : vals)
      {
        const auto res = tree.find(val);
        CKVS_ASSERT(res != std::nullopt);
        CKVS_ASSERT(*res == val * val);
      }

      shuffle(begin(vals), end(vals), gen);
      for(const auto val : vals)
        os << val << ',';
      os << '\n';
      for(size_t i = 0; i < size(vals); ++i)
      {
        tree.remove(vals[i]);
        const auto removed = tree.find(vals[i]);
        CKVS_ASSERT(removed == std::nullopt);
        os << "after " << vals[i] << " deletion:\n";
        tree.inspect(os);
        for(size_t j = i + 1; j < size(vals); ++j)
        {
          const auto res = tree.find(vals[j]);
          CKVS_ASSERT(res != std::nullopt);
          CKVS_ASSERT(*res == vals[j] * vals[j]);
        }
      }
      os << '\n';
    },
    var_tree);
}
