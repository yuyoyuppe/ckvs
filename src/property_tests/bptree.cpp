#include <iostream>
#include <vector>

#include <mutex>
#include <shared_mutex>
#include <thread>

#include <bptree.hpp>
#include <utils/random.hpp>

#include <boost/thread/null_mutex.hpp>


using namespace ckvs;

template <typename T>
struct ptr_wrap
{
  T * _ptr;

  bool operator==(const ptr_wrap & rhs) const { return _ptr == rhs._ptr; }
  bool operator!=(const ptr_wrap & rhs) const { return _ptr != rhs._ptr; }

  static inline ptr_wrap invalid() { return ptr_wrap{nullptr}; }
};

using key_t     = uint32_t;
using payload_t = uint32_t;

template <typename T>
using node_handle_t = ptr_wrap<T>;

template <typename NodeT>
struct default_exts
{
  using node_t        = NodeT;
  using node_handle_t = ptr_wrap<node_t>;

  using r_lock_t        = std::shared_lock<boost::null_mutex>;
  using w_lock_t        = r_lock_t;
  using r_locked_node_t = std::tuple<node_t &, r_lock_t>;
  using w_locked_node_t = r_locked_node_t;

  inline w_locked_node_t get_node_shared(const node_handle_t handle) { return {*handle._ptr, {}}; }
  inline r_locked_node_t get_node_exclusive(const node_handle_t handle) { return {*handle._ptr, {}}; }
  inline node_t &        get_node_unsafe(const node_handle_t handle) { return *handle._ptr; }
  inline w_locked_node_t upgrade_to_node_exclusive(const node_handle_t, r_locked_node_t & locked_node)
  {
    return locked_node;
  }

  inline void delete_node(const node_handle_t n) noexcept
  {
    static_assert(noexcept(std::declval<node_t>().~node_t()));
    delete n._ptr;
  }

  template <typename... NodeCtorParams>
  inline node_handle_t new_node(NodeCtorParams &&... params) noexcept
  {
    static_assert(noexcept(node_t(std::forward<NodeCtorParams>(params)...)));
    return {new node_t{std::forward<NodeCtorParams>(params)...}};
  }
};


template <typename NodeT>
struct bp_tree_rw_locked_ext
{
  using node_t        = NodeT;
  using node_handle_t = ptr_wrap<node_t>;
  using key_t         = typename node_t::key_t;

  static inline const node_handle_t invalid_node_handle = ptr_wrap<node_t>{nullptr};


  using r_locked_node_t = std::tuple<node_t &, std::shared_lock<std::shared_mutex>>;
  using w_locked_node_t = std::tuple<node_t &, std::unique_lock<std::shared_mutex>>;

  inline w_locked_node_t get_node_shared(const node_handle_t handle) { return *handle._ptr; }
  inline r_locked_node_t get_node_exclusive(const node_handle_t handle) { return *handle._ptr; }
  inline node_t &        get_node_unsafe(const node_handle_t handle) { return get_node_shared(handle); }
  inline w_locked_node_t upgrade_to_node_exclusive(const node_handle_t handle, r_locked_node_t & locked_node) {}

  inline void delete_node(const node_handle_t n) noexcept
  {
    static_assert(noexcept(std::declval<node_t>().~node_t()));
    delete n._ptr;
  }

  template <typename... NodeCtorParams>
  inline node_handle_t new_node(NodeCtorParams &&... params) noexcept
  {
    static_assert(noexcept(node_t(std::forward<NodeCtorParams>(params)...)));
    node_handle_t res;
    res._ptr = new node_t{std::forward<NodeCtorParams>(params)...};
    return res;
  }
};

using tree_variant_t = std::variant<bp_tree<bp_tree_config<key_t, payload_t, node_handle_t, 4>, default_exts>,
                                    bp_tree<bp_tree_config<key_t, payload_t, node_handle_t, 6>, default_exts>,
                                    bp_tree<bp_tree_config<key_t, payload_t, node_handle_t, 34>, default_exts>,
                                    bp_tree<bp_tree_config<key_t, payload_t, node_handle_t, 70>, default_exts>,
                                    bp_tree<bp_tree_config<key_t, payload_t, node_handle_t, 128>, default_exts>,
                                    bp_tree<bp_tree_config<key_t, payload_t, node_handle_t, 1024>, default_exts>>;

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
      tree.inspect(os);
      //tree.find(123);
      /*
      shuffle(begin(vals), end(vals), gen);
      for(const auto val : vals)
      {
        tree.insert(val, val * val);
        const auto res = tree.find(val);
        CKVS_ASSERT(res != std::nullopt);
        CKVS_ASSERT(*res == val * val);
      }
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
      */
    },
    var_tree);
}
