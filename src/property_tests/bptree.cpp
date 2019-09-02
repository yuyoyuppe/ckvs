#include <iostream>
#include <vector>

#include <mutex>
#include <shared_mutex>
#include <thread>

#include <bptree.hpp>
#include <utils/random.hpp>


using namespace ckvs;

using key_t     = uint32_t;
using payload_t = uint32_t;

using tree_variant_t = std::variant<bp_tree<bp_tree_config<key_t, payload_t, 4>>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 6>>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 34>>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 70>>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 128>>,
                                    bp_tree<bp_tree_config<key_t, payload_t, 1024>>>;
void bp_tree_test(const size_t iteration, std::default_random_engine & gen, std::ostream & os)
{
  tree_variant_t var_tree;
  utils::default_init_variant(var_tree, gen() % std::variant_size_v<tree_variant_t>);

  std::visit(
    [&](auto & tree) {
      std::vector<key_t> vals;
      vals.resize(iteration * iteration);
      const unsigned step = gen() % size(vals) + 1u;
      for(unsigned i = 0; i < size(vals); ++i)
        vals[i] = i + step;
      shuffle(begin(vals), end(vals), gen);
      for(const auto val : vals)
        os << val << ',';
      os << '\n';
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
    },
    var_tree);
}
