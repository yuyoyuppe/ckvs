#include <slotted_storage.hpp>
#include <slotted_types.hpp>
#include <locked_and_cached_extensions.hpp>

#include "utils/random.hpp"

#include <optional>
#include <future>
#include <filesystem>

using namespace ckvs;
using namespace utils;
constexpr size_t page_cache_capacity = 65536 / 2;
constexpr size_t bp_tree_order       = 1022;
constexpr size_t bp_tree_tiny_order  = 4;

#pragma warning(disable : 4309)

using bp_tree_config_t = bp_tree_config<uint64_t,
                                        uint64_t,
                                        //bp_tree_order,
                                        bp_tree_tiny_order,
                                        ckvs::detail::default_key_handle,
                                        ckvs::detail::default_payload_handle,
                                        page_handle>;

using tiny_bp_tree_config_t = bp_tree_config<uint64_t,
                                             uint64_t,
                                             bp_tree_tiny_order,
                                             ckvs::detail::default_key_handle,
                                             ckvs::detail::default_payload_handle,
                                             page_handle>;
using tiny_store_t          = bp_tree<tiny_bp_tree_config_t, locked_and_cached_extensions>;

template <typename StoreT = bp_tree<bp_tree_config_t, locked_and_cached_extensions>>
class flat_numeric_ckvs_store
{
public:
  page_cache_t _page_cache;
  paged_file   _paged_file;
  store_state  _store_state;
  StoreT       _store;

public:
  flat_numeric_ckvs_store(const char * volume_path, const page_id cache_capacity, const bool assume_valid = false)

    : _paged_file{volume_path, page_size}
    , _page_cache{cache_capacity, _paged_file}
    , _store_state{paged_file::extend_granularity + 1, &_paged_file, &_page_cache}
    , _store{_store_state, assume_valid}
  {
  }
  ~flat_numeric_ckvs_store()
  {
    // todo: find the last empty free_pages_page and shrink paged_file to it
    _page_cache.shutdown();
  }

  template <typename Visitor>
  void visit(const typename StoreT::key_t key, Visitor func)
  {
    return _store.visit(key, func);
  }
  void insert(typename StoreT::key_t key, typename StoreT::payload_t value) { _store.insert(key, value); }
  void remove(typename StoreT::key_t key) { _store.remove(key); }
};

void flat_numeric_ckvs_contention_test(const size_t iteration, std::default_random_engine & gen, std::ostream & os)
{
  const size_t          nThreads = std::min(iteration, static_cast<size_t>(std::thread::hardware_concurrency()));
  std::vector<uint64_t> vals;
  const size_t          nVals = 5'000 * iteration;
  vals.resize(nVals);
  const unsigned step = gen() % size(vals) + 1u;
  for(unsigned i = 0; i < size(vals); ++i)
    vals[i] = i + step;
  shuffle(begin(vals), end(vals), gen);
  auto                     parts = *split_to_random_parts(vals, nThreads, gen);
  std::vector<std::thread> threads;
  {
    std::filesystem::remove(RAM_PAGED_FILE_PATH);
    flat_numeric_ckvs_store flat_store{RAM_PAGED_FILE_PATH, page_cache_capacity};
    for(size_t i = 0; i < nThreads; ++i)
    {
      threads.emplace_back([&parts, &flat_store, &os, i]() {
        for(const uint64_t val : parts[i])
        {
          flat_store.insert(val, val * val);
          flat_store.visit(val, [&](auto * res) {
            CKVS_ASSERT(res != nullptr);
            CKVS_ASSERT(*res == val * val);
          });
        }
        for(size_t idx = 0; idx < std::size(parts[i]); ++idx)
        {
          flat_store.remove(parts[i][idx]);
          flat_store.visit(parts[i][idx], [&](auto * removed) { CKVS_ASSERT(removed == nullptr); });
        }
      });
    }
    for(auto & t : threads)
      if(t.joinable())
        t.join();
  }
  os << '\n';
}

void flat_numeric_ckvs_persistence_test(const size_t iteration, std::default_random_engine & gen, std::ostream & os)
{
  const size_t          nThreads = std::min(iteration, static_cast<size_t>(std::thread::hardware_concurrency()));
  std::vector<uint64_t> vals;
  const size_t          nVals = std::min(page_cache_capacity * bp_tree_order, 5'000 * iteration);
  vals.resize(nVals);
  const unsigned step = gen() % size(vals) + 1u;
  for(unsigned i = 0; i < size(vals); ++i)
    vals[i] = i + step;
  shuffle(begin(vals), end(vals), gen);
  auto                     parts = *split_to_random_parts(vals, nThreads, gen);
  std::vector<std::thread> threads;
  {
    std::filesystem::remove(RAM_PAGED_FILE_PATH);
    flat_numeric_ckvs_store flat_store{RAM_PAGED_FILE_PATH, page_cache_capacity};
    for(size_t i = 0; i < nThreads; ++i)
    {
      threads.emplace_back([&parts, &flat_store, &os, i]() {
        for(const uint64_t val : parts[i])
          flat_store.insert(val, val * val);
        for(const uint64_t val : parts[i])
          if((val & 1) != 0)
            flat_store.remove(val);
      });
    }
    for(auto & t : threads)
      if(t.joinable())
        t.join();
  }

  os << '\n';
  flat_numeric_ckvs_store flat_store{RAM_PAGED_FILE_PATH, page_cache_capacity, true};
  flat_store._store.inspect(os);
  for(const auto val : vals)
  {
    flat_store.visit(val, [&](auto * res) {
      if((val & 1) != 0)
      {
        CKVS_ASSERT(res == nullptr);
      }
      else
      {
        CKVS_ASSERT(res && *res == val * val);
      }
    });
  }
}

void flat_numeric_ckvs_tiny_order_test(const size_t iteration, std::default_random_engine & gen, std::ostream & os)
{
  const size_t          nThreads = std::min(iteration, static_cast<size_t>(std::thread::hardware_concurrency()));
  std::vector<uint64_t> vals;
  const size_t          nVals = 1'000 * iteration;
  vals.resize(nVals);
  const unsigned step = gen() % size(vals) + 1u;
  for(unsigned i = 0; i < size(vals); ++i)
    vals[i] = i + step;
  shuffle(begin(vals), end(vals), gen);
  auto                     parts = *split_to_random_parts(vals, nThreads, gen);
  std::vector<std::thread> threads;
  {
    std::filesystem::remove(RAM_PAGED_FILE_PATH);
    flat_numeric_ckvs_store<tiny_store_t> flat_store{RAM_PAGED_FILE_PATH, page_cache_capacity};
    for(size_t i = 0; i < nThreads; ++i)
    {
      threads.emplace_back([&parts, &flat_store, &os, i]() {
        for(const uint64_t val : parts[i])
        {
          flat_store.insert(val, val * val);
          flat_store.visit(val, [&](auto * res) {
            CKVS_ASSERT(res != nullptr);
            CKVS_ASSERT(*res == val * val);
          });
        }
        for(size_t idx = 0; idx < std::size(parts[i]); ++idx)
        {
          flat_store.remove(parts[i][idx]);
          flat_store.visit(parts[i][idx], [&](auto * removed) { CKVS_ASSERT(removed == nullptr); });
        }
      });
    }
    for(auto & t : threads)
      if(t.joinable())
        t.join();
  }
  os << '\n';
}

void flat_numeric_ckvs_custom_test(const size_t, std::default_random_engine & gen, std::ostream & os)
{
  const size_t          nThreads = 1; //std::thread::hardware_concurrency();
  std::vector<uint64_t> vals;
  //const size_t          nVals = 5'000'000;
  const size_t nVals = 1'800;
  vals.resize(nVals);
  const unsigned step = /*gen() % size(vals) +*/ 1u;
  for(unsigned i = 0; i < size(vals); ++i)
    vals[i] = i + step;
  shuffle(begin(vals), end(vals), gen);
  auto                     parts = *split_to_random_parts(vals, nThreads, gen);
  std::vector<std::thread> threads;
  {
    std::filesystem::remove(RAM_PAGED_FILE_PATH);

    {
      flat_numeric_ckvs_store flat_store{RAM_PAGED_FILE_PATH, page_cache_capacity};
      for(const auto val : vals)
        flat_store.insert(val, val * val);
    }
    flat_numeric_ckvs_store flat_store{RAM_PAGED_FILE_PATH, page_cache_capacity, true};
    os << "removed in " << utils::quick_profile([&] {
      for(size_t i = 0; i < nThreads; ++i)
      {
        threads.emplace_back([&parts, &flat_store, i]() {
          for(const auto val : parts[i])
            flat_store.remove(val);
        });
        for(auto & t : threads)
          if(t.joinable())
            t.join();
      }
    }) << " sec\n";
  }
  os << '\n';
}

template <typename TreeT>
class flat_slotted_ckvs_store
{
  using key_t     = typename TreeT::key_t;
  using payload_t = typename TreeT::payload_t;

public:
  page_cache_t _page_cache;
  paged_file   _paged_file;
  store_state  _store_state;
  TreeT        _tree;

public:
  flat_slotted_ckvs_store(const char * volume_path, const page_id cache_capacity, const bool assume_valid = false)

    : _paged_file{volume_path, page_size}
    , _page_cache{cache_capacity, _paged_file}
    , _store_state{paged_file::extend_granularity + 1, &_paged_file, &_page_cache}
    , _tree{_store_state, assume_valid}
  {
  }
  ~flat_slotted_ckvs_store() { _page_cache.shutdown(); }

  template <typename KeyComparable, typename Visitor>
  void visit(const KeyComparable key, Visitor func)
  {
    _tree.visit(key, func);
  }

  template <typename KeyConvertable, typename PayloadConvertable>
  void insert(const KeyConvertable key, const PayloadConvertable value)
  {
    _tree.insert(key, value);
  }
};

void slotted_ckvs_test(const size_t /*iteration*/, std::default_random_engine & /*gen*/, std::ostream & os)
{
  constexpr size_t order = 512;

  using fake_config_t           = bp_tree_config<uint64_t,
                                       uint64_t,
                                       order,
                                       apply_slotted_params<slotted_value_handle, page_size, true>::type,
                                       apply_slotted_params<slotted_value_handle, page_size, false>::type,
                                       page_handle>;
  using fake_bptree_node_page_t = bp_tree_node_page<page_size, fake_config_t>;

  using raw_value_t       = uint64_t;
  using slotted_key_t     = slotted_value<page_size, true, raw_value_t, fake_bptree_node_page_t>;
  using slotted_payload_t = slotted_value<page_size, false, raw_value_t, fake_bptree_node_page_t>;

  // Since we've used fake bptree_node_page type, we need to make sure we're accessing it correctly
  static_assert(utils::same_size_and_align_v<slotted_key_t, raw_value_t>);
  static_assert(utils::same_size_and_align_v<slotted_payload_t, raw_value_t>);

  using slotted_bp_tree_config_t = bp_tree_config<slotted_key_t,
                                                  slotted_payload_t,
                                                  order,
                                                  apply_slotted_params<slotted_value_handle, page_size, true>::type,
                                                  apply_slotted_params<slotted_value_handle, page_size, false>::type,
                                                  page_handle>;
  using slotted_bptree_t         = bp_tree<slotted_bp_tree_config_t, locked_and_cached_extensions>;
  using slotted_store_t          = flat_slotted_ckvs_store<slotted_bptree_t>;

  slotted_store_t bpt{RAM_PAGED_FILE_PATH, page_cache_capacity};

  std::vector<std::string> values_storage;
  constexpr size_t         n_vals = 30;
  for(uint64_t i = 1; i <= n_vals; ++i)
  {
    values_storage.emplace_back(std::to_string(i));

    slotted_value_view new_key{i};
    slotted_value_view new_value{i * i};
    bpt.insert(new_key, new_value);
    for(uint64_t j = 1; j <= i; ++j)
    {
      bpt.visit(slotted_value_view{j}, [&](auto * v) {
        slotted_value_view existing_value{j * j};
        CKVS_ASSERT(v && existing_value == *v);
      });
    }
  }

  for(size_t i = 0; i < n_vals; ++i)
  {
    //const auto v = bpt.find(i);
    //CKVS_ASSERT(v->as_span() == std::string_view{values_storage[i]});
  }

  os << '\n';
}
