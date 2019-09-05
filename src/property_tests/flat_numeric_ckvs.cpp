#include <page_cache.hpp>
#include <paged_file.hpp>
#include <bptree.hpp>

#include "utils/traits.hpp"
#include "utils/random.hpp"

#include <optional>
#include <future>
#include <filesystem>

using namespace ckvs;
using namespace utils;
constexpr size_t page_size           = 4096 * 4;
constexpr size_t page_cache_capacity = 65536 / 2;
constexpr size_t bp_tree_order       = 1022;
constexpr size_t bp_tree_tiny_order  = 4;
using page_id                        = uint32_t;

const size_t page_align = 4096;


//#define LOGGING
template <typename>
struct page_handle
{
  page_id                   _id;
  bool                      operator==(const page_handle rhs) const noexcept { return _id == rhs._id; }
  bool                      operator!=(const page_handle rhs) const noexcept { return _id != rhs._id; }
  static inline page_handle invalid() noexcept { return page_handle{0}; }
};

static_assert(sizeof(page_handle<void>) == sizeof(page_id) && alignof(page_handle<void>) == alignof(page_id),
              "we should be able to transmute between node_handle and page_handle");

// Here we use raw bptree nodes
struct alignas(page_align) raw_page
{
  std::aligned_storage_t<page_size, page_align> _contents;
};
static_assert(sizeof(raw_page) == page_size);

using nFree_pages_t = uint16_t;

struct alignas(page_align) index_page
{
  constexpr static uint64_t magic_sig_value = "ckvs"_magic_sig;

  uint64_t _magic_sig;
  page_id  _bptree_root_handle;

  bool is_valid() const noexcept { return _magic_sig == magic_sig_value; }

  index_page() noexcept { _magic_sig = magic_sig_value; }
  char padding[4096 * 3];
};
using page_cache_t = page_cache<raw_page, page_id>;
static_assert(sizeof(index_page) == page_size);

struct alignas(page_align) free_pages_page
{
  constexpr static size_t max_free_pages = 4095;

  nFree_pages_t _nFree_pages;
  page_id       _free_pages[max_free_pages];

  free_pages_page(const page_id first_free_page, const nFree_pages_t block_size) noexcept
  {
    _nFree_pages = block_size;

    for(nFree_pages_t i = 0; i < block_size; ++i)
      _free_pages[i] = block_size + first_free_page - 1 - i;
  }

  bool add_free_page(const page_id new_free_page) noexcept
  {
    CKVS_ASSERT(new_free_page % paged_file::extend_granularity != 1);
    if(_nFree_pages == max_free_pages)
      return false;
    _free_pages[_nFree_pages++] = new_free_page;
#if defined(LOGGING)
    printf("[%zu] id %u freed\n", std::this_thread::get_id(), new_free_page);
#endif
    return true;
  }

  bool extract_free_page(page_id & free_page) noexcept
  {
    if(_nFree_pages == 0)
      return false;
    free_page = _free_pages[--_nFree_pages];
#if defined(LOGGING)
    printf("[%zu] id %u now used\n", std::this_thread::get_id(), free_page);
#endif
    CKVS_ASSERT(free_page % paged_file::extend_granularity != 1);
    return true;
  }
};
static_assert(sizeof(free_pages_page) == page_size);

struct store_state
{
  std::atomic<page_id> _first_unused_page_id = paged_file::extend_granularity + 1;
};

struct slotted_value
{
  int              _value;
  bool             is_view = true;
  std::string_view _view;

  explicit slotted_value(int && value) noexcept : _value(std::move(value)) {}
  slotted_value()                          = default;
  slotted_value(const slotted_value & rhs) = default;
  slotted_value(slotted_value && rhs)      = default;
  slotted_value & operator=(slotted_value && rhs) = default;
  slotted_value & operator=(const slotted_value & rhs) = default;

  bool operator==(const int & rhs) const noexcept { return _value == rhs; }

  bool operator==(const slotted_value & rhs) const noexcept { return _value == rhs._value; }

  bool operator!=(const slotted_value & rhs) const noexcept { return !(*this == rhs); }

  bool operator<(const slotted_value & rhs) const noexcept { return _value < rhs._value; }
};

template <typename Config>
class rw_locked_and_cached_extensions
{
  paged_file &                    _paged_file;
  page_cache_t &                  _page_cache;
  store_state &                   _store_state;
  static constexpr inline page_id index_page_id            = 0;
  static constexpr inline page_id first_free_pages_page_id = 1;

protected:
  using node_t        = typename Config::node_t;
  using node_handle_t = page_handle<node_t>;

  using r_lock_t        = typename page_cache_t::shared_lazy_page_lock<false>;
  using w_lock_t        = typename page_cache_t::shared_lazy_page_lock<true>;
  using r_locked_node_t = ckvs::detail::locked_node_t<node_t, node_handle_t, r_lock_t>;
  using w_locked_node_t = ckvs::detail::locked_node_t<node_t, node_handle_t, w_lock_t>;

  static_assert(sizeof(node_t) < page_size);

  inline void mark_dirty(w_locked_node_t & locked_node) noexcept { locked_node._lock.mark_dirty(); }

  void update_root(const node_handle_t new_root) noexcept
  {
    CKVS_ASSERT(new_root._id % paged_file::extend_granularity != 1);

    w_lock_t exclusive_index{_page_cache.get_lazy_page_lock<true>(index_page_id)};
    exclusive_index.lock();
    auto index                 = exclusive_index.get_page_as<index_page>();
    index->_bptree_root_handle = new_root._id;
#if defined(LOGGING)
    printf("[%zu] update root to id %u \n", std::this_thread::get_id(), new_root._id);
#endif
  }

  template <bool ExclusivelyLocked>
  using select_locked_node_t = std::conditional_t<ExclusivelyLocked, w_locked_node_t, r_locked_node_t>;

  void validate(node_t * node) { CKVS_ASSERT(node->_kind != ckvs::detail::node_kind::Removed || !node->_nKeys); }

  template <bool ExclusivelyLocked>
  select_locked_node_t<ExclusivelyLocked> get_node(const node_handle_t handle) noexcept
  {
    CKVS_ASSERT(handle._id < _store_state._first_unused_page_id.load(std::memory_order_acquire));
    CKVS_ASSERT(handle._id % paged_file::extend_granularity != 1);
    r_lock_t shared_bptree_page{_page_cache.get_lazy_page_lock<false>(handle._id)};
    shared_bptree_page.lock();
    r_locked_node_t result;
    result._node   = shared_bptree_page.get_page_as<node_t>();
    result._lock   = std::move(shared_bptree_page);
    result._handle = handle;
    validate(result._node);
    return result;
  }

  template <>
  select_locked_node_t<true> get_node<true>(const node_handle_t handle) noexcept
  {
    CKVS_ASSERT(handle._id < _store_state._first_unused_page_id.load(std::memory_order_acquire));
    CKVS_ASSERT(handle._id % paged_file::extend_granularity != 1);
    w_lock_t exclusive_bptree_page{_page_cache.get_lazy_page_lock<true>(handle._id)};
    exclusive_bptree_page.lock();
    w_locked_node_t result;
    result._node   = exclusive_bptree_page.get_page_as<node_t>();
    result._lock   = std::move(exclusive_bptree_page);
    result._handle = handle;
    validate(result._node);
    return result;
  }

  inline std::optional<w_locked_node_t> upgrade_to_node_exclusive(r_locked_node_t & locked_node) noexcept
  {
#if defined(LOGGING)
    printf("[%zu] trying to upgrade #%u to exclusive!\n", std::this_thread::get_id(), locked_node._handle._id);
#endif
    std::optional<w_lock_t> maybe_upgraded_lock{locked_node._lock.upgrade_or_consume()};
    if(!maybe_upgraded_lock)
    {
#if defined(LOGGING)
      printf("[%zu] self-destructed!!\n", std::this_thread::get_id());
#endif
      return std::nullopt;
    }
#if defined(LOGGING)
    printf("[%zu] upgrade #%u to exclusive OK!!\n", std::this_thread::get_id(), locked_node._handle._id);
#endif
    w_locked_node_t result;
    result._node   = locked_node._node;
    result._lock   = std::move(*maybe_upgraded_lock);
    result._handle = locked_node._handle;
    return result;
  }

  inline void delete_node(w_locked_node_t & locked_node) noexcept
  {
    page_id first_unused_page_id = _store_state._first_unused_page_id.load(std::memory_order_acquire);
    for(page_id free_pages_page_id = 1; free_pages_page_id < first_unused_page_id;
        free_pages_page_id += paged_file::extend_granularity)
    {
      w_lock_t exclusive_free_pages_page{_page_cache.get_lazy_page_lock<true>(free_pages_page_id)};
      exclusive_free_pages_page.lock();
      if(exclusive_free_pages_page.get_page_as<free_pages_page>()->add_free_page(locked_node._handle._id))
      {
        exclusive_free_pages_page.mark_dirty();
        break;
      }
    }
  }
  template <typename... NodeCtorParams>
  inline w_locked_node_t new_node(NodeCtorParams &&... params) noexcept
  {
    node_handle_t new_node_handle      = node_handle_t::invalid();
    page_id       first_unused_page_id = _store_state._first_unused_page_id.load(std::memory_order_acquire);
    // Let's try find an existing free_pages_page. They're located at fixed offsets in the store file.

    w_lock_t exclusive_previous_page{};
    for(page_id free_pages_page_id = 1; free_pages_page_id < first_unused_page_id;
        free_pages_page_id += paged_file::extend_granularity)
    {
      w_lock_t exclusive_free_pages_page{_page_cache.get_lazy_page_lock<true>(free_pages_page_id)};
#if defined(LOGGING)
      printf("[%zu] new_node: waiting to lock #%u\n", std::this_thread::get_id(), free_pages_page_id);
#endif
      exclusive_free_pages_page.lock();
      if(exclusive_free_pages_page.get_page_as<free_pages_page>()->extract_free_page(new_node_handle._id))
      {
#if defined(LOGGING)
        printf("[%zu] new_node: extracted #%u from #%u\n",
               std::this_thread::get_id(),
               new_node_handle._id,
               free_pages_page_id);
#endif

        exclusive_free_pages_page.mark_dirty();
        break;
      }
#if defined(LOGGING)
      printf("[%zu] new_node: #%u is full, moving to next free_pages_page\n",
             std::this_thread::get_id(),
             free_pages_page_id);
#endif

      // Do not unlock previous node, so we can't be outrun
      exclusive_previous_page = std::move(exclusive_free_pages_page);
    }

    while(new_node_handle == node_handle_t::invalid())
    {
#if defined(LOGGING)
      printf("[%zu] new_node: couldn't find a free node, extending...\n", std::this_thread::get_id());
#endif
      // We couldn't extract it, so we need to consume unused pages block. That will trigger auto-extend from paged_file, since we're requesting the first 'non-existent' page.
      w_lock_t exclusive_free_pages_page{_page_cache.get_lazy_page_lock<true>(first_unused_page_id)};
      exclusive_free_pages_page.lock();
      auto * fpp = exclusive_free_pages_page.get_page_as<free_pages_page>();
      new(fpp) free_pages_page{first_unused_page_id + 1, paged_file::extend_granularity - 1};
      if(fpp->extract_free_page(new_node_handle._id))
      {

#if defined(LOGGING)
        printf("[%zu] new_node: created a new free pages page #%u and got #%u for new node\n",
               std::this_thread::get_id(),
               first_unused_page_id,
               new_node_handle._id);
#endif
        exclusive_free_pages_page.mark_dirty();
        _store_state._first_unused_page_id.fetch_add(paged_file::extend_granularity, std::memory_order_acq_rel);
      }
      else
      {
        // That should never happen, since we're lock-crabbing the free-pages.
        CKVS_ASSERT(false);
      }
    }
    if(exclusive_previous_page.owns_lock())
      exclusive_previous_page.unlock();
    w_lock_t exclusive_bptree_page{_page_cache.get_lazy_page_lock<true>(new_node_handle._id)};
    exclusive_bptree_page.lock();
    exclusive_bptree_page.mark_dirty();
    node_t * raw_node = exclusive_bptree_page.get_page_as<node_t>();
    new(raw_node) node_t{std::forward<NodeCtorParams>(params)...};

    w_locked_node_t result;
    result._lock   = std::move(exclusive_bptree_page);
    result._node   = raw_node;
    result._handle = new_node_handle;
    CKVS_ASSERT(new_node_handle._id % paged_file::extend_granularity != 1);

    return result;
  }

  node_handle_t get_root() noexcept
  {
    r_lock_t shared_index{_page_cache.get_lazy_page_lock<false>(index_page_id)};
    shared_index.lock();
    auto index = shared_index.get_page_as<index_page>();
    CKVS_ASSERT(index->_bptree_root_handle % paged_file::extend_granularity != 1);
    return {index->_bptree_root_handle};
  }


  rw_locked_and_cached_extensions(paged_file &   paged_file,
                                  page_cache_t & cache,
                                  store_state &  store_state,
                                  const bool     assume_valid)
    : _page_cache{cache}, _paged_file{paged_file}, _store_state{store_state}
  {
    auto exclusive_index_page = _page_cache.get_lazy_page_lock<true>(index_page_id);
    exclusive_index_page.lock();
    // Never evict index
    exclusive_index_page.pin();
    // Assume we'll change ~something~, so always flush root
    exclusive_index_page.mark_dirty();
    auto index = exclusive_index_page.get_page_as<index_page>();
    if(!index->is_valid())
    {
      CKVS_ASSERT(!assume_valid);
      {
        new(index) index_page{};
        {
          w_lock_t exclusive_first_free_pages_page{_page_cache.get_lazy_page_lock<true>(first_free_pages_page_id)};
          exclusive_first_free_pages_page.lock();
          new(exclusive_first_free_pages_page.get_page_as<free_pages_page>())
            free_pages_page{first_free_pages_page_id + 1, paged_file::extend_granularity - 1};
        }
        auto locked_new_root       = new_node(ckvs::detail::node_kind::RootLeaf);
        index->_bptree_root_handle = locked_new_root._handle._id;
      }
    }
  }
};

using key_t   = uint64_t;
using value_t = uint64_t;

using bp_tree_config_t      = bp_tree_config<key_t, value_t, bp_tree_order, page_handle>;
using tiny_bp_tree_config_t = bp_tree_config<key_t, value_t, bp_tree_tiny_order, page_handle>;

using store_t      = bp_tree<bp_tree_config_t, rw_locked_and_cached_extensions>;
using tiny_store_t = bp_tree<tiny_bp_tree_config_t, rw_locked_and_cached_extensions>;

template <typename StoreT = store_t>
class flat_numeric_ckvs_store
{
public:
  page_cache_t _page_cache;
  paged_file   _paged_file;
  store_state  _store_state;
  store_t      _store;

public:
  flat_numeric_ckvs_store(const char * volume_path, const page_id cache_capacity, const bool assume_valid = false)

    : _paged_file{volume_path, page_size}
    , _page_cache{cache_capacity, _paged_file}
    , _store{_paged_file, _page_cache, _store_state, assume_valid}
  {
  }
  ~flat_numeric_ckvs_store()
  {
    // todo: find the last empty free_pages_page and shrink paged_file to it
    _page_cache.shutdown();
  }

  std::optional<value_t> find(key_t key)
  {
#if defined(LOGGING)
    printf("[%zu] find %u started \n", std::this_thread::get_id(), key);
#endif
    auto res = _store.find(key);
#if defined(LOGGING)
    printf("[%zu] find %u finished \n", std::this_thread::get_id(), key);
#endif
    return res;
  }
  void insert(key_t key, value_t value)
  {
#if defined(LOGGING)
    printf("[%zu] insert %u,%u started \n", std::this_thread::get_id(), key, value);
#endif
    _store.insert(key, value);
#if defined(LOGGING)
    printf("[%zu] insert %u,%u finished \n", std::this_thread::get_id(), key, value);
#endif
  }
  void remove(key_t key)
  {
#if defined(LOGGING)
    printf("[%zu] remove %u started \n", std::this_thread::get_id(), key);
#endif
    _store.remove(key);
#if defined(LOGGING)
    printf("[%zu] remove %u finished \n", std::this_thread::get_id(), key);
#endif
  }
};

void flat_numeric_ckvs_contention_test(const size_t iteration, std::default_random_engine & gen, std::ostream & os)
{
  const size_t       nThreads = std::min(iteration, static_cast<size_t>(std::thread::hardware_concurrency()));
  std::vector<key_t> vals;
  const size_t       nVals = 5'000 * iteration;
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
        for(const key_t val : parts[i])
        {
          flat_store.insert(val, val * val);
          const auto res = flat_store.find(val);
          CKVS_ASSERT(res != std::nullopt);
          CKVS_ASSERT(*res == val * val);
        }
        for(size_t idx = 0; idx < std::size(parts[i]); ++idx)
        {
          flat_store.remove(parts[i][idx]);
          const auto removed = flat_store.find(parts[i][idx]);
          CKVS_ASSERT(removed == std::nullopt);
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
  const size_t       nThreads = std::min(iteration, static_cast<size_t>(std::thread::hardware_concurrency()));
  std::vector<key_t> vals;
  const size_t       nVals = std::min(page_cache_capacity * bp_tree_order, 5'000 * iteration);
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
        for(const key_t val : parts[i])
          flat_store.insert(val, val * val);
        for(const key_t val : parts[i])
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
    const auto res = flat_store.find(val);
    if((val & 1) != 0)
    {
      CKVS_ASSERT(res == std::nullopt);
    }
    else
    {
      CKVS_ASSERT(res && *res == val * val);
    }
  }
}

void flat_numeric_ckvs_tiny_order_test(const size_t iteration, std::default_random_engine & gen, std::ostream & os)
{
  const size_t       nThreads = std::min(iteration, static_cast<size_t>(std::thread::hardware_concurrency()));
  std::vector<key_t> vals;
  const size_t       nVals = 1'000 * iteration;
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
        for(const key_t val : parts[i])
        {
          flat_store.insert(val, val * val);
          const auto res = flat_store.find(val);
          CKVS_ASSERT(res != std::nullopt);
          CKVS_ASSERT(*res == val * val);
        }
        for(size_t idx = 0; idx < std::size(parts[i]); ++idx)
        {
          flat_store.remove(parts[i][idx]);
          const auto removed = flat_store.find(parts[i][idx]);
          CKVS_ASSERT(removed == std::nullopt);
        }
      });
    }
    for(auto & t : threads)
      if(t.joinable())
        t.join();
  }
  os << '\n';
}

template <typename StoreT = store_t>
class flat_slotted_ckvs_store
{
  // todo: to implement slotted values which could be loaded/saved w/o a (de)-serialization phase,
  // we need to introduce a new concept in the bptree class - assign proxy, which bptree could
  // get from a locked_node_t interface. then it performs all operations on _slots/_keys using that assign
  // proxy. those proxies will hold a slotted_storage(to be able to transfer the actual data between pages)
  // and kv-store pointer, so they can request a additional pages for overflow.

public:
  page_cache_t _page_cache;
  paged_file   _paged_file;
  store_state  _store_state;
  store_t      _store;

public:
  flat_slotted_ckvs_store(const char * volume_path, const page_id cache_capacity, const bool assume_valid = false)

    : _paged_file{volume_path, page_size}
    , _page_cache{cache_capacity, _paged_file}
    , _store{_paged_file, _page_cache, _store_state, assume_valid}
  {
  }
  ~flat_slotted_ckvs_store() { _page_cache.shutdown(); }

  std::optional<value_t> find(key_t key)
  {
#if defined(LOGGING)
    printf("[%zu] find %u started \n", std::this_thread::get_id(), key);
#endif
    auto res = _store.find(key);
#if defined(LOGGING)
    printf("[%zu] find %u finished \n", std::this_thread::get_id(), key);
#endif
    return res;
  }
  void insert(key_t key, value_t value)
  {
#if defined(LOGGING)
    printf("[%zu] insert %u,%u started \n", std::this_thread::get_id(), key, value);
#endif
    _store.insert(key, value);
#if defined(LOGGING)
    printf("[%zu] insert %u,%u finished \n", std::this_thread::get_id(), key, value);
#endif
  }
};

void slotted_ckvs_test(const size_t /*iteration*/, std::default_random_engine & /*gen*/, std::ostream & /*os*/)
{
  using slotted_bp_tree_config_t = bp_tree_config<key_t, slotted_value, 4, page_handle>;
  using slotted_store_t          = bp_tree<tiny_bp_tree_config_t, rw_locked_and_cached_extensions>;

  flat_slotted_ckvs_store<slotted_store_t> store{RAM_PAGED_FILE_PATH, page_cache_capacity};
}


void flat_numeric_ckvs_custom_test(const size_t , std::default_random_engine & gen, std::ostream & os)
{
  const size_t       nThreads = std::thread::hardware_concurrency();
  std::vector<key_t> vals;
  const size_t       nVals = 5'000'000;
  vals.resize(nVals);
  const unsigned step = gen() % size(vals) + 1u;
  for(unsigned i = 0; i < size(vals); ++i)
    vals[i] = i + step;
  shuffle(begin(vals), end(vals), gen);
  auto                     parts = *split_to_random_parts(vals, nThreads, gen);
  std::vector<std::thread> threads;
  {
    std::filesystem::remove(RAM_PAGED_FILE_PATH);
    
    {
      flat_numeric_ckvs_store flat_store{RAM_PAGED_FILE_PATH, page_cache_capacity};
      for(const key_t val : vals)
        flat_store.insert(val, val * val);
    }
    flat_numeric_ckvs_store flat_store{RAM_PAGED_FILE_PATH, page_cache_capacity, true};

    os << "removed in " << utils::quick_profile([&]{
      for(size_t i = 0; i < nThreads; ++i)
      {
        threads.emplace_back([&parts, &flat_store, i]() {
          for(const key_t val : parts[i])
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

