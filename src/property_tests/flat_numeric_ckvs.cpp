#include <page_cache.hpp>
#include <paged_file.hpp>
#include <bptree.hpp>

#include "utils/traits.hpp"

#include <optional>
#include <future>

using namespace ckvs;
using namespace utils;

constexpr size_t page_size           = 4096;
constexpr size_t page_cache_capacity = 16;
constexpr size_t bp_tree_order       = 256;
using page_id                        = uint32_t;

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
struct alignas(page_size) raw_page
{
  std::aligned_storage_t<page_size, page_size> _contents;
};
static_assert(sizeof(raw_page) == page_size);

using nFree_pages_t = uint16_t;

struct alignas(page_size) index_page
{
  constexpr static uint64_t magic_sig_value = "ckvs"_magic_sig;

  uint64_t _magic_sig;
  page_id  _bptree_root_handle;

  bool is_valid() const noexcept { return _magic_sig == magic_sig_value; }

  index_page() noexcept { _magic_sig = magic_sig_value; }
};
using page_cache_t = page_cache<raw_page, page_id>;
static_assert(sizeof(index_page) == page_size);

struct alignas(page_size) free_pages_page
{
  constexpr static size_t max_free_pages = 1023;

  nFree_pages_t _nFree_pages;
  page_id       _free_pages[max_free_pages];

  free_pages_page(const page_id unused_block_start, const page_id unused_block_size) noexcept
  {
    _nFree_pages = unused_block_size;

    for(nFree_pages_t i = unused_block_start; i < unused_block_size; ++i)
      _free_pages[i] = i;
  }

  bool add_free_page(const page_id new_free_page) noexcept
  {
    if(_nFree_pages == max_free_pages)
      return false;
    _free_pages[_nFree_pages++] = new_free_page;
    return true;
  }

  bool extract_free_page(page_id & free_page) noexcept
  {
    if(_nFree_pages == 0)
      return false;
    free_page = _free_pages[--_nFree_pages];
    return true;
  }
};
static_assert(sizeof(free_pages_page) == page_size);

struct store_state
{
  // todo: defaulting to this will overwrite the store on each reopen. we can get future<size_in_pages> instead of _io_thread_ready
  std::atomic<page_id> _first_unused_page_id  = paged_file::extend_granularity + 1;
  page_descriptor *    _index_page_descriptor = nullptr;
};

template <typename NodeT, typename>
class rw_locked_and_cached_extensions
{
  page_cache_t &                  _page_cache;
  paged_file &                    _paged_file;
  store_state &                   _store_state;
  static constexpr inline page_id index_page_id = 0;

protected:
  using node_t = NodeT;

  using r_lock_t        = std::shared_lock<typename page_cache_t::shared_page_lock>;
  using w_lock_t        = std::unique_lock<typename page_cache_t::shared_page_lock>;
  using r_locked_node_t = std::tuple<node_t &, r_lock_t>;
  using w_locked_node_t = std::tuple<node_t &, w_lock_t>;
  using node_handle_t   = page_handle<NodeT>;

  void update_root(const node_handle_t new_root) noexcept
  {
    std::unique_lock locked_index{_page_cache.get_shared_page_lock(index_page_id)};
    auto             index     = locked_index.mutex()->get_page_as<index_page>();
    index->_bptree_root_handle = new_root._id;
  }

  template <bool ExclusivelyLocked>
  using select_locked_node_t = std::conditional_t<ExclusivelyLocked, w_locked_node_t, r_locked_node_t>;

  template <bool ExclusivelyLocked>
  select_locked_node_t<ExclusivelyLocked> get_node(const node_handle_t handle) noexcept
  {
    std::shared_lock locked_bptree_page{_page_cache.get_shared_page_lock(handle._id)};
    return {*locked_bptree_page.mutex()->get_page_as<node_t>(), std::move(locked_bptree_page)};
  }
  template <>
  select_locked_node_t<true> get_node<true>(const node_handle_t handle) noexcept
  {
    std::unique_lock locked_bptree_page{_page_cache.get_shared_page_lock(handle._id)};
    return {*locked_bptree_page.mutex()->get_page_as<node_t>(), std::move(locked_bptree_page)};
  }

  inline node_t & get_node_unsafe(const node_handle_t) noexcept
  {
    // not supported in multithreaded env
    CKVS_ASSERT(false);
    return *(node_t &)nullptr;
  }
  inline w_locked_node_t upgrade_to_node_exclusive(const node_handle_t handle, r_locked_node_t & locked_node) noexcept
  {
    // lock bucket, unlock rlock, lock wlock
  }

  inline void delete_node(const node_handle_t handle) noexcept
  {
    page_id first_unused_page_id = _store_state._first_unused_page_id.load(std::memory_order_acquire);
    for(page_id free_pages_page_id = 1; free_pages_page_id < first_unused_page_id;
        free_pages_page_id += paged_file::extend_granularity)
    {
      std::unique_lock locked_free_pages_page{_page_cache.get_shared_page_lock(free_pages_page_id)};
      if(locked_free_pages_page.mutex()->get_page_as<free_pages_page>()->add_free_page(handle._id))
      {
        locked_free_pages_page.mutex()->mark_dirty();
        break;
      }
    }
    _page_cache.evict_free_page(handle._id);
  }

  template <typename... NodeCtorParams>
  inline std::tuple<node_handle_t, w_locked_node_t> new_node(NodeCtorParams &&... params) noexcept
  {
    node_handle_t new_node_handle      = node_handle_t::invalid();
    page_id       first_unused_page_id = _store_state._first_unused_page_id.load(std::memory_order_acquire);
    // Let's try find an existing free_pages_page. They're located at fixed offsets in the store file.
    for(page_id free_pages_page_id = 1; free_pages_page_id < first_unused_page_id;
        free_pages_page_id += paged_file::extend_granularity)
    {
      std::unique_lock locked_free_pages_page{_page_cache.get_shared_page_lock(free_pages_page_id)};
      if(locked_free_pages_page.mutex()->get_page_as<free_pages_page>()->extract_free_page(new_node_handle._id))
      {
        locked_free_pages_page.mutex()->mark_dirty();
        break;
      }
    }

    while(new_node_handle == node_handle_t::invalid())
    {
      // We couldn't extract it, so we need to consume unused pages block. That will trigger auto-extend from paged_file, since we're requesting the first 'non-existent' page.
      std::unique_lock locked_free_pages_page{_page_cache.get_shared_page_lock(first_unused_page_id)};
      auto *           fpp   = locked_free_pages_page.mutex()->get_page_as<free_pages_page>();
      const bool       clean = fpp->_nFree_pages == free_pages_page::max_free_pages;
      if(fpp->extract_free_page(new_node_handle._id))
      {
        if(clean)
        {
          locked_free_pages_page.mutex()->mark_dirty();
          _store_state._first_unused_page_id.fetch_add(paged_file::extend_granularity, std::memory_order_acq_rel);
        }
      }
      else
      {
        // Somewone already requested this "unused" page_id, let's try getting free page_id from it
        first_unused_page_id = _store_state._first_unused_page_id.load(std::memory_order_acquire);
      }
    }

    std::unique_lock locked_bptree_page{_page_cache.get_shared_page_lock(new_node_handle._id)};
    locked_bptree_page.mutex()->mark_dirty();
    node_t * raw_node = locked_bptree_page.mutex()->get_page_as<node_t>();
    new(raw_node) node_t{std::forward<NodeCtorParams>(params)...};
    return {new_node_handle, w_locked_node_t{*raw_node, std::move(locked_bptree_page)}};
  }

  rw_locked_and_cached_extensions(paged_file & paged_file, page_cache_t & cache, store_state & store_state)
    : _page_cache{std::move(cache)}, _paged_file{paged_file}, _store_state{store_state}
  {
    // We don't need to lock anything, since we're still in ctor
    auto index_page_mutex = _page_cache.get_shared_page_lock(index_page_id);
    index_page_mutex.pin();
    // Assume we'll change ~something~, so always flush root
    index_page_mutex.mark_dirty();

    auto index = index_page_mutex.get_page_as<index_page>();
    if(!index->is_valid())
      new(index) index_page{};
  }

public:
};

using key_t   = uint64_t;
using value_t = uint64_t;

using bp_tree_config_t = bp_tree_config<key_t, value_t, bp_tree_order, page_handle>;
using store_t          = bp_tree<bp_tree_config_t, rw_locked_and_cached_extensions>;

class flat_numeric_ckvs_store
{
  paged_file   _paged_file;
  page_cache_t _page_cache;
  store_state  _store_state;
  store_t      _store;

public:
  flat_numeric_ckvs_store(const char * volume_path, const page_id cache_capacity)

    : _paged_file{volume_path, page_size}
    , _page_cache{cache_capacity, _paged_file}
    , _store{_paged_file, _page_cache, _store_state}
  {
  }

  std::optional<value_t> get(key_t /*key*/) { return std::nullopt; }
  void                   insert(key_t key, value_t value) { _store.insert(key, value); }
  void                   remove(key_t /*key*/) {}
};

void flat_numeric_ckvs_test(const size_t /*iteration*/, std::default_random_engine & /*gen*/, std::ostream & os)
{
  const size_t            nThreads = std::thread::hardware_concurrency();
  flat_numeric_ckvs_store flat_store{RAM_PAGED_FILE_PATH, page_cache_capacity};
  for(key_t i = 1; i < 5; ++i)
    flat_store.insert(i, i * i);
}