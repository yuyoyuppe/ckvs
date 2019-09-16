#pragma once
#include "page_cache.hpp"
#include "paged_file.hpp"
#include "bptree.hpp"
#include "utils/common.hpp"

// todo: this is moved from tests. make it template arguments instead.
using page_id              = uint32_t;
constexpr size_t page_size = 4096 * 4;

namespace ckvs {
// todo: move page stuff to pages.hpp
struct alignas(utils::page_align) raw_page
{
  std::aligned_storage_t<page_size, utils::page_align> _contents;
};
static_assert(sizeof(raw_page) == page_size);

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

using nFree_pages_t = uint16_t;

struct alignas(utils::page_align) index_page
{
  constexpr static uint64_t magic_sig_value = "ckvs"_magic_sig;

  uint64_t _magic_sig;
  page_id  _bptree_root_handle;

  bool is_valid() const noexcept { return _magic_sig == magic_sig_value; }

  index_page() noexcept { _magic_sig = magic_sig_value; }
  char padding[4096 * 3];
};
static_assert(sizeof(index_page) == page_size);

struct alignas(utils::page_align) free_pages_page
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
    return true;
  }

  bool extract_free_page(page_id & free_page) noexcept
  {
    if(_nFree_pages == 0)
      return false;
    free_page = _free_pages[--_nFree_pages];
    CKVS_ASSERT(free_page % paged_file::extend_granularity != 1);
    return true;
  }
};
static_assert(sizeof(free_pages_page) == page_size);


using page_cache_t = page_cache<raw_page, page_id>;
struct store_state
{
  std::atomic<page_id> _first_unused_page_id = paged_file::extend_granularity + 1;
  paged_file *         _paged_file           = nullptr;
  page_cache_t *       _page_cache           = nullptr;
};

template <typename Config>
class locked_and_cached_extensions
{
  store_state &                   _store_state;
  static constexpr inline page_id index_page_id            = 0;
  static constexpr inline page_id first_free_pages_page_id = 1;

public:
  using node_t        = typename Config::node_t;
  using node_handle_t = page_handle<node_t>;

  using r_lock_t        = typename page_cache_t::shared_lazy_page_lock<false>;
  using w_lock_t        = typename page_cache_t::shared_lazy_page_lock<true>;
  using r_locked_node_t = ckvs::detail::locked_node<node_t, node_handle_t, r_lock_t>;
  using w_locked_node_t = ckvs::detail::locked_node<node_t, node_handle_t, w_lock_t>;

  static_assert(sizeof(node_t) < page_size);

  inline void mark_dirty(w_locked_node_t & locked_node) noexcept { locked_node._lock.mark_dirty(); }

  void update_root(const node_handle_t new_root) noexcept
  {
    CKVS_ASSERT(new_root._id % paged_file::extend_granularity != 1);

    w_lock_t exclusive_index{_store_state._page_cache->get_lazy_page_lock<true>(index_page_id)};
    exclusive_index.lock();
    auto index                 = exclusive_index.get_page_as<index_page>();
    index->_bptree_root_handle = new_root._id;
  }

  void validate(node_t * node) { CKVS_ASSERT(node->_kind != ckvs::detail::node_kind::Removed || !node->_nKeys); }

  template <bool ExclusivelyLocked,
            typename LockedNodeT = std::conditional_t<ExclusivelyLocked, w_locked_node_t, r_locked_node_t>,
            typename LockT       = typename LockedNodeT::lock_t>
  LockedNodeT get_node(const node_handle_t handle) noexcept
  {
    CKVS_ASSERT(handle._id < _store_state._first_unused_page_id.load(std::memory_order_acquire));
    CKVS_ASSERT(handle._id % paged_file::extend_granularity != 1);
    LockT locked_bptree_page{_store_state._page_cache->get_lazy_page_lock<ExclusivelyLocked>(handle._id)};
    locked_bptree_page.lock();
    LockedNodeT result;
    result._node   = locked_bptree_page.get_page_as<node_t>();
    result._lock   = std::move(locked_bptree_page);
    result._handle = handle;
    validate(result._node);
    return result;
  }

  inline std::optional<w_locked_node_t> upgrade_to_node_exclusive(r_locked_node_t & locked_node) noexcept
  {
    std::optional<w_lock_t> maybe_upgraded_lock{locked_node._lock.upgrade_or_consume()};
    if(!maybe_upgraded_lock)
    {
      return std::nullopt;
    }
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
      w_lock_t exclusive_free_pages_page{_store_state._page_cache->get_lazy_page_lock<true>(free_pages_page_id)};
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
      w_lock_t exclusive_free_pages_page{_store_state._page_cache->get_lazy_page_lock<true>(free_pages_page_id)};
      exclusive_free_pages_page.lock();
      if(exclusive_free_pages_page.get_page_as<free_pages_page>()->extract_free_page(new_node_handle._id))
      {
        exclusive_free_pages_page.mark_dirty();
        break;
      }
      // Do not unlock previous node, so we can't be outrun
      exclusive_previous_page = std::move(exclusive_free_pages_page);
    }

    while(new_node_handle == node_handle_t::invalid())
    {
      // We couldn't extract it, so we need to consume unused pages block. That will trigger auto-extend from paged_file, since we're requesting the first 'non-existent' page.
      w_lock_t exclusive_free_pages_page{_store_state._page_cache->get_lazy_page_lock<true>(first_unused_page_id)};
      exclusive_free_pages_page.lock();
      auto * fpp = exclusive_free_pages_page.get_page_as<free_pages_page>();
      new(fpp) free_pages_page{first_unused_page_id + 1, paged_file::extend_granularity - 1};
      if(fpp->extract_free_page(new_node_handle._id))
      {

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
    w_lock_t exclusive_bptree_page{_store_state._page_cache->get_lazy_page_lock<true>(new_node_handle._id)};
    exclusive_bptree_page.lock();
    exclusive_bptree_page.mark_dirty();
    node_t * raw_node = exclusive_bptree_page.get_page_as<node_t>();
    new(raw_node) node_t{std::forward<NodeCtorParams>(params)...};

    w_locked_node_t result;
    result._lock   = std::move(exclusive_bptree_page);
    result._node   = raw_node;
    result._handle = new_node_handle;
    CKVS_ASSERT(new_node_handle._id % paged_file::extend_granularity != 1);

    //#if defined(LOGGING)
    printf("new node: created #%u\n", new_node_handle._id);
    //#endif
    return result;
  }

  node_handle_t get_root() noexcept
  {
    r_lock_t shared_index{_store_state._page_cache->get_lazy_page_lock<false>(index_page_id)};
    shared_index.lock();
    auto index = shared_index.get_page_as<index_page>();
    CKVS_ASSERT(index->_bptree_root_handle % paged_file::extend_granularity != 1);
    return {index->_bptree_root_handle};
  }

  locked_and_cached_extensions(store_state & store_state, const bool assume_valid) : _store_state{store_state}
  {
    auto exclusive_index_page = _store_state._page_cache->get_lazy_page_lock<true>(index_page_id);
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
          w_lock_t exclusive_first_free_pages_page{
            _store_state._page_cache->get_lazy_page_lock<true>(first_free_pages_page_id)};
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

}