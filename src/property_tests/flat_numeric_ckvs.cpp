#include <page_cache.hpp>
#include <paged_file.hpp>
#include <bptree.hpp>

constexpr size_t page_size = 4096;
using page_id              = uint32_t;

using namespace ckvs;
using namespace utils;

struct alignas(page_size) page
{
};

template <typename>
struct page_handle
{
  page_id                   _id;
  bool                      operator==(const page_handle rhs) const noexcept { return _id == rhs._id; }
  bool                      operator!=(const page_handle rhs) const noexcept { return _id != rhs._id; }
  static inline page_handle invalid() noexcept { return page_handle{0}; }
};

using bp_tree_config_t = bp_tree_config<uint64_t, uint64_t, 1024>;

struct ttt
{
  uint64_t _;
};
static_assert(alignof(ttt) == 8);

class flat_numeric_ckvs_store
{
  //  bptree
  //public:
};

static_assert(sizeof(page) == page_size && std::is_trivially_copyable_v<page>);

void flat_numeric_ckvs_test(const size_t /*iteration*/, std::default_random_engine & /*gen*/, std::ostream & /*os*/)
{
  const size_t nThreads = std::thread::hardware_concurrency();
  //for()
}