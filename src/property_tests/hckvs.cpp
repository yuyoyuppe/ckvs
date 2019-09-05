#include <utils/common.hpp>
#include <utils/random.hpp>
#include <utils/traits.hpp>
#include <utils/validated_val.hpp>

#include <slotted_storage.hpp>

#include <iostream>
#include <unordered_set>
#include <algorithm>

using namespace ckvs;
using namespace utils;

// todo: handy high-level api for creating virtual store

struct node_tree
{
  std::string                                    _name;
  std::unordered_set<std::unique_ptr<node_tree>> _children;

  static bool is_valid_path(const std::string & s) noexcept
  {
    size_t slash_seq_len = 0;
    for(const char c : s)
    {
      if(c == '/')
      {
        if(++slash_seq_len > 1)
          return false;
        continue;
      }
      slash_seq_len = 0;
      if(c != std::isalnum(static_cast<char>(c)))
        return false;
      // todo: etc.
    }
    return true;
  }
};

class volume
{
  // todo: actual _store;
};


class virtual_store
{
public:
  void find();
  void insert();
  void remove();
};

class virtual_store_builder : utils::noncopyable
{
public:
  void mount(std::string /*mount_point*/, const size_t /*priority*/, std::unique_ptr<volume> /*volume*/) {}

  void unmount(std::string /*mount_point*/) {}

  virtual_store build() {}
};

void hckvs_basic_test(const size_t /*iteration*/, std::default_random_engine & /*gen*/, std::ostream & /*os*/) {}