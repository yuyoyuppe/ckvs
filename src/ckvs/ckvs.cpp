#include "utils/common.hpp"

#include <boost/predef.h>


#if defined(CHECK_LEAKS)
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
void * operator new(size_t n)
{
  void * ptr = _malloc_dbg(n, _NORMAL_BLOCK, "", 0);
  if(ptr)
    return ptr;
  else
    throw std::bad_alloc{};
}

void operator delete(void * p) noexcept { _free_dbg(p, _NORMAL_BLOCK); }
#endif

// Binary compatibility sanity checks
static_assert(sizeof(uint8_t) == alignof(uint8_t));
static_assert(sizeof(uint16_t) == alignof(uint16_t));
static_assert(sizeof(uint32_t) == alignof(uint32_t));
static_assert(sizeof(uint64_t) == alignof(uint64_t));

static_assert(CHAR_BIT == 8, "obscure architectures are not supported");
static constexpr bool little_endian =
#if defined(BOOST_ENDIAN_LITTLE_WORD)
  true;
#else
  false;
#endif
static_assert(little_endian, "only little endian architectures are binary compatible");
