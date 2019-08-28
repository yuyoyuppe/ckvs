#include "utils/common.hpp"

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