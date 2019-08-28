#pragma once

#include <cinttypes>
#include <exception>

namespace ckvs { namespace utils {

template <typename ValT, typename ValidatorT>
ValT && validated(ValT && val, ValidatorT && func, std::exception failure)
{
  if(!func(std::forward<ValT>(val)))
    throw failure;
  return std::forward<ValT>(val);
}
}}