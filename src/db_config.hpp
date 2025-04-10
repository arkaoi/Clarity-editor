#ifndef DB_CONFIG_HPP_
#define DB_CONFIG_HPP_

#include <cstddef>

namespace DBConfig {

constexpr const char* kDirectory = "database";
constexpr std::size_t kMemtableLimit = 100;
constexpr std::size_t kSstableLimit = 8;

}  // namespace DBConfig

#endif  // DB_CONFIG_HPP_