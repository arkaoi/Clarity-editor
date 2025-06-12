#ifndef DB_CONFIG_HPP_
#define DB_CONFIG_HPP_

#include <cstddef>

namespace DBConfig {

constexpr const char *kDirectory = "database";
constexpr std::size_t kMemtableLimit = 1000;
constexpr std::size_t kSstableLimit = 2;

}  // namespace DBConfig

#endif  // DB_CONFIG_HPP_