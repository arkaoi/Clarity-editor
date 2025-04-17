#ifndef DB_ENTRY_HPP_
#define DB_ENTRY_HPP_

#include <string>

namespace DB {

struct DBEntry {
  std::string value;
  bool tombstone = false;
};

} // namespace DB

#endif // DB_ENTRY_HPP_