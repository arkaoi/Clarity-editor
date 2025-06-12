#ifndef DB_ENTRY_HPP_
#define DB_ENTRY_HPP_

#include <cstdint>
#include <vector>

namespace DB {

struct DBEntry {
    std::vector<uint8_t> value;
    bool tombstone = false;
};

}  // namespace DB

#endif  // DB_ENTRY_HPP_