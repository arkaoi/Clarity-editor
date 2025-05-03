#ifndef DATABASE_HPP_
#define DATABASE_HPP_

#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <userver/engine/mutex.hpp>
#include <vector>

#include "db_entry.hpp"
#include "sstable.hpp"
#include "wal.hpp"

namespace DB {

class Database {
private:
    std::map<std::string, DBEntry> memtable;
    std::vector<SSTable> sstables;

    size_t memtableLimit;
    size_t sstableLimit;

    std::string directory;
    DB::WAL wal_;

    mutable userver::engine::Mutex db_mutex;

    void flushMemtable();
    void mergeSSTables();
    void recoverFromWAL();

    void loadSSTables();

    std::optional<std::string> selectInternal(const std::string &key);

public:
    Database(const std::string &directory, size_t memLimit, size_t sstLimit);
    ~Database();

    void insert(const std::string &key, const std::string &value);
    bool remove(const std::string &key);

    std::optional<std::string> select(const std::string &key);

    void flush();
    void merge();
};

}  // namespace DB

#endif  // DATABASE_HPP_
