#ifndef DATABASE_HPP_
#define DATABASE_HPP_

#include <map>
#include <sstream>
#include <string>
#include <vector>
#include "db_entry.hpp"
#include "sstable.hpp"

namespace DB {

class Database {
 private:
  std::map<std::string, DBEntry> memtable;
  std::vector<SSTable> sstables;
  size_t memtableLimit;
  size_t sstableLimit;
  std::string directory;

  void flushMemtable();
  void mergeSSTables();

 public:

  Database(const std::string& directory, size_t memLimit, size_t sstLimit);

  void insert(const std::string& key, const std::string& value);

  bool remove(const std::string& key);

  std::optional<std::string> select(const std::string& key);

  void flush();

  void merge();
};

}  // namespace DB

#endif  // DATABASE_HPP_
