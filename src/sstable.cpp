#include "sstable.hpp"
#include <iomanip>
#include <iostream>
#include <sstream>
namespace DB {

SSTable::SSTable(const std::string& file) : filename(file) { loadIndex(); }

void SSTable::loadIndex() {
  std::ifstream in(filename, std::ios::binary);
  if (!in) return;

  std::string line;
  std::streampos pos;
  while (std::getline(in, line)) {
    if (line == "##INDEX##") {
      break;
    }
  }
  if (in.eof()) return;

  size_t count = 0;
  in >> count;
  for (size_t i = 0; i < count; ++i) {
    std::string key;
    long long offset;
    in >> key >> offset;
    index[key] = static_cast<std::streampos>(offset);
  }
}

void SSTable::write(const std::map<std::string, DBEntry>& data) {
  std::ofstream out(filename, std::ios::binary | std::ios::trunc);
  if (!out) {
    std::cerr << "Failed to open file: " << filename << "\n";
    return;
  }

  std::map<std::string, std::streampos> newIndex;
  for (const auto& pair : data) {
    std::streampos pos = out.tellp();
    newIndex[pair.first] = pos;
    out << pair.first << " " << std::quoted(pair.second.value) << " "
        << (pair.second.tombstone ? "1" : "0") << "\n";
  }

  out << "##INDEX##\n";
  out << newIndex.size() << "\n";
  for (const auto& entry : newIndex) {
    out << entry.first << " " << entry.second << "\n";
  }

  index = newIndex;
}

bool SSTable::find(const std::string& key, DBEntry& entry) {
  auto it = index.find(key);
  if (it == index.end()) return false;

  std::ifstream in(filename, std::ios::binary);
  if (!in) return false;

  in.seekg(it->second);
  if (!in) return false;

  std::string fileKey;
  std::string fileValue;
  int tomb;
  if (!(in >> fileKey >> std::quoted(fileValue) >> tomb)) return false;
  if (fileKey != key) return false;
  entry.value = fileValue;
  entry.tombstone = (tomb == 1);
  return true;
}

std::map<std::string, DBEntry> SSTable::dump() const {
  std::map<std::string, DBEntry> records;
  std::ifstream in(filename, std::ios::binary);
  if (!in) return records;

  std::string line;
  while (std::getline(in, line)) {
    if (line == "##INDEX##") break;
    std::istringstream iss(line);
    std::string key, value;
    int tomb;
    if (!(iss >> key >> value >> tomb)) continue;
    records[key] = {value, tomb == 1};
  }
  return records;
}

}  // namespace DB
