#include "sstable.hpp"
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace DB {

static constexpr const char *DATA_BLOOM_MARKER = "##BLOOM##\n";
static constexpr const char *BLOOM_INDEX_MARKER = "##INDEX##\n";

SSTable::SSTable(const std::string &file) : filename(file), bf_() {
  if (std::filesystem::exists(filename) &&
      std::filesystem::file_size(filename) > 0) {
    loadIndex();
  }
}

void SSTable::loadIndex() {
  std::lock_guard<std::mutex> lock(indexMutex);
  index.clear();

  std::ifstream in(filename, std::ios::binary);
  if (!in)
    throw std::runtime_error("Cannot open SSTable: " + filename);

  const std::string bloomMarker(DATA_BLOOM_MARKER);
  std::string window;
  window.reserve(bloomMarker.size());
  char ch;
  while (in.get(ch)) {
    window.push_back(ch);
    if (window.size() > bloomMarker.size())
      window.erase(0, 1);
    if (window == bloomMarker)
      break;
  }
  if (in.eof())
    return;

  bf_.deserialize(in);

  const std::string indexMarker(BLOOM_INDEX_MARKER);
  window.clear();
  while (in.get(ch)) {
    window.push_back(ch);
    if (window.size() > indexMarker.size())
      window.erase(0, 1);
    if (window == indexMarker)
      break;
  }
  if (in.eof())
    return;

  size_t count = 0;
  in >> count;
  for (size_t i = 0; i < count; ++i) {
    std::string key;
    long long off;
    in >> key >> off;
    index[key] = static_cast<std::streampos>(off);
  }
}

void SSTable::write(SkipListMap<std::string, DBEntry> &data) {
  std::ofstream out(filename, std::ios::binary | std::ios::trunc);
  if (!out) {
    throw std::runtime_error("Cannot open SSTable for write: " + filename);
  }

  bf_ = BloomFilter(data.size() * 10, 7);
  std::map<std::string, std::streampos> newIndex;

  for (auto it = data.begin(); it != data.end(); ++it) {
    const auto &kv = *it;
    const std::string &key = kv.first;
    const DBEntry &entry = kv.second;

    bf_.add(key);

    std::streampos pos = out.tellp();
    newIndex[key] = pos;

    uint32_t keySize = static_cast<uint32_t>(key.size());
    out.write(reinterpret_cast<const char *>(&keySize), sizeof(keySize));
    out.write(key.data(), keySize);

    uint32_t valueSize = static_cast<uint32_t>(entry.value.size());
    out.write(reinterpret_cast<const char *>(&valueSize), sizeof(valueSize));
    if (valueSize > 0) {
      out.write(reinterpret_cast<const char *>(entry.value.data()), valueSize);
    }

    uint8_t tomb = entry.tombstone ? 1 : 0;
    out.write(reinterpret_cast<const char *>(&tomb), sizeof(tomb));
  }

  out << DATA_BLOOM_MARKER;

  bf_.serialize(out);

  out << BLOOM_INDEX_MARKER;
  out << newIndex.size() << "\n";
  for (const auto &e : newIndex) {
    out << e.first << " " << static_cast<long long>(e.second) << "\n";
  }

  out.flush();
  out.close();

  {
    std::lock_guard<std::mutex> lock(indexMutex);
    index = std::move(newIndex);
  }
}

bool SSTable::find(const std::string &key, DBEntry &entry) const {
  std::lock_guard<std::mutex> lock(indexMutex);

  if (!bf_.possiblyContains(key)) {
    return false;
  }

  auto it = index.find(key);
  if (it == index.end()) {
    return false;
  }

  std::ifstream in(filename, std::ios::binary);
  if (!in) {
    return false;
  }

  in.seekg(it->second);

  uint32_t keySize = 0;
  in.read(reinterpret_cast<char *>(&keySize), sizeof(keySize));
  std::string fileKey(keySize, '\0');
  in.read(&fileKey[0], keySize);
  if (fileKey != key) {
    return false;
  }

  uint32_t valueSize = 0;
  in.read(reinterpret_cast<char *>(&valueSize), sizeof(valueSize));
  std::vector<uint8_t> blob(valueSize);
  if (valueSize > 0) {
    in.read(reinterpret_cast<char *>(blob.data()), valueSize);
  }

  uint8_t tomb = 0;
  in.read(reinterpret_cast<char *>(&tomb), sizeof(tomb));

  entry.value = std::move(blob);
  entry.tombstone = (tomb == 1);
  return true;
}

std::map<std::string, DBEntry> SSTable::dump() const {
  std::map<std::string, std::streampos> idx_copy;
  {
    std::lock_guard<std::mutex> lock(indexMutex);
    idx_copy = index;
  }

  std::map<std::string, DBEntry> outMap;
  for (auto &p : idx_copy) {
    DBEntry e;
    if (find(p.first, e)) {
      outMap[p.first] = std::move(e);
    }
  }
  return outMap;
}

} // namespace DB
