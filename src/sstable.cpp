#include "sstable.hpp"
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace DB {

SSTable::SSTable(const std::string &file) : filename(file) {
    if (std::filesystem::exists(filename) &&
        std::filesystem::file_size(filename) > 0) {
        loadIndex();
    }
}

void SSTable::loadIndex() {
    std::lock_guard<std::mutex> lock(indexMutex);
    index.clear();

    std::ifstream in(filename, std::ios::binary);
    if (!in) {
        throw std::runtime_error(
            "Cannot open SSTable for index load: " + filename
        );
    }

    std::string line;
    while (std::getline(in, line) && line != "##INDEX##") {
    }
    if (in.eof())
        return;

    size_t count = 0;
    in >> count;
    for (size_t i = 0; i < count; ++i) {
        std::string key;
        long long off_ll;
        in >> key >> off_ll;
        index[key] = static_cast<std::streampos>(off_ll);
    }
}

void SSTable::write(const std::map<std::string, DBEntry> &data) {
    std::ofstream out(filename, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("Cannot open SSTable for write: " + filename);
    }

    std::map<std::string, std::streampos> newIndex;
    for (const auto &p : data) {
        std::streampos pos = out.tellp();
        newIndex[p.first] = pos;
        out << p.first << " " << std::quoted(p.second.value) << " "
            << (p.second.tombstone ? "1" : "0") << "\n";
    }
    out << "##INDEX##\n";
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

bool SSTable::find(const std::string &key, DBEntry &entry) {
    std::lock_guard<std::mutex> lock(indexMutex);
    auto it = index.find(key);
    if (it == index.end())
        return false;

    std::ifstream in(filename, std::ios::binary);
    if (!in)
        return false;
    in.seekg(it->second);
    std::string fileKey, fileValue;
    int tomb;
    if (!(in >> fileKey >> std::quoted(fileValue) >> tomb))
        return false;
    if (fileKey != key)
        return false;
    entry.value = fileValue;
    entry.tombstone = (tomb == 1);
    return true;
}

std::map<std::string, DBEntry> SSTable::dump() const {
    std::map<std::string, DBEntry> outMap;
    std::ifstream in(filename, std::ios::binary);
    if (!in)
        return outMap;

    std::string line;
    while (std::getline(in, line) && line != "##INDEX##") {
        std::istringstream iss(line);
        std::string key, val;
        int tomb;
        if (iss >> key >> std::quoted(val) >> tomb) {
            outMap[key] = {val, tomb == 1};
        }
    }
    return outMap;
}

}  // namespace DB