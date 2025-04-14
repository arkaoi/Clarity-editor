#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <unistd.h>

#include "sstable.hpp"

namespace DB {

using boost_file_source =
    boost::iostreams::stream<boost::iostreams::file_descriptor_source>;
using boost_file_sink =
    boost::iostreams::stream<boost::iostreams::file_descriptor_sink>;

SSTable::SSTable(const std::string &file) : filename(file) {
    loadIndex();
}

void SSTable::loadIndex() {
    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        return;
    }
    boost_file_source in(fd, boost::iostreams::close_handle);

    std::string line;
    while (std::getline(in, line)) {
        if (line == "##INDEX##") {
            break;
        }
    }
    if (in.eof()) {
        return;
    }

    size_t count = 0;
    in >> count;
    for (size_t i = 0; i < count; ++i) {
        std::string key;
        long long offset;
        in >> key >> offset;
        index[key] = static_cast<std::streampos>(offset);
    }
}

void SSTable::write(const std::map<std::string, DBEntry> &data) {
    int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << filename << "\n";
        return;
    }
    boost_file_sink out(fd, boost::iostreams::close_handle);

    std::map<std::string, std::streampos> newIndex;
    for (const auto &pair : data) {
        std::streampos pos = out.tellp();
        newIndex[pair.first] = pos;
        out << pair.first << " " << std::quoted(pair.second.value) << " "
            << (pair.second.tombstone ? "1" : "0") << "\n";
    }

    out << "##INDEX##\n";
    out << newIndex.size() << "\n";
    for (const auto &entry : newIndex) {
        out << entry.first << " " << entry.second << "\n";
    }
    index = newIndex;
}

bool SSTable::find(const std::string &key, DBEntry &entry) {
    auto it = index.find(key);
    if (it == index.end()) {
        return false;
    }

    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        return false;
    }
    
    boost_file_source in(fd, boost::iostreams::close_handle);
    in.seekg(it->second);
    if (!in) {
        return false;
    }

    std::string fileKey;
    std::string fileValue;
    int tomb;
    if (!(in >> fileKey >> std::quoted(fileValue) >> tomb)){
        return false;
    }
    if (fileKey != key) {
        return false;
    }

    entry.value = fileValue;
    entry.tombstone = (tomb == 1);

    return true;
}

std::map<std::string, DBEntry> SSTable::dump() const {
    std::map<std::string, DBEntry> records;
    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        return records;
    }
    boost_file_source in(fd, boost::iostreams::close_handle);

    std::string line;
    while (std::getline(in, line)) {
        if (line == "##INDEX##") {
            break;
        }

        std::istringstream iss(line);
        std::string key;
        std::string value;
        int tomb;

        if (!(iss >> key >> std::quoted(value) >> tomb)) {
            continue;
        }
        records[key] = {value, tomb == 1};
    }
    
    return records;
}

}  // namespace DB
